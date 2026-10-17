use std::{fmt::Debug, sync::Arc, time::Duration};

use alloy_consensus::Block;
use alloy_eips::BlockNumberOrTag;
use alloy_provider::{Provider, RootProvider};
use alloy_pubsub::PubSubConnect;
use alloy_rpc_client::ClientBuilder;
use alloy_transport_ws::WsConnect;
use async_trait::async_trait;
use base_common_consensus::BaseTxEnvelope;
use base_common_network::Base;
use base_common_rpc_types_engine::{BaseExecutionPayload, BaseExecutionPayloadEnvelope};
use base_protocol::BlockInfo;
use futures::StreamExt;
use thiserror::Error;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use url::Url;

use crate::follow::sources::{SourcePolicy, SourceSet};

/// A push connection that delivered no head for this long is rebuilt (Base makes a block every
/// 2 s). Only used with `--follow.http-sources`.
const PUSH_IDLE_RECONNECT: Duration = Duration::from_secs(10);
/// First pause between push connection attempts; doubles per attempt up to [`PUSH_BACKOFF_MAX`].
const PUSH_BACKOFF_BASE: Duration = Duration::from_secs(1);
/// Longest pause between push connection attempts.
const PUSH_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A push connection that lasted this long resets the backoff.
const PUSH_HEALTHY_AFTER: Duration = Duration::from_secs(30);
/// alloy reconnect attempts inside one push connection before it is rebuilt from scratch.
const PUSH_MAX_RETRIES: u32 = 3;

/// Spacing of push (WebSocket) connection attempts, shared by alloy's own reconnects and the
/// rebuild loop: the first attempt after a healthy period is immediate, then 1 s, 2 s, 4 s, ...
/// up to 30 s while attempts keep coming within 30 s of each other. During a storm of 1013
/// closes this is at most one TLS handshake per backoff step instead of one per close.
#[derive(Debug)]
struct PushBackoff {
    base: Duration,
    max: Duration,
    healthy_after: Duration,
    /// (start of the previous attempt, delay to use for the next one)
    state: std::sync::Mutex<(Option<std::time::Instant>, Duration)>,
}

impl PushBackoff {
    fn new(base: Duration, max: Duration, healthy_after: Duration) -> Self {
        Self { base, max, healthy_after, state: std::sync::Mutex::new((None, base)) }
    }

    /// Delay to wait before an attempt starting at `now`; records the attempt.
    fn next_delay(&self, now: std::time::Instant) -> Duration {
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let delay = match s.0 {
            Some(prev) if now.saturating_duration_since(prev) < self.healthy_after => s.1,
            _ => Duration::ZERO,
        };
        let next = if delay.is_zero() { self.base } else { delay.saturating_mul(2).min(self.max) };
        *s = (Some(now + delay), next);
        delay
    }
}

/// WebSocket connector whose reconnects (alloy calls `try_reconnect` after every close) wait for
/// the shared [`PushBackoff`].
#[derive(Debug, Clone)]
struct BackoffWsConnect {
    inner: WsConnect,
    backoff: Arc<PushBackoff>,
}

impl PubSubConnect for BackoffWsConnect {
    fn is_local(&self) -> bool {
        self.inner.is_local()
    }

    fn connect(
        &self,
    ) -> impl std::future::Future<
        Output = alloy_transport::TransportResult<alloy_pubsub::ConnectionHandle>,
    > + Send {
        self.inner.connect()
    }

    fn try_reconnect(
        &self,
    ) -> impl std::future::Future<
        Output = alloy_transport::TransportResult<alloy_pubsub::ConnectionHandle>,
    > + Send {
        let (inner, backoff) = (self.inner.clone(), Arc::clone(&self.backoff));
        async move {
            let delay = backoff.next_delay(std::time::Instant::now());
            if !delay.is_zero() {
                debug!(target: "follow", delay_ms = delay.as_millis() as u64, "Push reconnect backing off");
                tokio::time::sleep(delay).await;
            }
            inner.connect().await
        }
    }
}

/// Error type for [`RemoteL2Client`] operations.
#[derive(Debug, Error)]
pub enum RemoteL2ClientError {
    /// Connecting the configured HTTP or WebSocket source failed.
    #[error("failed to connect source L2 RPC: {0}")]
    Connect(alloy_transport::TransportError),
    /// Failed to fetch block from L2 EL.
    #[error("failed to fetch block at {tag}: {source}")]
    FetchBlock {
        /// The block tag that was requested.
        tag: String,
        /// The underlying transport error.
        source: alloy_transport::TransportError,
    },

    /// Block not found at the requested tag.
    #[error("block not found at {0}")]
    BlockNotFound(String),

    /// A fallback source did not answer within the request timeout.
    #[error("{what} request to {endpoint} timed out after {timeout_ms} ms")]
    Timeout {
        /// The request kind.
        what: &'static str,
        /// The source host.
        endpoint: String,
        /// The timeout that elapsed.
        timeout_ms: u64,
    },

    /// No fallback source is configured.
    #[error("no follow source configured")]
    NoSources,

    /// A fallback source URL is not usable.
    #[error("invalid follow source: {0}")]
    InvalidSource(String),

    /// An unsafe chain repair before `block` waits for the engine to reach the common ancestor
    /// (a normal transient while the insert loop drains its queue).
    #[error(
        "unsafe chain repair before block {block} waits for the engine (unsafe head {unsafe_head})"
    )]
    EngineBehind {
        /// The RPC payload height that did not extend the served chain.
        block: u64,
        /// The engine's current unsafe head number.
        unsafe_head: u64,
    },

    /// No common ancestor with the RPC chain within the repair depth: the local unsafe chain
    /// needs a manual unwind.
    #[error("no common ancestor with the RPC chain before block {block} within {searched} blocks")]
    ReorgTooDeep {
        /// The RPC payload height that did not extend the served chain.
        block: u64,
        /// RPC blocks walked back.
        searched: usize,
    },

    /// The RPC payload at `block` does not extend the block served before it, and replacing the
    /// served blocks with the RPC chain did not succeed (retried on the next fetch).
    #[error("unsafe chain repair before block {block} failed: {reason}")]
    ReorgRepair {
        /// The RPC payload height that did not extend the served chain.
        block: u64,
        /// Why the repair did not complete.
        reason: String,
    },
}

/// Trait for fetching L2 block data from the remote node.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait RemoteClient: Debug + Send + Sync {
    /// Fetches the block number at the given tag.
    async fn get_block_number(&self, tag: BlockNumberOrTag) -> Result<u64, RemoteL2ClientError>;

    /// Fetches the block info at the given tag.
    async fn get_block_info(&self, tag: BlockNumberOrTag)
    -> Result<BlockInfo, RemoteL2ClientError>;

    /// Fetches a block by number and converts it to an [`BaseExecutionPayloadEnvelope`].
    async fn get_payload_by_number(
        &self,
        number: u64,
    ) -> Result<BaseExecutionPayloadEnvelope, RemoteL2ClientError>;
}

/// Opt-in options for [`RemoteL2Client::with_options`].
///
/// With no `http_sources` the client is the legacy single-connection follower: one provider on
/// `--source-l2-rpc` carries the newHeads push, height polls and payload fetches, with alloy's
/// default WebSocket keepalive and no request timeout.
#[derive(Debug, Clone, Default)]
pub struct RemoteL2ClientOptions {
    /// Ordered HTTP(S) sources for every read (heights, payloads, labels). Non-empty enables the
    /// fallback mode; `--source-l2-rpc` is then used only as a newHeads wake-up push when it is a
    /// WebSocket URL.
    pub http_sources: Vec<Url>,
    /// Per-request timeout for each fallback source.
    pub request_timeout: Duration,
    /// Keepalive of the push WebSocket in fallback mode.
    pub ws_keepalive: Duration,
}

#[derive(Debug, Clone)]
enum Mode {
    /// One provider for push, polls and fetches (unchanged behaviour).
    Legacy { provider: RootProvider<Base> },
    /// Reads through ordered fallback sources; optional WebSocket push used only as a hint.
    Fallback { reads: Arc<SourceSet<RootProvider<Base>>>, push: Option<PushSource> },
}

#[derive(Debug, Clone)]
struct PushSource {
    url: Url,
    keepalive: Duration,
    backoff: Arc<PushBackoff>,
}

/// Client that follows an HTTP or WebSocket L2 execution layer source and
/// converts blocks into [`BaseExecutionPayloadEnvelope`] for engine insertion.
#[derive(Debug, Clone)]
pub struct RemoteL2Client {
    mode: Mode,
}

/// Host part of a URL for logs (never the path or query, which may carry keys).
pub(crate) fn url_label(url: &Url) -> String {
    match (url.host_str(), url.port()) {
        (Some(h), Some(p)) => format!("{}://{h}:{p}", url.scheme()),
        (Some(h), None) => format!("{}://{h}", url.scheme()),
        _ => url.scheme().to_string(),
    }
}

impl RemoteL2Client {
    /// Creates a new [`RemoteL2Client`] from a source L2 node URL (legacy mode).
    pub async fn new(url: Url) -> Result<Self, RemoteL2ClientError> {
        let provider = RootProvider::<Base>::connect(url.as_str())
            .await
            .map_err(RemoteL2ClientError::Connect)?;
        Ok(Self { mode: Mode::Legacy { provider } })
    }

    /// Creates a client with opt-in fallback sources. Without `http_sources` this is exactly
    /// [`RemoteL2Client::new`].
    pub async fn with_options(
        url: Url,
        options: RemoteL2ClientOptions,
    ) -> Result<Self, RemoteL2ClientError> {
        if options.http_sources.is_empty() {
            return Self::new(url).await;
        }
        if options.request_timeout.is_zero() {
            return Err(RemoteL2ClientError::InvalidSource("request timeout must be > 0".into()));
        }
        if options.ws_keepalive.is_zero() {
            return Err(RemoteL2ClientError::InvalidSource("push keepalive must be > 0".into()));
        }
        let mut sources = Vec::with_capacity(options.http_sources.len());
        for u in &options.http_sources {
            if !matches!(u.scheme(), "http" | "https") {
                return Err(RemoteL2ClientError::InvalidSource(format!(
                    "{} is not an http(s) URL",
                    url_label(u)
                )));
            }
            sources.push((url_label(u), RootProvider::<Base>::new_http(u.clone())));
        }
        let reads =
            Arc::new(SourceSet::new(sources, SourcePolicy::with_timeout(options.request_timeout)));
        let push = matches!(url.scheme(), "ws" | "wss").then(|| PushSource {
            url: url.clone(),
            keepalive: options.ws_keepalive,
            backoff: Arc::new(PushBackoff::new(
                PUSH_BACKOFF_BASE,
                PUSH_BACKOFF_MAX,
                PUSH_HEALTHY_AFTER,
            )),
        });
        info!(target: "follow", reads = ?reads.labels(), push = ?push.as_ref().map(|p| url_label(&p.url)),
            timeout_ms = options.request_timeout.as_millis() as u64,
            keepalive_ms = options.ws_keepalive.as_millis() as u64,
            "Follow sources: ordered HTTP reads with timeouts; WebSocket push is a wake-up hint only");
        Ok(Self { mode: Mode::Fallback { reads, push } })
    }

    /// Whether this client reads through fallback sources.
    pub const fn is_fallback(&self) -> bool {
        matches!(self.mode, Mode::Fallback { .. })
    }

    /// Test-only: a shorter push backoff.
    #[cfg(test)]
    pub(super) fn with_push_backoff(
        mut self,
        base: Duration,
        max: Duration,
        healthy_after: Duration,
    ) -> Self {
        if let Mode::Fallback { push: Some(push), .. } = &mut self.mode {
            push.backoff = Arc::new(PushBackoff::new(base, max, healthy_after));
        }
        self
    }

    /// Publishes coalesced height hints; payload identity and execution remain independently checked.
    pub(super) async fn follow_heads(
        self,
        sender: watch::Sender<u64>,
        cancellation: CancellationToken,
    ) -> Result<(), crate::follow::error::FollowError> {
        match self.mode {
            Mode::Legacy { provider } => {
                Self::follow_heads_legacy(provider, sender, cancellation).await
            }
            Mode::Fallback { push: Some(push), .. } => {
                Self::follow_heads_push(push, sender, cancellation).await
            }
            Mode::Fallback { push: None, .. } => {
                cancellation.cancelled().await;
                Ok(())
            }
        }
    }

    async fn follow_heads_legacy(
        provider: RootProvider<Base>,
        sender: watch::Sender<u64>,
        cancellation: CancellationToken,
    ) -> Result<(), crate::follow::error::FollowError> {
        if provider.client().pubsub_frontend().is_none() {
            cancellation.cancelled().await;
            return Ok(());
        }
        loop {
            let subscription = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                result = provider.subscribe_blocks() => result,
            };
            match subscription {
                Ok(subscription) => {
                    let mut heads = subscription.into_stream();
                    loop {
                        tokio::select! {
                            _ = cancellation.cancelled() => return Ok(()),
                            head = heads.next() => {
                                let Some(head) = head else { break; };
                                info!(target: "follow", block = head.number, block_timestamp = head.timestamp,
                                    "Received source head");
                                if sender.send(head.number).is_err() { return Ok(()); }
                            }
                        }
                    }
                    warn!(target: "follow", "Source head subscription closed; resubscribing while polling remains active");
                }
                Err(error) => {
                    warn!(target: "follow", %error, "Source head subscription failed; retrying while polling remains active")
                }
            }
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            }
        }
    }

    /// Fallback-mode push: its own WebSocket with a short keepalive, rebuilt from scratch when it
    /// fails, closes, or delivers no head for [`PUSH_IDLE_RECONNECT`]. Every connection attempt
    /// (alloy's internal reconnects and these rebuilds) waits for the shared [`PushBackoff`].
    /// Reads never use the push, so a dead push only removes the early wake-up; 200 ms HTTP
    /// polling continues.
    async fn follow_heads_push(
        push: PushSource,
        sender: watch::Sender<u64>,
        cancellation: CancellationToken,
    ) -> Result<(), crate::follow::error::FollowError> {
        let label = url_label(&push.url);
        loop {
            let delay = push.backoff.next_delay(std::time::Instant::now());
            if !delay.is_zero() {
                tokio::select! {
                    _ = cancellation.cancelled() => return Ok(()),
                    _ = tokio::time::sleep(delay) => {}
                }
            }
            let ws = BackoffWsConnect {
                inner: WsConnect::new(push.url.as_str())
                    .with_keepalive_interval(push.keepalive)
                    .with_max_retries(PUSH_MAX_RETRIES),
                backoff: Arc::clone(&push.backoff),
            };
            let connected = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                c = tokio::time::timeout(PUSH_IDLE_RECONNECT, ClientBuilder::default().pubsub(ws)) => c,
            };
            let provider = match connected {
                Ok(Ok(client)) => RootProvider::<Base>::new(client),
                Ok(Err(error)) => {
                    warn!(target: "follow", push = %label, %error, "Push connect failed; polling continues");
                    continue;
                }
                Err(_) => {
                    warn!(target: "follow", push = %label, "Push connect timed out; polling continues");
                    continue;
                }
            };
            let subscription = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                s = tokio::time::timeout(PUSH_IDLE_RECONNECT, provider.subscribe_blocks()) => s,
            };
            match subscription {
                Ok(Ok(subscription)) => {
                    let mut heads = subscription.into_stream();
                    loop {
                        let head = tokio::select! {
                            _ = cancellation.cancelled() => return Ok(()),
                            h = tokio::time::timeout(PUSH_IDLE_RECONNECT, heads.next()) => h,
                        };
                        match head {
                            Ok(Some(head)) => {
                                info!(target: "follow", block = head.number, block_timestamp = head.timestamp,
                                    "Received source head");
                                if sender.send(head.number).is_err() {
                                    return Ok(());
                                }
                            }
                            Ok(None) => {
                                warn!(target: "follow", push = %label, "Push subscription closed; rebuilding push, polling continues");
                                break;
                            }
                            Err(_) => {
                                warn!(target: "follow", push = %label, idle_s = PUSH_IDLE_RECONNECT.as_secs(),
                                    "No source head pushed; rebuilding push, polling continues");
                                break;
                            }
                        }
                    }
                }
                Ok(Err(error)) => {
                    warn!(target: "follow", push = %label, %error, "Push subscribe failed; polling continues")
                }
                Err(_) => {
                    warn!(target: "follow", push = %label, "Push subscribe timed out; polling continues")
                }
            }
            drop(provider);
        }
    }
}

async fn fetch_block_number(provider: &RootProvider<Base>) -> Result<u64, RemoteL2ClientError> {
    provider.get_block_number().await.map_err(|e| RemoteL2ClientError::FetchBlock {
        tag: format!("{:?}", BlockNumberOrTag::Latest),
        source: e,
    })
}

async fn fetch_block_info(
    provider: &RootProvider<Base>,
    tag: BlockNumberOrTag,
) -> Result<BlockInfo, RemoteL2ClientError> {
    let block = provider
        .get_block_by_number(tag)
        .await
        .map_err(|e| RemoteL2ClientError::FetchBlock { tag: format!("{tag:?}"), source: e })?
        .ok_or_else(|| RemoteL2ClientError::BlockNotFound(format!("{tag:?}")))?;
    let block = block.map_header(|header| header.into_inner());

    Ok(BlockInfo::from(&block))
}

async fn fetch_payload(
    provider: &RootProvider<Base>,
    number: u64,
) -> Result<BaseExecutionPayloadEnvelope, RemoteL2ClientError> {
    let rpc_block = provider
        .get_block_by_number(number.into())
        .full()
        .await
        .map_err(|e| RemoteL2ClientError::FetchBlock { tag: format!("{number}"), source: e })?
        .ok_or_else(|| RemoteL2ClientError::BlockNotFound(format!("{number}")))?;
    let rpc_block = rpc_block.map_header(|header| header.into_inner());

    let block_hash = rpc_block.header.hash;
    let parent_beacon_block_root = rpc_block.header.parent_beacon_block_root;

    let txs: Vec<BaseTxEnvelope> =
        rpc_block.transactions.into_transactions().map(|tx| tx.inner.inner.into_inner()).collect();

    let consensus_block: Block<BaseTxEnvelope> = Block {
        header: rpc_block.header.inner,
        body: alloy_consensus::BlockBody {
            transactions: txs,
            ommers: vec![],
            withdrawals: rpc_block.withdrawals,
        },
    };

    let (execution_payload, _sidecar) =
        BaseExecutionPayload::from_block_unchecked(block_hash, &consensus_block);

    Ok(BaseExecutionPayloadEnvelope { parent_beacon_block_root, execution_payload })
}

#[async_trait]
impl RemoteClient for RemoteL2Client {
    async fn get_block_number(&self, tag: BlockNumberOrTag) -> Result<u64, RemoteL2ClientError> {
        match &self.mode {
            Mode::Legacy { provider } => {
                if matches!(tag, BlockNumberOrTag::Latest) {
                    return fetch_block_number(provider).await;
                }
                fetch_block_info(provider, tag).await.map(|block| block.number)
            }
            Mode::Fallback { reads, .. } => {
                if matches!(tag, BlockNumberOrTag::Latest) {
                    return reads
                        .call("height", |p| async move { fetch_block_number(&p).await })
                        .await;
                }
                reads
                    .call("label", |p| async move { fetch_block_info(&p, tag).await })
                    .await
                    .map(|block| block.number)
            }
        }
    }

    async fn get_block_info(
        &self,
        tag: BlockNumberOrTag,
    ) -> Result<BlockInfo, RemoteL2ClientError> {
        match &self.mode {
            Mode::Legacy { provider } => fetch_block_info(provider, tag).await,
            Mode::Fallback { reads, .. } => {
                reads.call("label", |p| async move { fetch_block_info(&p, tag).await }).await
            }
        }
    }

    async fn get_payload_by_number(
        &self,
        number: u64,
    ) -> Result<BaseExecutionPayloadEnvelope, RemoteL2ClientError> {
        match &self.mode {
            Mode::Legacy { provider } => fetch_payload(provider, number).await,
            Mode::Fallback { reads, .. } => {
                reads
                    .call_block(
                        "payload",
                        number,
                        |p| async move { fetch_payload(&p, number).await },
                    )
                    .await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_backoff_doubles_to_the_cap_and_resets_after_a_healthy_period() {
        let b = PushBackoff::new(
            Duration::from_secs(1),
            Duration::from_secs(30),
            Duration::from_secs(30),
        );
        let t0 = std::time::Instant::now();
        let mut t = t0;
        let mut got = Vec::new();
        for _ in 0..8 {
            let d = b.next_delay(t);
            got.push(d.as_secs());
            t += d + Duration::from_millis(100); // the attempt fails right away
        }
        assert_eq!(got, vec![0, 1, 2, 4, 8, 16, 30, 30]);
        // a connection that stayed up for 30 s resets it: the next attempt is immediate
        assert_eq!(b.next_delay(t + Duration::from_secs(31)), Duration::ZERO);
        assert_eq!(b.next_delay(t + Duration::from_secs(31)), Duration::from_secs(1));
    }

    #[test]
    fn url_label_never_includes_path_or_query() {
        let u = Url::parse("https://user:pw@rpc.example.com/v2/SECRETKEY?apikey=1").unwrap();
        assert_eq!(url_label(&u), "https://rpc.example.com");
        let u = Url::parse("http://10.0.64.4:18547").unwrap();
        assert_eq!(url_label(&u), "http://10.0.64.4:18547");
    }

    #[tokio::test]
    async fn with_options_rejects_non_http_fallback_sources() {
        let err = RemoteL2Client::with_options(
            Url::parse("ws://127.0.0.1:9").unwrap(),
            RemoteL2ClientOptions {
                http_sources: vec![Url::parse("wss://rpc.example.com/key").unwrap()],
                request_timeout: Duration::from_secs(1),
                ws_keepalive: Duration::from_secs(2),
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, RemoteL2ClientError::InvalidSource(ref m) if !m.contains("key")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn with_options_rejects_zero_timeout() {
        let err = RemoteL2Client::with_options(
            Url::parse("ws://127.0.0.1:9").unwrap(),
            RemoteL2ClientOptions {
                http_sources: vec![Url::parse("https://rpc.example.com").unwrap()],
                request_timeout: Duration::ZERO,
                ws_keepalive: Duration::from_secs(2),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, RemoteL2ClientError::InvalidSource(_)));
    }

    #[tokio::test]
    async fn with_options_rejects_zero_keepalive() {
        let err = RemoteL2Client::with_options(
            Url::parse("ws://127.0.0.1:9").unwrap(),
            RemoteL2ClientOptions {
                http_sources: vec![Url::parse("https://rpc.example.com").unwrap()],
                request_timeout: Duration::from_secs(1),
                ws_keepalive: Duration::ZERO,
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, RemoteL2ClientError::InvalidSource(ref m) if m.contains("keepalive")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn fallback_mode_does_not_connect_the_push_at_startup() {
        // The push URL points nowhere; construction must still succeed (push is lazy and optional).
        let client = RemoteL2Client::with_options(
            Url::parse("ws://127.0.0.1:9").unwrap(),
            RemoteL2ClientOptions {
                http_sources: vec![Url::parse("http://127.0.0.1:9").unwrap()],
                request_timeout: Duration::from_millis(100),
                ws_keepalive: Duration::from_secs(2),
            },
        )
        .await
        .unwrap();
        assert!(client.is_fallback());
    }
}
