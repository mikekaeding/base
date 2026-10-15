use std::{fmt::Debug, sync::Arc, time::Duration};

use alloy_consensus::Block;
use alloy_eips::BlockNumberOrTag;
use alloy_provider::{Provider, RootProvider};
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
use tracing::{info, warn};
use url::Url;

use crate::follow::sources::{SOURCE_COOLDOWN, SourceSet};

/// A push connection that delivered no head for this long is rebuilt (Base makes a block every
/// 2 s). Only used with `--follow.http-sources`.
const PUSH_IDLE_RECONNECT: Duration = Duration::from_secs(10);
/// Pause between push reconnect attempts. Only used with `--follow.http-sources`.
const PUSH_RECONNECT_DELAY: Duration = Duration::from_secs(1);
/// alloy reconnect attempts inside one push connection before it is rebuilt from scratch.
const PUSH_MAX_RETRIES: u32 = 3;

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
        let reads = Arc::new(SourceSet::new(sources, options.request_timeout, SOURCE_COOLDOWN));
        let push = matches!(url.scheme(), "ws" | "wss")
            .then(|| PushSource { url: url.clone(), keepalive: options.ws_keepalive });
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
    /// fails, closes, or delivers no head for [`PUSH_IDLE_RECONNECT`]. Reads never use it, so a
    /// dead push only removes the early wake-up; 200 ms HTTP polling continues.
    async fn follow_heads_push(
        push: PushSource,
        sender: watch::Sender<u64>,
        cancellation: CancellationToken,
    ) -> Result<(), crate::follow::error::FollowError> {
        let label = url_label(&push.url);
        loop {
            let ws = WsConnect::new(push.url.as_str())
                .with_keepalive_interval(push.keepalive)
                .with_max_retries(PUSH_MAX_RETRIES);
            let connected = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                c = tokio::time::timeout(PUSH_IDLE_RECONNECT, ClientBuilder::default().ws(ws)) => c,
            };
            let provider = match connected {
                Ok(Ok(client)) => RootProvider::<Base>::new(client),
                Ok(Err(error)) => {
                    warn!(target: "follow", push = %label, %error, "Push connect failed; polling continues");
                    if Self::pause(&cancellation).await {
                        return Ok(());
                    }
                    continue;
                }
                Err(_) => {
                    warn!(target: "follow", push = %label, "Push connect timed out; polling continues");
                    if Self::pause(&cancellation).await {
                        return Ok(());
                    }
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
            if Self::pause(&cancellation).await {
                return Ok(());
            }
        }
    }

    /// Sleeps [`PUSH_RECONNECT_DELAY`]; returns true when cancelled.
    async fn pause(cancellation: &CancellationToken) -> bool {
        tokio::select! {
            _ = cancellation.cancelled() => true,
            _ = tokio::time::sleep(PUSH_RECONNECT_DELAY) => false,
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
                reads.call("payload", |p| async move { fetch_payload(&p, number).await }).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
