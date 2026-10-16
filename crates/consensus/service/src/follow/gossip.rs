//! Optional P2P unsafe-block gossip as the first payload source of the follow node.
//!
//! The follow runtime stays the only driver of the engine. The libp2p network actor validates
//! each gossiped block (sequencer signature, timestamp window, hash) exactly as in node mode and
//! hands it to [`GossipCacheEngineClient`], which only stores it in a bounded cache. The
//! prefetcher then asks [`GossipFirstClient`] for each payload:
//!
//! - [`FollowP2pMode::Shadow`]: every payload still comes from the RPC sources. Gossip is only
//!   measured (arrival lag behind the block timestamp, and how much earlier gossip had each block
//!   than the RPC fetch completed).
//! - [`FollowP2pMode::Primary`]: a cached gossip payload is used when it is the only candidate at
//!   that height and its parent hash is the block returned just before it. Anything else (missing,
//!   ambiguous, parent mismatch, a second request for the same height) falls through to the RPC
//!   sources, so P2P can only make a block earlier, never block the follower.
//!
//! A network that fails to start, closes or loses all peers leaves the follower on RPC.

use std::{
    collections::BTreeMap,
    fmt::Debug,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::B256;
use async_trait::async_trait;
use base_common_rpc_types_engine::BaseExecutionPayloadEnvelope;
use base_protocol::BlockInfo;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::{
    EngineClientResult, Metrics, NetworkEngineClient,
    follow::source::{RemoteClient, RemoteL2ClientError},
};

/// Heights kept in the gossip cache (about 8.5 minutes of 2 s blocks, 51 s of 200 ms blocks).
pub const GOSSIP_CACHE_HEIGHTS: u64 = 256;

/// Distinct payloads kept per height. Gossip validation already caps this at 5.
const MAX_CANDIDATES_PER_HEIGHT: usize = 5;

/// How the follow node uses P2P unsafe-block gossip.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FollowP2pMode {
    /// No P2P stack (today's follower).
    #[default]
    Off,
    /// Join the gossip network and measure it; payloads still come from the RPC sources.
    Shadow,
    /// Use validated gossip payloads first; the RPC sources remain the fallback.
    Primary,
}

impl FollowP2pMode {
    /// Whether the P2P stack runs at all.
    pub const fn enabled(self) -> bool {
        !matches!(self, Self::Off)
    }
}

impl FromStr for FollowP2pMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "off" | "false" | "0" => Ok(Self::Off),
            "shadow" => Ok(Self::Shadow),
            "primary" => Ok(Self::Primary),
            other => Err(format!("invalid follow P2P mode {other:?}: expected off, shadow or primary")),
        }
    }
}

impl std::fmt::Display for FollowP2pMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
            Self::Primary => "primary",
        })
    }
}

#[derive(Debug, Clone)]
struct Candidate {
    hash: B256,
    parent_hash: B256,
    payload: BaseExecutionPayloadEnvelope,
    received: Instant,
}

/// Bounded cache of validated gossip payloads, keyed by block number.
#[derive(Debug, Default)]
pub struct GossipPayloadCache {
    inner: Mutex<BTreeMap<u64, Vec<Candidate>>>,
}

impl GossipPayloadCache {
    /// Stores a payload. Returns `true` when it is new (not a duplicate of a cached one).
    fn insert(&self, payload: BaseExecutionPayloadEnvelope, received: Instant) -> bool {
        let number = payload.execution_payload.block_number();
        let hash = payload.execution_payload.block_hash();
        let parent_hash = payload.execution_payload.parent_hash();
        let mut map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((&highest, _)) = map.last_key_value()
            && number.saturating_add(GOSSIP_CACHE_HEIGHTS) <= highest
        {
            // Far behind the newest gossip; never useful to the follower.
            return false;
        }
        let candidates = map.entry(number).or_default();
        if candidates.iter().any(|c| c.hash == hash) || candidates.len() >= MAX_CANDIDATES_PER_HEIGHT
        {
            return false;
        }
        candidates.push(Candidate { hash, parent_hash, payload, received });
        let highest = *map.last_key_value().map(|(k, _)| k).unwrap_or(&number);
        let floor = highest.saturating_sub(GOSSIP_CACHE_HEIGHTS);
        while let Some((&lowest, _)) = map.first_key_value() {
            if lowest >= floor {
                break;
            }
            map.remove(&lowest);
        }
        true
    }

    fn candidates(&self, number: u64) -> Vec<Candidate> {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&number).cloned().unwrap_or_default()
    }

    /// Highest cached height.
    pub fn highest(&self) -> Option<u64> {
        let map = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        map.last_key_value().map(|(k, _)| *k)
    }
}

fn unix_now_secs_f64() -> f64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs_f64()).unwrap_or_default()
}

/// Receives validated gossip blocks from the network actor and caches them.
///
/// It never fails: an error here would stop the network actor.
#[derive(Debug, Clone)]
pub struct GossipCacheEngineClient {
    cache: Arc<GossipPayloadCache>,
    mode: FollowP2pMode,
    /// Wake-up for the prefetcher, only in [`FollowP2pMode::Primary`].
    heads: Option<watch::Sender<u64>>,
}

impl GossipCacheEngineClient {
    /// Creates the client.
    pub const fn new(
        cache: Arc<GossipPayloadCache>,
        mode: FollowP2pMode,
        heads: Option<watch::Sender<u64>>,
    ) -> Self {
        Self { cache, mode, heads }
    }
}

#[async_trait]
impl NetworkEngineClient for GossipCacheEngineClient {
    async fn send_unsafe_block(&self, block: BaseExecutionPayloadEnvelope) -> EngineClientResult<()> {
        let received = Instant::now();
        let number = block.execution_payload.block_number();
        let timestamp = block.execution_payload.timestamp();
        let lag_ms = (unix_now_secs_f64() - timestamp as f64) * 1000.0;
        let hash = block.execution_payload.block_hash();
        if self.cache.insert(block, received) {
            Metrics::follow_p2p_gossip_blocks_total().increment(1);
            Metrics::follow_p2p_gossip_lag_seconds().record(lag_ms / 1000.0);
            info!(target: "follow::p2p", block = number, %hash, lag_ms = lag_ms.round() as i64,
                mode = %self.mode, "Gossip block received");
            if self.mode == FollowP2pMode::Primary
                && let Some(heads) = &self.heads
            {
                heads.send_if_modified(|current| {
                    if number > *current {
                        *current = number;
                        true
                    } else {
                        false
                    }
                });
            }
        }
        Ok(())
    }

    async fn send_admin_unsafe_block(
        &self,
        _block: BaseExecutionPayloadEnvelope,
    ) -> EngineClientResult<()> {
        // Follow mode mounts no admin RPC; nothing reaches here.
        Ok(())
    }
}

#[derive(Debug, Default)]
struct ServeState {
    /// The last payload returned to the prefetcher (number, hash).
    last: Option<(u64, B256)>,
    /// The last height served from gossip; a repeat request for it goes to RPC.
    last_gossip: Option<u64>,
}

/// A [`RemoteClient`] that answers payload requests from validated gossip first.
#[derive(Debug)]
pub struct GossipFirstClient<R> {
    inner: R,
    cache: Arc<GossipPayloadCache>,
    mode: FollowP2pMode,
    state: Mutex<ServeState>,
}

impl<R> GossipFirstClient<R> {
    /// Wraps `inner` (the RPC sources).
    pub fn new(inner: R, cache: Arc<GossipPayloadCache>, mode: FollowP2pMode) -> Self {
        Self { inner, cache, mode, state: Mutex::new(ServeState::default()) }
    }

    fn pick_gossip(&self, number: u64) -> Result<Candidate, &'static str> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.last_gossip == Some(number) {
            return Err("retry");
        }
        let Some((last_number, last_hash)) = state.last else {
            return Err("no_parent");
        };
        drop(state);
        let candidates = self.cache.candidates(number);
        if candidates.is_empty() {
            return Err("missing");
        }
        if last_number.saturating_add(1) != number {
            return Err("parent_mismatch");
        }
        let mut matching = candidates.into_iter().filter(|c| c.parent_hash == last_hash);
        match (matching.next(), matching.next()) {
            (Some(c), None) => Ok(c),
            (Some(_), Some(_)) => Err("ambiguous"),
            (None, _) => Err("parent_mismatch"),
        }
    }

    fn record_served(&self, number: u64, hash: B256, from_gossip: bool) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.last = Some((number, hash));
        if from_gossip {
            state.last_gossip = Some(number);
        }
    }
}

#[async_trait]
impl<R: RemoteClient> RemoteClient for GossipFirstClient<R> {
    async fn get_block_number(&self, tag: BlockNumberOrTag) -> Result<u64, RemoteL2ClientError> {
        self.inner.get_block_number(tag).await
    }

    async fn get_block_info(
        &self,
        tag: BlockNumberOrTag,
    ) -> Result<BlockInfo, RemoteL2ClientError> {
        self.inner.get_block_info(tag).await
    }

    async fn get_payload_by_number(
        &self,
        number: u64,
    ) -> Result<BaseExecutionPayloadEnvelope, RemoteL2ClientError> {
        if self.mode == FollowP2pMode::Primary {
            match self.pick_gossip(number) {
                Ok(candidate) => {
                    Metrics::follow_p2p_payload_source_total("p2p").increment(1);
                    debug!(target: "follow::p2p", block = number, hash = %candidate.hash,
                        "Serving payload from gossip");
                    self.record_served(number, candidate.hash, true);
                    return Ok(candidate.payload);
                }
                Err(reason) => {
                    Metrics::follow_p2p_fallback_total(reason).increment(1);
                    if reason != "missing" && reason != "no_parent" {
                        warn!(target: "follow::p2p", block = number, reason,
                            "Gossip payload not used; reading from RPC sources");
                    }
                }
            }
        }

        let payload = self.inner.get_payload_by_number(number).await?;
        let rpc_ready = Instant::now();
        let hash = payload.execution_payload.block_hash();
        if self.mode == FollowP2pMode::Primary {
            // An RPC block that does not extend the block served just before it means the chain
            // moved under us (an unsafe reorg); flag it, most of all after a gossip block.
            let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Some((last_number, last_hash)) = state.last
                && last_number.saturating_add(1) == number
                && payload.execution_payload.parent_hash() != last_hash
            {
                let after_gossip = state.last_gossip == Some(last_number);
                drop(state);
                Metrics::follow_p2p_fallback_total("rpc_parent_mismatch").increment(1);
                warn!(target: "follow::p2p", block = number, after_gossip,
                    "RPC payload does not extend the previously served block");
            }
        }
        Metrics::follow_p2p_payload_source_total("rpc").increment(1);
        if self.mode.enabled() {
            // How much earlier gossip had this exact block (positive: gossip first).
            let candidates = self.cache.candidates(number);
            match candidates.iter().find(|c| c.hash == hash) {
                Some(c) => {
                    let lead_ms = rpc_ready.saturating_duration_since(c.received).as_secs_f64()
                        * 1000.0;
                    Metrics::follow_p2p_lead_seconds().record(lead_ms / 1000.0);
                    debug!(target: "follow::p2p", block = number, lead_ms = lead_ms.round() as i64,
                        "Gossip had block before RPC fetch completed");
                }
                None if candidates.is_empty() => {
                    debug!(target: "follow::p2p", block = number, "RPC payload not seen on gossip yet");
                }
                None => {
                    Metrics::follow_p2p_fallback_total("hash_mismatch").increment(1);
                    warn!(target: "follow::p2p", block = number, rpc_hash = %hash,
                        gossip_candidates = candidates.len(),
                        "RPC payload differs from every gossip payload at this height");
                }
            }
        }
        self.record_served(number, hash, false);
        Ok(payload)
    }
}

/// Logs the outcome of the network actor; it never stops the follower.
pub(super) fn log_network_exit(result: Result<(), impl std::fmt::Display>) {
    match result {
        Ok(()) => info!(target: "follow::p2p", "P2P network stopped; following RPC sources only"),
        Err(e) => warn!(target: "follow::p2p", error = %e,
            "P2P network stopped with an error; following RPC sources only"),
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, Bloom, Bytes, U256};
    use alloy_rpc_types_engine::ExecutionPayloadV1;
    use base_common_rpc_types_engine::BaseExecutionPayload;

    use super::*;
    use crate::follow::source::MockRemoteClient;

    fn hash(n: u64, variant: u8) -> B256 {
        let mut b = [0u8; 32];
        b[..8].copy_from_slice(&n.to_be_bytes());
        b[31] = variant;
        B256::from(b)
    }

    fn payload(n: u64, variant: u8, parent_variant: u8) -> BaseExecutionPayloadEnvelope {
        BaseExecutionPayloadEnvelope {
            parent_beacon_block_root: None,
            execution_payload: BaseExecutionPayload::V1(ExecutionPayloadV1 {
                parent_hash: hash(n.saturating_sub(1), parent_variant),
                fee_recipient: Address::ZERO,
                state_root: B256::ZERO,
                receipts_root: B256::ZERO,
                logs_bloom: Bloom::ZERO,
                prev_randao: B256::ZERO,
                block_number: n,
                gas_limit: 0,
                gas_used: 0,
                timestamp: 1_700_000_000 + 2 * n,
                extra_data: Bytes::new(),
                base_fee_per_gas: U256::ZERO,
                block_hash: hash(n, variant),
                transactions: vec![],
            }),
        }
    }

    fn rpc_source(variant: u8) -> MockRemoteClient {
        let mut source = MockRemoteClient::new();
        source.expect_get_payload_by_number().returning(move |n| Ok(payload(n, variant, variant)));
        source
    }

    fn served_hash(p: &BaseExecutionPayloadEnvelope) -> B256 {
        p.execution_payload.block_hash()
    }

    #[test]
    fn mode_parses_and_defaults_off() {
        assert_eq!("".parse::<FollowP2pMode>().unwrap(), FollowP2pMode::Off);
        assert_eq!("Shadow".parse::<FollowP2pMode>().unwrap(), FollowP2pMode::Shadow);
        assert_eq!("primary".parse::<FollowP2pMode>().unwrap(), FollowP2pMode::Primary);
        assert!("on".parse::<FollowP2pMode>().is_err());
        assert_eq!(FollowP2pMode::default(), FollowP2pMode::Off);
    }

    #[test]
    fn cache_dedupes_bounds_and_prunes() {
        let cache = GossipPayloadCache::default();
        let now = Instant::now();
        assert!(cache.insert(payload(10, 1, 1), now));
        assert!(!cache.insert(payload(10, 1, 1), now));
        for v in 2..=5 {
            assert!(cache.insert(payload(10, v, 1), now));
        }
        assert!(!cache.insert(payload(10, 6, 1), now), "at most 5 candidates per height");
        assert!(cache.insert(payload(10 + GOSSIP_CACHE_HEIGHTS + 5, 1, 1), now));
        assert!(cache.candidates(10).is_empty(), "old heights are pruned");
        assert!(!cache.insert(payload(11, 1, 1), now), "far-behind gossip is ignored");
        assert_eq!(cache.highest(), Some(10 + GOSSIP_CACHE_HEIGHTS + 5));
    }

    #[tokio::test]
    async fn shadow_always_reads_rpc() {
        let cache = Arc::new(GossipPayloadCache::default());
        let client = GossipFirstClient::new(rpc_source(7), Arc::clone(&cache), FollowP2pMode::Shadow);
        client.get_payload_by_number(1).await.unwrap();
        cache.insert(payload(2, 1, 7), Instant::now());
        let p = client.get_payload_by_number(2).await.unwrap();
        assert_eq!(served_hash(&p), hash(2, 7), "shadow never serves gossip");
    }

    #[tokio::test]
    async fn primary_serves_gossip_only_on_a_matching_parent() {
        let cache = Arc::new(GossipPayloadCache::default());
        let client = GossipFirstClient::new(rpc_source(7), Arc::clone(&cache), FollowP2pMode::Primary);
        // First payload after start comes from RPC (no known parent yet).
        cache.insert(payload(1, 1, 7), Instant::now());
        assert_eq!(served_hash(&client.get_payload_by_number(1).await.unwrap()), hash(1, 7));
        // Gossip block 2 extends RPC block 1: served from gossip.
        cache.insert(payload(2, 1, 7), Instant::now());
        assert_eq!(served_hash(&client.get_payload_by_number(2).await.unwrap()), hash(2, 1));
        // Gossip block 3 whose parent is not the served block 2: RPC.
        cache.insert(payload(3, 1, 9), Instant::now());
        assert_eq!(served_hash(&client.get_payload_by_number(3).await.unwrap()), hash(3, 7));
        // Missing at 4: RPC.
        assert_eq!(served_hash(&client.get_payload_by_number(4).await.unwrap()), hash(4, 7));
    }

    #[tokio::test]
    async fn primary_falls_back_on_ambiguity_and_on_repeat_requests() {
        let cache = Arc::new(GossipPayloadCache::default());
        let client = GossipFirstClient::new(rpc_source(7), Arc::clone(&cache), FollowP2pMode::Primary);
        client.get_payload_by_number(1).await.unwrap();
        // Two candidates with the right parent: ambiguous, so RPC.
        cache.insert(payload(2, 1, 7), Instant::now());
        cache.insert(payload(2, 2, 7), Instant::now());
        assert_eq!(served_hash(&client.get_payload_by_number(2).await.unwrap()), hash(2, 7));
        // Gossip 3 served once; a repeat request for 3 goes to RPC.
        cache.insert(payload(3, 1, 7), Instant::now());
        assert_eq!(served_hash(&client.get_payload_by_number(3).await.unwrap()), hash(3, 1));
        assert_eq!(served_hash(&client.get_payload_by_number(3).await.unwrap()), hash(3, 7));
    }

    #[tokio::test]
    async fn rpc_errors_pass_through_unchanged() {
        let mut source = MockRemoteClient::new();
        source
            .expect_get_payload_by_number()
            .returning(|n| Err(RemoteL2ClientError::BlockNotFound(n.to_string())));
        let client = GossipFirstClient::new(
            source,
            Arc::new(GossipPayloadCache::default()),
            FollowP2pMode::Primary,
        );
        assert!(matches!(
            client.get_payload_by_number(5).await,
            Err(RemoteL2ClientError::BlockNotFound(_))
        ));
    }

    #[tokio::test]
    async fn engine_client_wakes_prefetcher_only_in_primary() {
        let (tx, rx) = watch::channel(0u64);
        let cache = Arc::new(GossipPayloadCache::default());
        let shadow = GossipCacheEngineClient::new(
            Arc::clone(&cache),
            FollowP2pMode::Shadow,
            Some(tx.clone()),
        );
        shadow.send_unsafe_block(payload(5, 1, 1)).await.unwrap();
        assert_eq!(*rx.borrow(), 0);
        assert_eq!(cache.highest(), Some(5));

        let primary = GossipCacheEngineClient::new(Arc::clone(&cache), FollowP2pMode::Primary, Some(tx));
        primary.send_unsafe_block(payload(6, 1, 1)).await.unwrap();
        assert_eq!(*rx.borrow(), 6);
        // An older block never lowers the hint.
        primary.send_unsafe_block(payload(4, 1, 1)).await.unwrap();
        assert_eq!(*rx.borrow(), 6);
    }
}
