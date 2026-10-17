//! Optional P2P unsafe-block gossip as the first payload source of the follow node.
//!
//! The libp2p network actor validates each gossiped block (sequencer signature, timestamp
//! window, hash) exactly as in node mode and hands it to [`GossipCacheEngineClient`], which only
//! stores it in a bounded cache. The prefetcher then asks [`GossipFirstClient`] for each payload:
//!
//! - [`FollowP2pMode::Shadow`]: every payload still comes from the RPC sources. Gossip is only
//!   measured (arrival lag behind the block timestamp, and how much earlier gossip had each block
//!   than the RPC fetch completed).
//! - [`FollowP2pMode::Primary`]: a cached gossip payload is used when it is the only candidate at
//!   that height and its parent hash is the block returned just before it, including one that
//!   arrives while the RPC fetch of that height is in flight. Anything else (missing, ambiguous,
//!   parent mismatch, a second request for the same height) falls through to the RPC sources.
//!
//! The RPC sources stay authoritative. A validated gossip block can still be dropped from the
//! chain (the sequencer signed two blocks at one height, e.g. during a failover). When an RPC
//! payload then does not extend the block served before it, the primary client replaces the
//! served blocks back to the common ancestor with the RPC chain (one authoritative insert, then
//! ordinary ones) before it returns that payload, so the insert loop never sees a gap.
//!
//! A network that fails to start, closes or loses all peers leaves the follower on RPC.

use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Debug,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_eips::{BlockNumberOrTag, eip2718::Decodable2718};
use alloy_primitives::B256;
use async_trait::async_trait;
use base_common_consensus::BaseTxEnvelope;
use base_common_rpc_types_engine::{BaseExecutionPayload, BaseExecutionPayloadEnvelope};
use base_protocol::{BaseTimeUpdateTx, BlockInfo, L2BlockInfo};
use tokio::sync::{Notify, watch};
use tracing::{debug, info, warn};

use crate::{
    EngineClientResult, Metrics, NetworkEngineClient,
    follow::{
        engine::{ChainReplace, FollowEngine},
        error::FollowError,
        local::FollowLocalClient,
        source::{RemoteClient, RemoteL2ClientError},
    },
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
            other => {
                Err(format!("invalid follow P2P mode {other:?}: expected off, shadow or primary"))
            }
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
    /// Woken on every new payload (a pending RPC fetch may take the gossip copy instead).
    arrivals: Notify,
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
        if candidates.iter().any(|c| c.hash == hash)
            || candidates.len() >= MAX_CANDIDATES_PER_HEIGHT
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
        drop(map);
        self.arrivals.notify_waiters();
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

/// Block time in milliseconds. From Denim the `BaseTime` deposit at `tx[1]` carries the
/// sub-second part (about five 200 ms blocks share one header `timestamp`); before Denim there is
/// none and the header seconds are exact.
fn payload_timestamp_ms(payload: &BaseExecutionPayload) -> f64 {
    let number = payload.block_number();
    let millis = payload
        .transactions()
        .get(1)
        .and_then(|raw| BaseTxEnvelope::decode_2718(&mut raw.as_ref()).ok())
        .and_then(|transaction| {
            let deposit = transaction.as_deposit()?;
            BaseTimeUpdateTx::validate_deposit(deposit.inner(), number).ok()
        })
        .map_or(0, |base_time| base_time.timestamp_millis_part());
    payload.timestamp() as f64 * 1000.0 + f64::from(millis)
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
    async fn send_unsafe_block(
        &self,
        block: BaseExecutionPayloadEnvelope,
    ) -> EngineClientResult<()> {
        let received = Instant::now();
        let number = block.execution_payload.block_number();
        let lag_ms = unix_now_secs_f64() * 1000.0 - payload_timestamp_ms(&block.execution_payload);
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

/// Served blocks remembered for unsafe-reorg repair.
const SERVED_RING: usize = 64;

/// Deepest unsafe reorg the follower repairs by itself (blocks replaced).
const MAX_REORG_REPAIR_DEPTH: usize = 16;

/// Upper bound on reading the local chain at startup.
const LOCAL_SEED_TIMEOUT: Duration = Duration::from_secs(5);

/// Where a served (or seeded) block came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Provenance {
    /// Returned from the RPC sources.
    Rpc,
    /// Returned from validated gossip.
    Gossip,
    /// The local chain at startup (it may end in a gossip block the RPC chain dropped).
    Local,
}

impl Provenance {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Rpc => "rpc",
            Self::Gossip => "gossip",
            Self::Local => "local",
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Served {
    number: u64,
    hash: B256,
    provenance: Provenance,
}

#[derive(Debug, Default)]
struct ServeState {
    /// The last payloads returned to the prefetcher: consecutive heights, oldest first.
    served: VecDeque<Served>,
    /// The last height served from gossip; a repeat request for it goes to RPC.
    last_gossip: Option<u64>,
    /// Set by a repair: heights at or below it are never served from gossip again.
    no_gossip_through: Option<u64>,
    /// An RPC payload did not extend the served chain and its repair has not completed.
    repair_pending: bool,
}

impl ServeState {
    fn last(&self) -> Option<Served> {
        self.served.back().copied()
    }

    fn hash_at(&self, number: u64) -> Option<B256> {
        let first = self.served.front()?.number;
        let index = usize::try_from(number.checked_sub(first)?).ok()?;
        self.served.get(index).map(|s| s.hash)
    }

    fn record(&mut self, number: u64, hash: B256, provenance: Provenance) {
        while self.served.back().is_some_and(|s| s.number >= number) {
            self.served.pop_back();
        }
        if self.served.back().is_some_and(|s| s.number.saturating_add(1) != number) {
            self.served.clear();
        }
        self.served.push_back(Served { number, hash, provenance });
        while self.served.len() > SERVED_RING {
            self.served.pop_front();
        }
        if provenance == Provenance::Gossip {
            self.last_gossip = Some(number);
        }
    }
}

/// Result of racing the RPC fetch of a height against its gossip copy.
enum Fetched {
    Gossip(Candidate),
    Rpc(Result<BaseExecutionPayloadEnvelope, RemoteL2ClientError>),
}

/// A [`RemoteClient`] that answers payload requests from validated gossip first.
///
/// In [`FollowP2pMode::Primary`] with a repair engine, an RPC payload that does not extend the
/// block served just before it (the sequencer signed two blocks at one height and gossip carried
/// the one that was dropped) is not returned until the engine's unsafe chain has been replaced
/// with the RPC chain from the common ancestor; see [`FollowEngine::replace_unsafe_chain`].
#[derive(Debug)]
pub struct GossipFirstClient<R> {
    inner: R,
    cache: Arc<GossipPayloadCache>,
    mode: FollowP2pMode,
    state: Mutex<ServeState>,
    repair: Option<Arc<dyn FollowEngine>>,
}

impl<R> GossipFirstClient<R> {
    /// Wraps `inner` (the RPC sources).
    pub fn new(inner: R, cache: Arc<GossipPayloadCache>, mode: FollowP2pMode) -> Self {
        Self { inner, cache, mode, state: Mutex::new(ServeState::default()), repair: None }
    }

    /// Repairs the engine's unsafe chain through `engine` when the RPC chain drops a served block
    /// (only used in [`FollowP2pMode::Primary`]).
    pub(super) fn with_repair_engine(mut self, engine: Arc<dyn FollowEngine>) -> Self {
        self.repair = Some(engine);
        self
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, ServeState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn pick_gossip(&self, number: u64) -> Result<Candidate, &'static str> {
        let state = self.lock_state();
        if state.repair_pending || state.no_gossip_through.is_some_and(|h| number <= h) {
            return Err("reorg");
        }
        if state.last_gossip == Some(number) {
            return Err("retry");
        }
        let Some(last) = state.last() else {
            return Err("no_parent");
        };
        drop(state);
        let candidates = self.cache.candidates(number);
        if candidates.is_empty() {
            return Err("missing");
        }
        if last.number.saturating_add(1) != number {
            return Err("parent_mismatch");
        }
        let mut matching = candidates.into_iter().filter(|c| c.parent_hash == last.hash);
        match (matching.next(), matching.next()) {
            (Some(c), None) => Ok(c),
            (Some(_), Some(_)) => Err("ambiguous"),
            (None, _) => Err("parent_mismatch"),
        }
    }

    fn serve_gossip(
        &self,
        number: u64,
        candidate: Candidate,
        raced: bool,
    ) -> BaseExecutionPayloadEnvelope {
        Metrics::follow_p2p_payload_source_total("p2p").increment(1);
        debug!(target: "follow::p2p", block = number, hash = %candidate.hash, raced,
            "Serving payload from gossip");
        self.lock_state().record(number, candidate.hash, Provenance::Gossip);
        candidate.payload
    }

    fn repair_failed(number: u64, reason: &'static str, detail: String) -> RemoteL2ClientError {
        Metrics::follow_p2p_reorg_repair_failed_total(reason).increment(1);
        warn!(target: "follow::p2p", block = number, reason, detail = %detail,
            "Unsafe chain repair failed; retrying on the next fetch");
        RemoteL2ClientError::ReorgRepair { block: number, reason: detail }
    }
}

impl<R: RemoteClient> GossipFirstClient<R> {
    /// Seeds the served blocks with the local chain ending at `head` (the follower's start
    /// block), only in [`FollowP2pMode::Primary`].
    ///
    /// The local head can be a gossip block the RPC chain dropped (served just before a
    /// restart). Seeding lets the first RPC payload be checked against it and repaired like any
    /// served block. Reads `head` and up to [`MAX_REORG_REPAIR_DEPTH`] ancestors that link to it;
    /// if the local node fails to answer, seeds only `head` and its parent hash (enough for a
    /// depth-1 repair). Never fails and is bounded in time.
    pub(super) async fn seed_local_chain(&self, local: &dyn FollowLocalClient, head: L2BlockInfo) {
        if self.mode != FollowP2pMode::Primary {
            return;
        }
        let head = head.block_info;
        let chain = match tokio::time::timeout(
            LOCAL_SEED_TIMEOUT,
            Self::local_ancestors(local, head.number, head.parent_hash),
        )
        .await
        {
            Ok(Ok(ancestors)) => ancestors,
            Ok(Err(error)) => {
                warn!(target: "follow::p2p", block = head.number, %error,
                    "Reading local blocks to seed the served chain failed; seeding the start block only");
                vec![(head.number.saturating_sub(1), head.parent_hash)]
            }
            Err(_) => {
                warn!(target: "follow::p2p", block = head.number,
                    timeout_ms = LOCAL_SEED_TIMEOUT.as_millis() as u64,
                    "Reading local blocks to seed the served chain timed out; seeding the start block only");
                vec![(head.number.saturating_sub(1), head.parent_hash)]
            }
        };
        let mut state = self.lock_state();
        for (n, h) in chain {
            state.record(n, h, Provenance::Local);
        }
        state.record(head.number, head.hash, Provenance::Local);
        let from = state.served.front().map_or(head.number, |s| s.number);
        drop(state);
        info!(target: "follow::p2p", from, to = head.number, hash = %head.hash,
            "Seeded the served chain from local blocks");
    }

    /// The local blocks below `number` that link to `parent_hash`, oldest first.
    async fn local_ancestors(
        local: &dyn FollowLocalClient,
        number: u64,
        mut parent_hash: B256,
    ) -> Result<Vec<(u64, B256)>, FollowError> {
        let mut chain = Vec::new();
        for n in (number.saturating_sub(MAX_REORG_REPAIR_DEPTH as u64)..number).rev() {
            let Some(block) = local.block_info(BlockNumberOrTag::Number(n)).await? else {
                break;
            };
            if block.block_info.hash != parent_hash {
                break;
            }
            parent_hash = block.block_info.parent_hash;
            chain.push((n, block.block_info.hash));
        }
        chain.reverse();
        Ok(chain)
    }

    /// Fetches `number` from the RPC sources, but takes its gossip copy instead when one that
    /// passes [`Self::pick_gossip`] arrives before the RPC answer.
    async fn rpc_or_gossip(&self, number: u64) -> Fetched {
        let mut rpc = self.inner.get_payload_by_number(number);
        loop {
            // Registered before the cache is read, so an insert in between still wakes us.
            let arrival = self.cache.arrivals.notified();
            tokio::pin!(arrival);
            arrival.as_mut().enable();
            match self.pick_gossip(number) {
                Ok(candidate) => return Fetched::Gossip(candidate),
                Err("missing") => {}
                Err(_) => return Fetched::Rpc(rpc.await),
            }
            tokio::select! {
                biased;
                result = &mut rpc => return Fetched::Rpc(result),
                _ = &mut arrival => {}
            }
        }
    }

    /// Makes the engine's unsafe chain end at the parent of `next` (the RPC payload at
    /// `number`), replacing the served blocks the RPC chain did not keep.
    ///
    /// Walks the RPC chain back from `number - 1` to the first block whose parent is the block
    /// served at that height (the common ancestor), at most [`MAX_REORG_REPAIR_DEPTH`] blocks and
    /// never past the served blocks still remembered, checking that each RPC block is the parent
    /// of the one after it. Any failure leaves the served state as it was, so the next request
    /// for `number` retries; RPC errors are returned unchanged for the prefetcher's backoff.
    async fn repair_served_chain(
        &self,
        engine: &dyn FollowEngine,
        number: u64,
        next: &BaseExecutionPayloadEnvelope,
    ) -> Result<(), RemoteL2ClientError> {
        let mut chain: Vec<BaseExecutionPayloadEnvelope> = Vec::new();
        let mut expected = next.execution_payload.parent_hash();
        let mut height = number.saturating_sub(1);
        let ancestor = loop {
            let served_parent = height.checked_sub(1).and_then(|p| self.lock_state().hash_at(p));
            let Some(served_parent) =
                served_parent.filter(|_| chain.len() < MAX_REORG_REPAIR_DEPTH)
            else {
                return Err(Self::repair_failed(
                    number,
                    "too_deep",
                    format!(
                        "no common ancestor within {} blocks (searched down to block {height})",
                        chain.len()
                    ),
                ));
            };
            let payload = match self.inner.get_payload_by_number(height).await {
                Ok(payload) => payload,
                Err(e) => {
                    Metrics::follow_p2p_reorg_repair_failed_total("rpc_error").increment(1);
                    warn!(target: "follow::p2p", block = number, height, error = %e,
                        "Unsafe chain repair could not read an RPC block; retrying on the next fetch");
                    return Err(e);
                }
            };
            let got = &payload.execution_payload;
            if got.block_number() != height || got.block_hash() != expected {
                return Err(Self::repair_failed(
                    number,
                    "discontinuous",
                    format!(
                        "RPC block {} {} is not the expected parent {expected} at {height}",
                        got.block_number(),
                        got.block_hash()
                    ),
                ));
            }
            let parent = got.parent_hash();
            chain.push(payload);
            if parent == served_parent {
                break parent;
            }
            expected = parent;
            height -= 1;
        };
        chain.reverse();
        let canonical: Vec<(u64, B256)> = chain
            .iter()
            .map(|p| (p.execution_payload.block_number(), p.execution_payload.block_hash()))
            .collect();
        let from = canonical[0].0;
        let depth = canonical.len();
        match engine.replace_unsafe_chain(chain).await {
            Ok(ChainReplace::Applied) => {}
            Ok(ChainReplace::EngineBehind { unsafe_head }) => {
                return Err(Self::repair_failed(
                    number,
                    "engine_behind",
                    format!("engine unsafe head {unsafe_head} is below block {}", from - 1),
                ));
            }
            Ok(ChainReplace::Unexpected { number: head, hash }) => {
                return Err(Self::repair_failed(
                    number,
                    "engine_unexpected",
                    format!("engine unsafe head is {head} {hash}"),
                ));
            }
            Err(e) => return Err(Self::repair_failed(number, "engine_error", e.to_string())),
        }

        let mut state = self.lock_state();
        let replaced = state.last();
        let replaced_from = |p: Provenance| {
            state.served.iter().filter(|s| s.number >= from && s.provenance == p).count()
        };
        let (gossip_replaced, local_replaced) =
            (replaced_from(Provenance::Gossip), replaced_from(Provenance::Local));
        for &(n, h) in &canonical {
            state.record(n, h, Provenance::Rpc);
        }
        state.no_gossip_through = Some(number.saturating_sub(1));
        state.repair_pending = false;
        drop(state);
        Metrics::follow_p2p_reorg_repaired_total().increment(1);
        warn!(target: "follow::p2p", block = number, from, to = number.saturating_sub(1), depth,
            gossip_replaced, local_replaced, ancestor = %ancestor,
            replaced_hash = %replaced.map(|s| s.hash).unwrap_or_default(),
            canonical_hash = %next.execution_payload.parent_hash(),
            "Repaired unsafe chain: replaced served blocks the RPC sources did not keep");
        Ok(())
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
        let fetched = if self.mode == FollowP2pMode::Primary {
            match self.pick_gossip(number) {
                Ok(candidate) => return Ok(self.serve_gossip(number, candidate, false)),
                Err("missing") => match self.rpc_or_gossip(number).await {
                    Fetched::Gossip(candidate) => {
                        return Ok(self.serve_gossip(number, candidate, true));
                    }
                    Fetched::Rpc(result) => {
                        Metrics::follow_p2p_fallback_total("missing").increment(1);
                        result
                    }
                },
                Err(reason) => {
                    Metrics::follow_p2p_fallback_total(reason).increment(1);
                    if reason != "no_parent" {
                        warn!(target: "follow::p2p", block = number, reason,
                            "Gossip payload not used; reading from RPC sources");
                    }
                    self.inner.get_payload_by_number(number).await
                }
            }
        } else {
            self.inner.get_payload_by_number(number).await
        };

        let payload = fetched?;
        let rpc_ready = Instant::now();
        let hash = payload.execution_payload.block_hash();
        if self.mode == FollowP2pMode::Primary {
            // An RPC block that does not extend the block served just before it means the chain
            // moved under us (an unsafe reorg), most of all after a gossip block. Returning it
            // as is would make the engine drop it and then sync on the block after it.
            let parent_hash = payload.execution_payload.parent_hash();
            let (tip, first_report) = {
                let mut state = self.lock_state();
                let tip = state.last().filter(|tip| {
                    tip.number.saturating_add(1) == number && tip.hash != parent_hash
                });
                let first_report = !state.repair_pending;
                state.repair_pending = tip.is_some() && self.repair.is_some();
                (tip, first_report)
            };
            if let Some(tip) = tip {
                if first_report {
                    Metrics::follow_p2p_fallback_total("rpc_parent_mismatch").increment(1);
                    warn!(target: "follow::p2p", block = number, served_from = tip.provenance.as_str(),
                        served_hash = %tip.hash, rpc_parent_hash = %parent_hash,
                        "RPC payload does not extend the previously served block");
                }
                if let Some(engine) = &self.repair {
                    self.repair_served_chain(engine.as_ref(), number, &payload).await?;
                }
            }
        }
        Metrics::follow_p2p_payload_source_total("rpc").increment(1);
        if self.mode.enabled() {
            // How much earlier gossip had this exact block (positive: gossip first).
            let candidates = self.cache.candidates(number);
            match candidates.iter().find(|c| c.hash == hash) {
                Some(c) => {
                    let lead_ms =
                        rpc_ready.saturating_duration_since(c.received).as_secs_f64() * 1000.0;
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
        self.lock_state().record(number, hash, Provenance::Rpc);
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
    use std::{
        collections::{HashMap, HashSet},
        time::Duration,
    };

    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::{Address, Bloom, Bytes, U256};
    use alloy_rpc_types_engine::{
        ExecutionPayloadV1, ForkchoiceUpdated, PayloadStatus, PayloadStatusEnum,
    };
    use alloy_transport::TransportErrorKind;
    use base_common_consensus::{BaseBlock, BaseTxEnvelope, TxDeposit};
    use base_common_genesis::RollupConfig;
    use base_common_rpc_types_engine::BaseExecutionPayload;
    use base_consensus_engine::test_utils::{MockEngineClient, test_engine_client_builder};
    use base_protocol::{L1BlockInfoBedrock, L2BlockInfo};

    use super::*;
    use crate::follow::{
        engine::{EngineApiFollowEngine, MockFollowEngine},
        local::MockFollowLocalClient,
        source::MockRemoteClient,
    };

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

    #[test]
    fn gossip_lag_uses_denim_milliseconds() {
        use alloy_eips::eip2718::Encodable2718;
        use alloy_primitives::Sealable;
        use base_common_consensus::TxDeposit;

        let mut envelope = payload(9, 0, 0);
        let BaseExecutionPayload::V1(inner) = &mut envelope.execution_payload else {
            unreachable!()
        };
        inner.timestamp = 42;
        assert_eq!(payload_timestamp_ms(&envelope.execution_payload), 42_000.0);
        let BaseExecutionPayload::V1(inner) = &mut envelope.execution_payload else {
            unreachable!()
        };
        inner.transactions = vec![
            BaseTxEnvelope::from(TxDeposit::default().seal_slow()).encoded_2718().into(),
            BaseTxEnvelope::from(BaseTimeUpdateTx::new(600).unwrap().into_deposit_tx(9))
                .encoded_2718()
                .into(),
        ];
        assert_eq!(payload_timestamp_ms(&envelope.execution_payload), 42_600.0);
    }

    fn rpc_source(variant: u8) -> MockRemoteClient {
        let mut source = MockRemoteClient::new();
        source.expect_get_payload_by_number().returning(move |n| Ok(payload(n, variant, variant)));
        source
    }

    fn served_hash(p: &BaseExecutionPayloadEnvelope) -> B256 {
        p.execution_payload.block_hash()
    }

    /// RPC sources backed by a chain the test can change, with a call log and one-shot failures.
    #[derive(Clone, Default)]
    struct Rpc {
        chain: Arc<Mutex<HashMap<u64, BaseExecutionPayloadEnvelope>>>,
        calls: Arc<Mutex<Vec<u64>>>,
        fail_once: Arc<Mutex<HashSet<u64>>>,
    }

    impl Rpc {
        fn with(blocks: impl IntoIterator<Item = BaseExecutionPayloadEnvelope>) -> Self {
            let rpc = Self::default();
            rpc.set(blocks);
            rpc
        }

        fn set(&self, blocks: impl IntoIterator<Item = BaseExecutionPayloadEnvelope>) {
            let mut chain = self.chain.lock().unwrap();
            for b in blocks {
                chain.insert(b.execution_payload.block_number(), b);
            }
        }

        fn calls(&self) -> Vec<u64> {
            self.calls.lock().unwrap().clone()
        }

        fn client(&self) -> MockRemoteClient {
            let rpc = self.clone();
            let mut source = MockRemoteClient::new();
            source.expect_get_payload_by_number().returning(move |n| {
                rpc.calls.lock().unwrap().push(n);
                if rpc.fail_once.lock().unwrap().remove(&n) {
                    return Err(RemoteL2ClientError::FetchBlock {
                        tag: n.to_string(),
                        source: TransportErrorKind::custom_str("rpc fixture"),
                    });
                }
                rpc.chain
                    .lock()
                    .unwrap()
                    .get(&n)
                    .cloned()
                    .ok_or_else(|| RemoteL2ClientError::BlockNotFound(n.to_string()))
            });
            source
        }
    }

    /// Fake-hash chain `variant` for heights `range` (each block's parent has the same variant).
    fn fake_chain(
        range: std::ops::RangeInclusive<u64>,
        variant: u8,
    ) -> Vec<BaseExecutionPayloadEnvelope> {
        range.map(|n| payload(n, variant, variant)).collect()
    }

    /// A repair engine that records the heights of each replacement and accepts it.
    fn recording_engine(replaced: Arc<Mutex<Vec<Vec<u64>>>>) -> MockFollowEngine {
        let mut engine = MockFollowEngine::new();
        engine.expect_replace_unsafe_chain().returning(move |payloads| {
            replaced
                .lock()
                .unwrap()
                .push(payloads.iter().map(|p| p.execution_payload.block_number()).collect());
            Ok(ChainReplace::Applied)
        });
        engine
    }

    fn primary(rpc: &Rpc, cache: &Arc<GossipPayloadCache>) -> GossipFirstClient<MockRemoteClient> {
        GossipFirstClient::new(rpc.client(), Arc::clone(cache), FollowP2pMode::Primary)
    }

    // Real engine fixtures: blocks with an L1 info deposit and their true header hash, so the
    // engine's insert rules (extend, skip older, authoritative reorg) apply as in production.

    const GENESIS: B256 = B256::repeat_byte(0xaa);

    fn sealed(number: u64, parent_hash: B256, salt: u8) -> BaseExecutionPayloadEnvelope {
        let deposit = BaseTxEnvelope::from(TxDeposit {
            input: L1BlockInfoBedrock::default().encode_calldata(),
            ..Default::default()
        })
        .encoded_2718();
        let mut inner = ExecutionPayloadV1 {
            parent_hash,
            fee_recipient: Address::ZERO,
            state_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: Bloom::ZERO,
            prev_randao: B256::ZERO,
            block_number: number,
            gas_limit: 30_000_000,
            gas_used: 0,
            timestamp: number,
            extra_data: Bytes::from(vec![salt]),
            base_fee_per_gas: U256::ZERO,
            block_hash: B256::ZERO,
            transactions: vec![deposit.into()],
        };
        let block: BaseBlock =
            BaseExecutionPayload::V1(inner.clone()).try_into_block().expect("valid fixture");
        inner.block_hash = block.header.hash_slow();
        BaseExecutionPayloadEnvelope {
            parent_beacon_block_root: None,
            execution_payload: BaseExecutionPayload::V1(inner),
        }
    }

    /// Blocks `from..=to` on top of `parent`, distinguished by `salt`.
    fn sealed_chain(
        parent: B256,
        from: u64,
        to: u64,
        salt: u8,
    ) -> Vec<BaseExecutionPayloadEnvelope> {
        let mut parent = parent;
        (from..=to)
            .map(|n| {
                let block = sealed(n, parent, salt);
                parent = block.execution_payload.block_hash();
                block
            })
            .collect()
    }

    fn genesis() -> L2BlockInfo {
        L2BlockInfo {
            block_info: BlockInfo { hash: GENESIS, number: 0, ..Default::default() },
            ..Default::default()
        }
    }

    fn info_of(p: &BaseExecutionPayloadEnvelope) -> L2BlockInfo {
        L2BlockInfo {
            block_info: BlockInfo {
                hash: p.execution_payload.block_hash(),
                number: p.execution_payload.block_number(),
                parent_hash: p.execution_payload.parent_hash(),
                timestamp: p.execution_payload.timestamp(),
            },
            ..Default::default()
        }
    }

    /// A local node whose canonical chain is `blocks`.
    fn local_chain(blocks: Vec<L2BlockInfo>) -> MockFollowLocalClient {
        let mut local = MockFollowLocalClient::new();
        local.expect_block_info().returning(move |tag| {
            Ok(match tag {
                BlockNumberOrTag::Number(n) => {
                    blocks.iter().find(|b| b.block_info.number == n).copied()
                }
                _ => None,
            })
        });
        local
    }

    fn real_engine() -> (Arc<EngineApiFollowEngine<MockEngineClient>>, Arc<MockEngineClient>) {
        real_engine_at(genesis())
    }

    /// A real engine whose unsafe head is `head` (safe and finalized at genesis).
    fn real_engine_at(
        head: L2BlockInfo,
    ) -> (Arc<EngineApiFollowEngine<MockEngineClient>>, Arc<MockEngineClient>) {
        let rollup_config = Arc::new(RollupConfig::default());
        let valid = PayloadStatus { status: PayloadStatusEnum::Valid, latest_valid_hash: None };
        let client = Arc::new(
            test_engine_client_builder()
                .with_config(Arc::clone(&rollup_config))
                .with_new_payload_v2_response(valid.clone())
                .with_fork_choice_updated_v3_response(ForkchoiceUpdated {
                    payload_status: valid,
                    payload_id: None,
                })
                .build(),
        );
        let engine = Arc::new(EngineApiFollowEngine::new(
            Arc::clone(&client),
            rollup_config,
            head,
            genesis(),
            genesis(),
        ));
        (engine, client)
    }

    async fn head_hash(engine: &EngineApiFollowEngine<MockEngineClient>) -> B256 {
        engine.unsafe_head().await.block_info.hash
    }

    async fn last_new_payload(client: &MockEngineClient) -> B256 {
        client.last_new_payload_v2().await.expect("a payload was sent").execution_payload.block_hash
    }

    async fn forkchoice_heads(client: &MockEngineClient) -> Vec<B256> {
        let storage = client.storage();
        let storage = storage.read().await;
        storage.fork_choice_updated_v3_requests.iter().map(|(s, _)| s.head_block_hash).collect()
    }

    /// Canonical blocks 1..=6, a sibling gossip block 3' (same parent as canonical 3) and a
    /// gossip 4' on top of it, plus a primary client wired to the real engine.
    struct Equivocation {
        canonical: Vec<BaseExecutionPayloadEnvelope>,
        fork: Vec<BaseExecutionPayloadEnvelope>,
        rpc: Rpc,
        cache: Arc<GossipPayloadCache>,
        engine: Arc<EngineApiFollowEngine<MockEngineClient>>,
        client: Arc<MockEngineClient>,
        source: GossipFirstClient<MockRemoteClient>,
    }

    impl Equivocation {
        fn new() -> Self {
            let canonical = sealed_chain(GENESIS, 1, 6, 0);
            let fork = sealed_chain(canonical[1].execution_payload.block_hash(), 3, 4, 9);
            let rpc = Rpc::with(canonical.clone());
            let cache = Arc::new(GossipPayloadCache::default());
            let (engine, client) = real_engine();
            let source = primary(&rpc, &cache)
                .with_repair_engine(Arc::clone(&engine) as Arc<dyn FollowEngine>);
            Self { canonical, fork, rpc, cache, engine, client, source }
        }

        fn hash(&self, n: u64) -> B256 {
            self.canonical[n as usize - 1].execution_payload.block_hash()
        }

        fn fork_hash(&self, n: u64) -> B256 {
            self.fork[n as usize - 3].execution_payload.block_hash()
        }

        /// Serves and inserts canonical 1 and 2 from RPC, then serves gossip `3'..=to'`.
        async fn serve_fork(&self, to: u64) -> Vec<BaseExecutionPayloadEnvelope> {
            for n in 1..=2 {
                let p = self.source.get_payload_by_number(n).await.unwrap();
                self.engine.insert_payload(p).await.unwrap();
            }
            let mut served = Vec::new();
            for n in 3..=to {
                self.cache.insert(self.fork[n as usize - 3].clone(), Instant::now());
                let p = self.source.get_payload_by_number(n).await.unwrap();
                assert_eq!(served_hash(&p), self.fork_hash(n), "gossip {n}' is served");
                served.push(p);
            }
            served
        }
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
        let client =
            GossipFirstClient::new(rpc_source(7), Arc::clone(&cache), FollowP2pMode::Shadow);
        client.get_payload_by_number(1).await.unwrap();
        cache.insert(payload(2, 1, 7), Instant::now());
        let p = client.get_payload_by_number(2).await.unwrap();
        assert_eq!(served_hash(&p), hash(2, 7), "shadow never serves gossip");
    }

    #[tokio::test]
    async fn primary_serves_gossip_only_on_a_matching_parent() {
        let cache = Arc::new(GossipPayloadCache::default());
        let client =
            GossipFirstClient::new(rpc_source(7), Arc::clone(&cache), FollowP2pMode::Primary);
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
        let client =
            GossipFirstClient::new(rpc_source(7), Arc::clone(&cache), FollowP2pMode::Primary);
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

        let primary =
            GossipCacheEngineClient::new(Arc::clone(&cache), FollowP2pMode::Primary, Some(tx));
        primary.send_unsafe_block(payload(6, 1, 1)).await.unwrap();
        assert_eq!(*rx.borrow(), 6);
        // An older block never lowers the hint.
        primary.send_unsafe_block(payload(4, 1, 1)).await.unwrap();
        assert_eq!(*rx.borrow(), 6);
    }

    #[tokio::test]
    async fn repair_after_the_gossip_block_was_inserted_reorgs_to_canonical() {
        let t = Equivocation::new();
        let fork = t.serve_fork(3).await;
        // The insert loop already applied 3'.
        t.engine.insert_payload(fork[0].clone()).await.unwrap();
        assert_eq!(head_hash(&t.engine).await, t.fork_hash(3));

        let p4 = t.source.get_payload_by_number(4).await.unwrap();
        assert_eq!(served_hash(&p4), t.hash(4));
        assert_eq!(head_hash(&t.engine).await, t.hash(3), "authoritative insert reorged 3' -> 3");
        t.engine.insert_payload(p4).await.unwrap();
        assert_eq!(head_hash(&t.engine).await, t.hash(4), "RPC 4 now extends the head");
        let heads = forkchoice_heads(&t.client).await;
        assert_eq!(&heads[heads.len() - 3..], &[t.fork_hash(3), t.hash(3), t.hash(4)]);
        assert_eq!(t.rpc.calls(), vec![1, 2, 4, 3]);
    }

    #[tokio::test]
    async fn repair_before_the_queued_gossip_block_is_inserted_converges() {
        let t = Equivocation::new();
        let fork = t.serve_fork(3).await;
        // 3' is still queued for the insert loop when RPC 4 shows the canonical chain.
        let p4 = t.source.get_payload_by_number(4).await.unwrap();
        assert_eq!(head_hash(&t.engine).await, t.hash(3));
        // The queued 3' arrives afterwards: same height as the head, so it is skipped unsent.
        t.engine.insert_payload(fork[0].clone()).await.unwrap();
        assert_eq!(head_hash(&t.engine).await, t.hash(3));
        assert_eq!(last_new_payload(&t.client).await, t.hash(3), "3' never reaches the EL");
        t.engine.insert_payload(p4).await.unwrap();
        assert_eq!(head_hash(&t.engine).await, t.hash(4));
        assert!(!forkchoice_heads(&t.client).await.contains(&t.fork_hash(3)));
    }

    #[tokio::test]
    async fn depth_two_repair_replaces_both_gossip_blocks() {
        let t = Equivocation::new();
        let fork = t.serve_fork(4).await;
        // 3' was inserted, 4' is still queued.
        t.engine.insert_payload(fork[0].clone()).await.unwrap();

        let p5 = t.source.get_payload_by_number(5).await.unwrap();
        assert_eq!(served_hash(&p5), t.hash(5));
        assert_eq!(head_hash(&t.engine).await, t.hash(4));
        t.engine.insert_payload(fork[1].clone()).await.unwrap();
        assert_eq!(head_hash(&t.engine).await, t.hash(4), "queued 4' is skipped");
        t.engine.insert_payload(p5).await.unwrap();
        assert_eq!(head_hash(&t.engine).await, t.hash(5));
        assert_eq!(t.rpc.calls(), vec![1, 2, 5, 4, 3]);

        // Later heights follow the repaired chain: gossip 6 on canonical 5 is served again.
        t.cache.insert(t.canonical[5].clone(), Instant::now());
        let p6 = t.source.get_payload_by_number(6).await.unwrap();
        assert_eq!(served_hash(&p6), t.hash(6));
        assert_eq!(t.rpc.calls(), vec![1, 2, 5, 4, 3], "6 came from gossip");
    }

    #[tokio::test]
    async fn repair_waits_for_the_engine_to_reach_the_common_ancestor() {
        let t = Equivocation::new();
        // Served 1, 2 (RPC) and 3' (gossip) but the insert loop has not applied anything yet.
        for n in 1..=2 {
            t.source.get_payload_by_number(n).await.unwrap();
        }
        t.cache.insert(t.fork[0].clone(), Instant::now());
        let fork3 = t.source.get_payload_by_number(3).await.unwrap();
        let err = t.source.get_payload_by_number(4).await.unwrap_err();
        assert!(matches!(err, RemoteL2ClientError::ReorgRepair { block: 4, .. }), "{err:?}");
        assert!(t.client.last_new_payload_v2().await.is_none(), "nothing sent ahead of the EL");

        // The insert loop drains the queue (1, 2, 3'), then the retry repairs.
        for p in [t.canonical[0].clone(), t.canonical[1].clone(), fork3] {
            t.engine.insert_payload(p).await.unwrap();
        }
        let p4 = t.source.get_payload_by_number(4).await.unwrap();
        assert_eq!(served_hash(&p4), t.hash(4));
        assert_eq!(head_hash(&t.engine).await, t.hash(3));
    }

    async fn serve_fake_fork(
        depth: u64,
    ) -> (Result<BaseExecutionPayloadEnvelope, RemoteL2ClientError>, Vec<Vec<u64>>, Vec<u64>) {
        // RPC: chain 7. Gossip: 2..=depth+1 on chain 1, forking off RPC block 1.
        let rpc = Rpc::with(fake_chain(1..=depth + 2, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let replaced = Arc::new(Mutex::new(Vec::new()));
        let source = primary(&rpc, &cache)
            .with_repair_engine(Arc::new(recording_engine(Arc::clone(&replaced))));
        source.get_payload_by_number(1).await.unwrap();
        for n in 2..=depth + 1 {
            cache.insert(payload(n, 1, if n == 2 { 7 } else { 1 }), Instant::now());
            assert_eq!(served_hash(&source.get_payload_by_number(n).await.unwrap()), hash(n, 1));
        }
        let result = source.get_payload_by_number(depth + 2).await;
        let replaced = replaced.lock().unwrap().clone();
        (result, replaced, rpc.calls())
    }

    #[tokio::test]
    async fn repair_is_bounded_in_depth() {
        let depth = MAX_REORG_REPAIR_DEPTH as u64;
        let (result, replaced, _) = serve_fake_fork(depth).await;
        assert_eq!(served_hash(&result.unwrap()), hash(depth + 2, 7));
        assert_eq!(replaced, vec![(2..=depth + 1).collect::<Vec<_>>()]);

        let (result, replaced, calls) = serve_fake_fork(depth + 1).await;
        assert!(
            matches!(result, Err(RemoteL2ClientError::ReorgRepair { block, .. }) if block == depth + 3)
        );
        assert!(replaced.is_empty(), "nothing is inserted without a common ancestor");
        assert_eq!(calls.len(), 1 + 1 + MAX_REORG_REPAIR_DEPTH, "1, the new block, the walk");
    }

    #[tokio::test]
    async fn repair_never_walks_past_the_served_blocks() {
        let rpc = Rpc::with(fake_chain(1..=3, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let replaced = Arc::new(Mutex::new(Vec::new()));
        let source = primary(&rpc, &cache)
            .with_repair_engine(Arc::new(recording_engine(Arc::clone(&replaced))));
        source.get_payload_by_number(1).await.unwrap();
        cache.insert(payload(2, 1, 7), Instant::now());
        source.get_payload_by_number(2).await.unwrap();
        // The RPC chain now differs from block 1 on, which was the first block served.
        rpc.set(fake_chain(1..=3, 8));
        let err = source.get_payload_by_number(3).await.unwrap_err();
        assert!(matches!(err, RemoteL2ClientError::ReorgRepair { block: 3, .. }), "{err:?}");
        assert!(replaced.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn repair_rejects_a_discontinuous_rpc_chain() {
        let rpc = Rpc::with(fake_chain(1..=4, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let replaced = Arc::new(Mutex::new(Vec::new()));
        let source = primary(&rpc, &cache)
            .with_repair_engine(Arc::new(recording_engine(Arc::clone(&replaced))));
        for n in 1..=2 {
            source.get_payload_by_number(n).await.unwrap();
        }
        cache.insert(payload(3, 1, 7), Instant::now());
        source.get_payload_by_number(3).await.unwrap();
        // Block 3 from RPC is not the parent RPC block 4 names (sources disagree).
        rpc.set([payload(3, 5, 7)]);
        let err = source.get_payload_by_number(4).await.unwrap_err();
        assert!(matches!(err, RemoteL2ClientError::ReorgRepair { block: 4, .. }), "{err:?}");
        assert!(replaced.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn rpc_error_mid_repair_is_returned_and_the_retry_repairs() {
        let rpc = Rpc::with(fake_chain(1..=4, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let replaced = Arc::new(Mutex::new(Vec::new()));
        let source = primary(&rpc, &cache)
            .with_repair_engine(Arc::new(recording_engine(Arc::clone(&replaced))));
        for n in 1..=2 {
            source.get_payload_by_number(n).await.unwrap();
        }
        cache.insert(payload(3, 1, 7), Instant::now());
        assert_eq!(served_hash(&source.get_payload_by_number(3).await.unwrap()), hash(3, 1));

        rpc.fail_once.lock().unwrap().insert(3);
        let err = source.get_payload_by_number(4).await.unwrap_err();
        assert!(matches!(err, RemoteL2ClientError::FetchBlock { .. }), "{err:?}");
        assert!(replaced.lock().unwrap().is_empty());

        // The fork continues on gossip, but nothing is served from gossip until the repair.
        cache.insert(payload(4, 1, 1), Instant::now());
        let p4 = source.get_payload_by_number(4).await.unwrap();
        assert_eq!(served_hash(&p4), hash(4, 7));
        assert_eq!(*replaced.lock().unwrap(), vec![vec![3]]);
        assert_eq!(rpc.calls(), vec![1, 2, 4, 3, 4, 3]);
    }

    #[tokio::test]
    async fn shadow_never_repairs() {
        let rpc = Rpc::with(fake_chain(1..=2, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        // No expectations: any engine call panics.
        let source =
            GossipFirstClient::new(rpc.client(), Arc::clone(&cache), FollowP2pMode::Shadow)
                .with_repair_engine(Arc::new(MockFollowEngine::new()));
        for n in 1..=2 {
            source.get_payload_by_number(n).await.unwrap();
        }
        rpc.set(fake_chain(3..=3, 8));
        assert_eq!(served_hash(&source.get_payload_by_number(3).await.unwrap()), hash(3, 8));
        assert_eq!(rpc.calls(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn consistent_chain_needs_no_extra_rpc_calls() {
        let rpc = Rpc::with(fake_chain(1..=5, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let source = primary(&rpc, &cache).with_repair_engine(Arc::new(MockFollowEngine::new()));
        source.get_payload_by_number(1).await.unwrap();
        for n in [2, 3, 5] {
            cache.insert(payload(n, 7, 7), Instant::now());
        }
        for n in 2..=5 {
            assert_eq!(served_hash(&source.get_payload_by_number(n).await.unwrap()), hash(n, 7));
        }
        assert_eq!(rpc.calls(), vec![1, 4], "2, 3 and 5 came from gossip");
    }

    /// RPC sources that answer after a delay; `automock` cannot hold an answer back while a gossip
    /// block arrives.
    #[derive(Debug)]
    struct SlowRpc {
        delay: Duration,
    }

    #[async_trait]
    impl RemoteClient for SlowRpc {
        async fn get_block_number(&self, _: BlockNumberOrTag) -> Result<u64, RemoteL2ClientError> {
            Ok(0)
        }

        async fn get_block_info(
            &self,
            _: BlockNumberOrTag,
        ) -> Result<BlockInfo, RemoteL2ClientError> {
            Ok(BlockInfo::default())
        }

        async fn get_payload_by_number(
            &self,
            number: u64,
        ) -> Result<BaseExecutionPayloadEnvelope, RemoteL2ClientError> {
            if number > 1 {
                tokio::time::sleep(self.delay).await;
            }
            Ok(payload(number, 7, 7))
        }
    }

    #[tokio::test]
    async fn gossip_arriving_during_the_rpc_fetch_is_served() {
        let delay = Duration::from_secs(2);
        let cache = Arc::new(GossipPayloadCache::default());
        let source =
            GossipFirstClient::new(SlowRpc { delay }, Arc::clone(&cache), FollowP2pMode::Primary);
        source.get_payload_by_number(1).await.unwrap();

        let arrive = |p: BaseExecutionPayloadEnvelope| {
            let cache = Arc::clone(&cache);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(20)).await;
                cache.insert(p, Instant::now());
            })
        };
        arrive(payload(2, 1, 7));
        let started = Instant::now();
        let p2 = source.get_payload_by_number(2).await.unwrap();
        assert_eq!(served_hash(&p2), hash(2, 1), "gossip copy wins the race");
        assert!(started.elapsed() < delay / 2);

        // A gossip block that does not extend the served one does not end the RPC wait early.
        arrive(payload(3, 1, 9));
        let p3 = source.get_payload_by_number(3).await.unwrap();
        assert_eq!(served_hash(&p3), hash(3, 7));
    }

    /// Restart fixture: canonical 1..=5 on RPC, and a local node (and engine) whose head is the
    /// gossip block 3' that the RPC chain dropped.
    struct Restart {
        canonical: Vec<BaseExecutionPayloadEnvelope>,
        fork3: BaseExecutionPayloadEnvelope,
        rpc: Rpc,
        engine: Arc<EngineApiFollowEngine<MockEngineClient>>,
        client: Arc<MockEngineClient>,
        source: GossipFirstClient<MockRemoteClient>,
    }

    fn restart_on_fork() -> Restart {
        let canonical = sealed_chain(GENESIS, 1, 5, 0);
        let fork3 = sealed(3, canonical[1].execution_payload.block_hash(), 9);
        let rpc = Rpc::with(canonical.clone());
        let (engine, client) = real_engine_at(info_of(&fork3));
        let source = primary(&rpc, &Arc::new(GossipPayloadCache::default()))
            .with_repair_engine(Arc::clone(&engine) as Arc<dyn FollowEngine>);
        Restart { canonical, fork3, rpc, engine, client, source }
    }

    #[tokio::test]
    async fn restart_on_a_dropped_gossip_head_repairs_from_the_seeded_local_chain() {
        let Restart { canonical, fork3, rpc, engine, client, source } = restart_on_fork();
        let local = local_chain(vec![
            genesis(),
            info_of(&canonical[0]),
            info_of(&canonical[1]),
            info_of(&fork3),
        ]);
        source.seed_local_chain(&local, info_of(&fork3)).await;

        let p4 = source.get_payload_by_number(4).await.unwrap();
        assert_eq!(served_hash(&p4), canonical[3].execution_payload.block_hash());
        assert_eq!(head_hash(&engine).await, canonical[2].execution_payload.block_hash());
        engine.insert_payload(p4).await.unwrap();
        assert_eq!(head_hash(&engine).await, canonical[3].execution_payload.block_hash());
        let heads = forkchoice_heads(&client).await;
        assert_eq!(
            heads,
            vec![
                canonical[2].execution_payload.block_hash(),
                canonical[3].execution_payload.block_hash()
            ]
        );
        assert_eq!(rpc.calls(), vec![4, 3]);
    }

    #[tokio::test]
    async fn failed_local_seeding_falls_back_to_the_start_block() {
        let Restart { canonical, fork3, rpc, engine, source, .. } = restart_on_fork();
        let mut local = MockFollowLocalClient::new();
        local
            .expect_block_info()
            .times(1)
            .returning(|tag| Err(FollowError::LocalBlockUnavailable(tag)));
        source.seed_local_chain(&local, info_of(&fork3)).await;

        // The start block and its parent hash still allow a depth-1 repair.
        let p4 = source.get_payload_by_number(4).await.unwrap();
        assert_eq!(served_hash(&p4), canonical[3].execution_payload.block_hash());
        assert_eq!(head_hash(&engine).await, canonical[2].execution_payload.block_hash());
        assert_eq!(rpc.calls(), vec![4, 3]);
    }

    #[tokio::test]
    async fn consistent_local_chain_at_start_needs_no_extra_rpc_calls() {
        let canonical = sealed_chain(GENESIS, 1, 4, 0);
        let rpc = Rpc::with(canonical.clone());
        let cache = Arc::new(GossipPayloadCache::default());
        // No expectations: any engine call panics.
        let source = primary(&rpc, &cache).with_repair_engine(Arc::new(MockFollowEngine::new()));
        let local = local_chain(canonical[..3].iter().map(info_of).collect());
        source.seed_local_chain(&local, info_of(&canonical[2])).await;
        let p4 = source.get_payload_by_number(4).await.unwrap();
        assert_eq!(served_hash(&p4), canonical[3].execution_payload.block_hash());
        assert_eq!(rpc.calls(), vec![4]);

        // Shadow never reads the local chain (no expectations: a call panics).
        let shadow = GossipFirstClient::new(rpc.client(), cache, FollowP2pMode::Shadow);
        shadow.seed_local_chain(&MockFollowLocalClient::new(), info_of(&canonical[2])).await;
    }
}
