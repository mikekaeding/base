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

use alloy_eips::BlockNumberOrTag;
use alloy_primitives::B256;
use async_trait::async_trait;
use base_common_rpc_types_engine::BaseExecutionPayloadEnvelope;
use base_protocol::{BlockInfo, L2BlockInfo};
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

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

/// Served blocks remembered for unsafe-reorg repair.
const SERVED_RING: usize = 64;

/// Deepest unsafe reorg the follower repairs by itself (blocks replaced); also the local
/// ancestors seeded at startup.
///
/// A deeper reorg of the RPC chain itself (more than 24 blocks, about 48 s at 2 s blocks) wedges
/// primary with the "manual unwind needed" ERROR until someone unwinds the local chain, where
/// off mode would let the EL sync past it. Accepted: Base unsafe reorgs are far shallower.
const MAX_REORG_REPAIR_DEPTH: usize = 24;

/// Most served blocks at the tip (gossip, or the local chain seeded at startup) that the RPC
/// sources have not confirmed yet. At the cap the next payload comes from RPC, so a fork that
/// gossip keeps extending stays well inside [`MAX_REORG_REPAIR_DEPTH`]. 2.4 s of 200 ms blocks.
const MAX_UNCONFIRMED_RUN: usize = 12;

/// Upper bound on reading the local chain at startup.
const LOCAL_SEED_TIMEOUT: Duration = Duration::from_secs(5);

/// Youngest a served block may be before it is read for confirmation (gossip leads the RPC
/// sources by about 60-330 ms). Raised by 100 ms on each "not found" and lowered by 10 ms on
/// each match, within [`CONFIRM_AGE_MAX`].
const CONFIRM_AGE: Duration = Duration::from_millis(300);

/// Upper bound of the adaptive confirmation age.
const CONFIRM_AGE_MAX: Duration = Duration::from_secs(1);

/// Shortest time between two confirmation reads (and the wait after a "not found"); at
/// 200 ms blocks each read confirms one or two blocks at once.
const CONFIRM_INTERVAL: Duration = Duration::from_millis(300);

/// Wait after a failed confirmation read.
const CONFIRM_FAILURE_BACKOFF: Duration = Duration::from_secs(1);

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
    /// The RPC sources have this block (an RPC block, or one whose hash RPC returned).
    confirmed: bool,
    /// When it was served (or seeded).
    at: tokio::time::Instant,
}

#[derive(Debug)]
struct ServeState {
    /// The last payloads returned to the prefetcher: consecutive heights, oldest first.
    served: VecDeque<Served>,
    /// The last height served from gossip; a repeat request for it goes to RPC.
    last_gossip: Option<u64>,
    /// Set by a repair: heights at or below it are never served from gossip again.
    no_gossip_through: Option<u64>,
    /// An RPC payload did not extend the served chain and its repair has not completed.
    repair_pending: bool,
    /// The RPC sources returned a different block at this served height.
    rpc_disagrees: Option<u64>,
    /// The block whose repair last failed as too deep (reported once at ERROR).
    too_deep_reported: Option<u64>,
    /// The last repair failure reported at WARN (block, reason); repeats log at debug.
    last_failure: Option<(u64, &'static str)>,
    /// The RPC chain walked for a repair that waits for the engine, reused by its retries.
    walked: Option<Walked>,
    /// Current confirmation age (adapts to how far the RPC sources trail gossip).
    confirm_age: Duration,
}

impl Default for ServeState {
    fn default() -> Self {
        Self {
            served: VecDeque::new(),
            last_gossip: None,
            no_gossip_through: None,
            repair_pending: false,
            rpc_disagrees: None,
            too_deep_reported: None,
            last_failure: None,
            walked: None,
            confirm_age: CONFIRM_AGE,
        }
    }
}

impl ServeState {
    fn last(&self) -> Option<Served> {
        self.served.back().copied()
    }

    fn hash_at(&self, number: u64) -> Option<B256> {
        self.served_at(number).map(|s| s.hash)
    }

    fn served_at(&self, number: u64) -> Option<&Served> {
        let first = self.served.front()?.number;
        let index = usize::try_from(number.checked_sub(first)?).ok()?;
        self.served.get(index)
    }

    fn record(&mut self, number: u64, hash: B256, provenance: Provenance) {
        while self.served.back().is_some_and(|s| s.number >= number) {
            self.served.pop_back();
        }
        if self.served.back().is_some_and(|s| s.number.saturating_add(1) != number) {
            self.served.clear();
        }
        let confirmed = provenance == Provenance::Rpc;
        let at = tokio::time::Instant::now();
        self.served.push_back(Served { number, hash, provenance, confirmed, at });
        while self.served.len() > SERVED_RING {
            self.served.pop_front();
        }
        if provenance == Provenance::Gossip {
            self.last_gossip = Some(number);
        }
    }

    /// Marks every served block up to `number` as on the RPC chain (block hashes commit to
    /// their ancestors, and the remembered blocks are consecutive and linked).
    fn confirm_through(&mut self, number: u64) {
        for s in self.served.iter_mut().filter(|s| s.number <= number) {
            s.confirmed = true;
        }
    }

    fn unconfirmed_run(&self) -> usize {
        self.served.iter().rev().take_while(|s| !s.confirmed).count()
    }
}

/// An RPC chain walked (and verified) back to the common ancestor for the repair before `number`.
#[derive(Debug, Clone)]
struct Walked {
    number: u64,
    /// Parent hash of the RPC payload at `number` the walk started from.
    tip: B256,
    ancestor: B256,
    /// Oldest first.
    chain: Vec<BaseExecutionPayloadEnvelope>,
}

/// Result of racing the RPC fetch of a height against its gossip copy.
enum Fetched {
    Gossip(Candidate),
    /// The RPC answer and why gossip was not used.
    Rpc(Result<BaseExecutionPayloadEnvelope, RemoteL2ClientError>, &'static str),
}

/// Outcome of one RPC confirmation read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Confirm {
    /// Nothing to confirm (or a repair is due first).
    Idle,
    /// No unconfirmed block is old enough yet; the oldest one is in this long.
    NotDue(Duration),
    /// A block was confirmed (or the served chain changed meanwhile); look again at once.
    Progress,
    /// The RPC sources have a different block at a served height.
    Disagrees,
    /// The RPC sources do not have the block yet.
    NotYet,
    /// The read failed.
    Failed,
}

/// A [`RemoteClient`] that answers payload requests from validated gossip first.
///
/// In [`FollowP2pMode::Primary`] with a repair engine, an RPC payload that does not extend the
/// block served just before it (the sequencer signed two blocks at one height and gossip carried
/// the one that was dropped) is not returned until the engine's unsafe chain has been replaced
/// with the RPC chain from the common ancestor; see [`FollowEngine::replace_unsafe_chain`].
/// [`Self::run_confirmations`] checks each gossip block against the RPC sources in the
/// background; a disagreement, or [`MAX_UNCONFIRMED_RUN`] unconfirmed blocks, sends the next
/// fetch to RPC.
#[derive(Debug)]
pub struct GossipFirstClient<R> {
    inner: R,
    cache: Arc<GossipPayloadCache>,
    mode: FollowP2pMode,
    state: Mutex<ServeState>,
    repair: Option<Arc<dyn FollowEngine>>,
    /// Wakes [`Self::run_confirmations`] when there is a new block to confirm.
    confirm_wake: Notify,
}

impl<R> GossipFirstClient<R> {
    /// Wraps `inner` (the RPC sources).
    pub fn new(inner: R, cache: Arc<GossipPayloadCache>, mode: FollowP2pMode) -> Self {
        Self {
            inner,
            cache,
            mode,
            state: Mutex::new(ServeState::default()),
            repair: None,
            confirm_wake: Notify::new(),
        }
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
        if state.rpc_disagrees.is_some() {
            return Err("rpc_disagrees");
        }
        if state.last_gossip == Some(number) {
            return Err("retry");
        }
        let Some(last) = state.last() else {
            return Err("no_parent");
        };
        if state.unconfirmed_run() >= MAX_UNCONFIRMED_RUN {
            return Err("unconfirmed_run");
        }
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
        self.confirm_wake.notify_one();
        candidate.payload
    }

    /// Counts a failed repair attempt; WARN once per block and reason, debug on retries.
    fn repair_failed(
        &self,
        number: u64,
        reason: &'static str,
        detail: String,
    ) -> RemoteL2ClientError {
        Metrics::follow_p2p_reorg_repair_failed_total(reason).increment(1);
        let first =
            self.lock_state().last_failure.replace((number, reason)) != Some((number, reason));
        if first {
            warn!(target: "follow::p2p", block = number, reason, detail = %detail,
                "Unsafe chain repair failed; retrying on the next fetch");
        } else {
            debug!(target: "follow::p2p", block = number, reason, detail = %detail,
                "Unsafe chain repair failed again");
        }
        RemoteL2ClientError::ReorgRepair { block: number, reason: detail }
    }
}

impl<R: RemoteClient> GossipFirstClient<R> {
    /// Seeds the served blocks with the local chain ending at `head` (the follower's start
    /// block), only in [`FollowP2pMode::Primary`].
    ///
    /// The local head can be a gossip block the RPC chain dropped (served just before a
    /// restart). Seeding lets the first RPC payload be checked against it and repaired like any
    /// served block; seeded blocks count as unconfirmed. Reads `head` and up to
    /// [`MAX_REORG_REPAIR_DEPTH`] ancestors that link to it; if the local node fails to answer,
    /// seeds only `head` and its parent hash (enough for a depth-1 repair). Never fails, is
    /// bounded in time and stops on `cancellation`.
    pub(super) async fn seed_local_chain(
        &self,
        local: &dyn FollowLocalClient,
        head: L2BlockInfo,
        cancellation: &CancellationToken,
    ) {
        if self.mode != FollowP2pMode::Primary {
            return;
        }
        let head = head.block_info;
        let read = tokio::time::timeout(
            LOCAL_SEED_TIMEOUT,
            Self::local_ancestors(local, head.number, head.parent_hash),
        );
        let chain = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return,
            read = read => match read {
                // No linked local ancestor: still seed the parent hash for a depth-1 repair.
                Ok(Ok(ancestors)) if ancestors.is_empty() => {
                    vec![(head.number.saturating_sub(1), head.parent_hash)]
                }
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
            },
        };
        let from = {
            let mut state = self.lock_state();
            for (n, h) in chain {
                state.record(n, h, Provenance::Local);
            }
            state.record(head.number, head.hash, Provenance::Local);
            state.served.front().map_or(head.number, |s| s.number)
        };
        self.confirm_wake.notify_one();
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

    /// Confirms served blocks against the RPC sources until `cancellation`, only needed in
    /// [`FollowP2pMode::Primary`].
    ///
    /// One header read at a time, of the newest unconfirmed block at least the confirmation age
    /// old (a match confirms everything below it), via
    /// [`RemoteClient::confirm_block_hash`] so "not found" never fans out to the fallback
    /// sources. At most one read per [`CONFIRM_INTERVAL`] while gossip flows, whatever the
    /// block rate.
    pub(super) async fn run_confirmations(
        self: Arc<Self>,
        cancellation: CancellationToken,
    ) -> Result<(), FollowError> {
        loop {
            let outcome = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                outcome = self.confirm_next() => outcome,
            };
            let wait = match outcome {
                Confirm::Idle => {
                    tokio::select! {
                        _ = cancellation.cancelled() => return Ok(()),
                        _ = self.confirm_wake.notified() => {}
                    }
                    continue;
                }
                Confirm::NotDue(wait) => wait,
                Confirm::Disagrees => continue,
                Confirm::Progress | Confirm::NotYet => CONFIRM_INTERVAL,
                Confirm::Failed => CONFIRM_FAILURE_BACKOFF,
            };
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }

    /// Reads the newest unconfirmed served block that is old enough from the RPC sources once.
    async fn confirm_next(&self) -> Confirm {
        let target = {
            let state = self.lock_state();
            if state.repair_pending || state.rpc_disagrees.is_some() {
                return Confirm::Idle;
            }
            let now = tokio::time::Instant::now();
            let age = state.confirm_age;
            let Some(oldest) = state.served.iter().find(|s| !s.confirmed) else {
                return Confirm::Idle;
            };
            match state
                .served
                .iter()
                .rev()
                .find(|s| !s.confirmed && now.saturating_duration_since(s.at) >= age)
            {
                Some(s) => (s.number, s.hash),
                None => return Confirm::NotDue(age.saturating_sub(now - oldest.at)),
            }
        };
        let (number, hash) = target;
        let rpc_hash = match self.inner.confirm_block_hash(number).await {
            Ok(rpc_hash) => rpc_hash,
            Err(RemoteL2ClientError::BlockNotFound(_)) => {
                let mut state = self.lock_state();
                state.confirm_age =
                    (state.confirm_age + Duration::from_millis(100)).min(CONFIRM_AGE_MAX);
                return Confirm::NotYet;
            }
            Err(error) => {
                debug!(target: "follow::p2p", block = number, %error, "Confirmation read failed");
                return Confirm::Failed;
            }
        };
        {
            let mut state = self.lock_state();
            if state.hash_at(number) != Some(hash) {
                // Replaced meanwhile (a repair or a newer RPC block); look again.
                return Confirm::Progress;
            }
            if rpc_hash == hash {
                state.confirm_through(number);
                state.confirm_age =
                    state.confirm_age.saturating_sub(Duration::from_millis(10)).max(CONFIRM_AGE);
                return Confirm::Progress;
            }
            state.rpc_disagrees = Some(number);
        }
        warn!(target: "follow::p2p", block = number, served_hash = %hash, %rpc_hash,
            "RPC sources have a different block than the one served; reading from RPC until repaired");
        Confirm::Disagrees
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
                Err(reason) => return Fetched::Rpc(rpc.await, reason),
            }
            tokio::select! {
                biased;
                result = &mut rpc => return Fetched::Rpc(result, "missing"),
                _ = &mut arrival => {}
            }
        }
    }

    /// Makes the engine's unsafe chain end at the parent of `next` (the RPC payload at
    /// `number`), replacing the served blocks the RPC chain did not keep.
    ///
    /// See [`Self::walk_rpc_chain`]. A repair that waits for the engine keeps the walked chain,
    /// so its retries read only the payload at `number` (as an RPC-only follower would). Any
    /// failure leaves the served state as it was, so the next request for `number` retries; RPC
    /// errors are returned unchanged for the prefetcher's backoff.
    async fn repair_served_chain(
        &self,
        engine: &dyn FollowEngine,
        number: u64,
        next: &BaseExecutionPayloadEnvelope,
    ) -> Result<(), RemoteL2ClientError> {
        let tip = next.execution_payload.parent_hash();
        let cached = self.lock_state().walked.take().filter(|w| w.number == number && w.tip == tip);
        let walked = match cached {
            Some(walked) => walked,
            None => self.walk_rpc_chain(number, tip).await?,
        };
        let ancestor = walked.ancestor;
        let canonical: Vec<(u64, B256)> = walked
            .chain
            .iter()
            .map(|p| (p.execution_payload.block_number(), p.execution_payload.block_hash()))
            .collect();
        let from = canonical[0].0;
        let depth = canonical.len();

        match engine.replace_unsafe_chain(walked.chain.clone()).await {
            Ok(ChainReplace::Applied) => {}
            Ok(ChainReplace::EngineBehind { unsafe_head }) => {
                // Normal while the insert loop drains the queue; retried shortly.
                Metrics::follow_p2p_reorg_repair_failed_total("engine_behind").increment(1);
                debug!(target: "follow::p2p", block = number, unsafe_head, from,
                    "Unsafe chain repair waits for the engine to reach the common ancestor");
                self.lock_state().walked = Some(walked);
                return Err(RemoteL2ClientError::EngineBehind { block: number, unsafe_head });
            }
            Ok(ChainReplace::BelowSafe { safe_head }) => {
                return Err(self.repair_failed(
                    number,
                    "below_safe",
                    format!("block {from} is at or below the safe head {safe_head}"),
                ));
            }
            Ok(ChainReplace::TimedOut) => {
                return Err(self.repair_failed(
                    number,
                    "engine_timeout",
                    "engine replacement timed out".to_owned(),
                ));
            }
            Ok(ChainReplace::Unexpected { number: head, hash }) => {
                return Err(self.repair_failed(
                    number,
                    "engine_unexpected",
                    format!("engine unsafe head is {head} {hash}"),
                ));
            }
            Err(e) => return Err(self.repair_failed(number, "engine_error", e.to_string())),
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
        state.confirm_through(number);
        state.no_gossip_through = Some(number.saturating_sub(1));
        state.repair_pending = false;
        state.rpc_disagrees = None;
        state.too_deep_reported = None;
        state.last_failure = None;
        drop(state);
        Metrics::follow_p2p_reorg_repaired_total().increment(1);
        warn!(target: "follow::p2p", block = number, from, to = number.saturating_sub(1), depth,
            gossip_replaced, local_replaced, ancestor = %ancestor,
            replaced_hash = %replaced.map(|s| s.hash).unwrap_or_default(),
            canonical_hash = %next.execution_payload.parent_hash(),
            "Repaired unsafe chain: replaced served blocks the RPC sources did not keep");
        Ok(())
    }

    /// Walks the RPC chain back from `number - 1` (whose hash is `tip`) to the first block whose
    /// parent is the block served at that height (the common ancestor), at most
    /// [`MAX_REORG_REPAIR_DEPTH`] blocks and never past the served blocks still remembered,
    /// checking that each RPC block is the parent of the one after it.
    ///
    /// Unless the replaced blocks include an unconfirmed gossip block (the expected case: the
    /// sequencer equivocated and gossip carried the dropped block), the served blocks are
    /// probably canonical, so one odd backend read must not reorg them: every walked hash must
    /// read the same a second time.
    async fn walk_rpc_chain(&self, number: u64, tip: B256) -> Result<Walked, RemoteL2ClientError> {
        let mut chain: Vec<BaseExecutionPayloadEnvelope> = Vec::new();
        let mut expected = tip;
        let mut height = number.saturating_sub(1);
        let ancestor = loop {
            let served_parent = height.checked_sub(1).and_then(|p| self.lock_state().hash_at(p));
            let Some(served_parent) =
                served_parent.filter(|_| chain.len() < MAX_REORG_REPAIR_DEPTH)
            else {
                return Err(self.too_deep(number, chain.len(), height));
            };
            let payload = match self.inner.get_payload_by_number(height).await {
                Ok(payload) => payload,
                Err(e) => {
                    Metrics::follow_p2p_reorg_repair_failed_total("rpc_error").increment(1);
                    debug!(target: "follow::p2p", block = number, height, error = %e,
                        "Unsafe chain repair could not read an RPC block; retrying on the next fetch");
                    return Err(e);
                }
            };
            let got = &payload.execution_payload;
            if got.block_number() != height || got.block_hash() != expected {
                return Err(self.repair_failed(
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
        let from = chain[0].execution_payload.block_number();

        let unconfirmed_gossip = self
            .lock_state()
            .served
            .iter()
            .any(|s| s.number >= from && s.provenance == Provenance::Gossip && !s.confirmed);
        if !unconfirmed_gossip {
            for p in &chain {
                let (n, h) = (p.execution_payload.block_number(), p.execution_payload.block_hash());
                match self.inner.get_block_info(BlockNumberOrTag::Number(n)).await {
                    Ok(info) if info.hash == h => {}
                    Ok(info) => {
                        return Err(self.repair_failed(
                            number,
                            "rpc_unstable",
                            format!("RPC block {n} read as {h}, then as {}", info.hash),
                        ));
                    }
                    Err(e) => {
                        Metrics::follow_p2p_reorg_repair_failed_total("rpc_error").increment(1);
                        return Err(e);
                    }
                }
            }
        }
        Ok(Walked { number, tip, ancestor, chain })
    }

    /// A repair that found no common ancestor: reported at ERROR once per block.
    fn too_deep(&self, number: u64, searched: usize, lowest: u64) -> RemoteL2ClientError {
        Metrics::follow_p2p_reorg_repair_failed_total("too_deep").increment(1);
        let first = {
            let mut state = self.lock_state();
            state.too_deep_reported.replace(number) != Some(number)
        };
        if first {
            error!(target: "follow::p2p", block = number, searched, lowest,
                max_depth = MAX_REORG_REPAIR_DEPTH,
                "No common ancestor with the RPC chain within the repair depth; manual unwind needed");
        } else {
            debug!(target: "follow::p2p", block = number, "Unsafe chain repair still too deep");
        }
        RemoteL2ClientError::ReorgTooDeep { block: number, searched }
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

    async fn confirm_block_hash(&self, number: u64) -> Result<B256, RemoteL2ClientError> {
        self.inner.confirm_block_hash(number).await
    }

    async fn get_payload_by_number(
        &self,
        number: u64,
    ) -> Result<BaseExecutionPayloadEnvelope, RemoteL2ClientError> {
        let fetched = if self.mode == FollowP2pMode::Primary {
            let (result, reason) = match self.pick_gossip(number) {
                Ok(candidate) => return Ok(self.serve_gossip(number, candidate, false)),
                Err("missing") => match self.rpc_or_gossip(number).await {
                    Fetched::Gossip(candidate) => {
                        return Ok(self.serve_gossip(number, candidate, true));
                    }
                    Fetched::Rpc(result, reason) => (result, reason),
                },
                Err(reason) => (self.inner.get_payload_by_number(number).await, reason),
            };
            Metrics::follow_p2p_fallback_total(reason).increment(1);
            if matches!(reason, "parent_mismatch" | "ambiguous" | "retry") {
                warn!(target: "follow::p2p", block = number, reason,
                    "Gossip payload not used; reading from RPC sources");
            } else {
                debug!(target: "follow::p2p", block = number, reason,
                    "Gossip payload not used; reading from RPC sources");
            }
            result
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
                let tip = state.last().filter(|tip| tip.number.saturating_add(1) == number);
                let mismatch = tip.filter(|tip| tip.hash != parent_hash);
                if tip.is_some() && mismatch.is_none() {
                    // RPC extends the served chain: everything served so far is on it.
                    state.confirm_through(number);
                    state.rpc_disagrees = None;
                }
                let first_report = !state.repair_pending;
                state.repair_pending = mismatch.is_some() && self.repair.is_some();
                (mismatch, first_report)
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
        proof_gate::NoopProofGate,
        runtime::FollowRuntime,
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
        /// Payload reads.
        calls: Arc<Mutex<Vec<u64>>>,
        /// Header (block info) reads.
        info_calls: Arc<Mutex<Vec<u64>>>,
        fail_once: Arc<Mutex<HashSet<u64>>>,
        /// Header hashes that differ from the payload chain (a flapping source).
        info_override: Arc<Mutex<HashMap<u64, B256>>>,
        /// Every read fails (an outage).
        down: Arc<std::sync::atomic::AtomicBool>,
    }

    fn outage(n: u64) -> RemoteL2ClientError {
        RemoteL2ClientError::FetchBlock {
            tag: n.to_string(),
            source: TransportErrorKind::custom_str("rpc outage fixture"),
        }
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

        fn info_calls(&self) -> Vec<u64> {
            self.info_calls.lock().unwrap().clone()
        }

        fn client(&self) -> MockRemoteClient {
            let mut source = MockRemoteClient::new();
            let rpc = self.clone();
            source.expect_get_block_number().returning(move |_| {
                Ok(rpc.chain.lock().unwrap().keys().max().copied().unwrap_or_default())
            });
            let rpc = self.clone();
            source.expect_confirm_block_hash().returning(move |n| {
                rpc.info_calls.lock().unwrap().push(n);
                if rpc.down.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err(outage(n));
                }
                let chain = rpc.chain.lock().unwrap();
                let block = chain
                    .get(&n)
                    .ok_or_else(|| RemoteL2ClientError::BlockNotFound(n.to_string()))?;
                let hash = rpc.info_override.lock().unwrap().get(&n).copied();
                Ok(hash.unwrap_or_else(|| block.execution_payload.block_hash()))
            });
            let rpc = self.clone();
            source.expect_get_block_info().returning(move |tag| {
                let BlockNumberOrTag::Number(n) = tag else {
                    return Err(RemoteL2ClientError::BlockNotFound(format!("{tag:?}")));
                };
                rpc.info_calls.lock().unwrap().push(n);
                let chain = rpc.chain.lock().unwrap();
                let block = chain
                    .get(&n)
                    .ok_or_else(|| RemoteL2ClientError::BlockNotFound(n.to_string()))?;
                let p = &block.execution_payload;
                let hash = rpc.info_override.lock().unwrap().get(&n).copied();
                Ok(BlockInfo {
                    hash: hash.unwrap_or_else(|| p.block_hash()),
                    number: n,
                    parent_hash: p.parent_hash(),
                    timestamp: p.timestamp(),
                })
            });
            let rpc = self.clone();
            source.expect_get_payload_by_number().returning(move |n| {
                rpc.calls.lock().unwrap().push(n);
                if rpc.down.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err(outage(n));
                }
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
        let engine = Arc::new(
            EngineApiFollowEngine::new(
                Arc::clone(&client),
                rollup_config,
                head,
                genesis(),
                genesis(),
            )
            .with_strict_extension(true),
        );
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
        assert!(matches!(err, RemoteL2ClientError::EngineBehind { block: 4, .. }), "{err:?}");
        assert!(t.client.last_new_payload_v2().await.is_none(), "nothing sent ahead of the EL");

        // The insert loop drains the queue (1, 2, 3'), then the retry repairs.
        for p in [t.canonical[0].clone(), t.canonical[1].clone(), fork3] {
            t.engine.insert_payload(p).await.unwrap();
        }
        let p4 = t.source.get_payload_by_number(4).await.unwrap();
        assert_eq!(served_hash(&p4), t.hash(4));
        assert_eq!(head_hash(&t.engine).await, t.hash(3));
        // The retry reused the walked chain: only RPC 4 was read again.
        assert_eq!(t.rpc.calls(), vec![1, 2, 4, 3, 4]);
    }

    /// Restarts on a local fork of `depth` blocks (2..=depth+1 on chain 1, forking off block 1
    /// of RPC chain 7) and fetches the next RPC block.
    async fn restart_on_fake_fork(
        depth: u64,
    ) -> (GossipFirstClient<MockRemoteClient>, Rpc, Arc<Mutex<Vec<Vec<u64>>>>) {
        let rpc = Rpc::with(fake_chain(1..=depth + 2, 7));
        let replaced = Arc::new(Mutex::new(Vec::new()));
        let source = primary(&rpc, &Arc::new(GossipPayloadCache::default()))
            .with_repair_engine(Arc::new(recording_engine(Arc::clone(&replaced))));
        let mut local: Vec<_> = vec![payload(1, 7, 7), payload(2, 1, 7)];
        local.extend((3..=depth + 1).map(|n| payload(n, 1, 1)));
        let head = info_of(local.last().unwrap());
        let local = local_chain(local.iter().map(info_of).collect());
        source.seed_local_chain(&local, head, &CancellationToken::new()).await;
        (source, rpc, replaced)
    }

    #[tokio::test]
    async fn repair_is_bounded_in_depth() {
        let depth = MAX_REORG_REPAIR_DEPTH as u64;
        let (source, _, replaced) = restart_on_fake_fork(depth).await;
        let p = source.get_payload_by_number(depth + 2).await.unwrap();
        assert_eq!(served_hash(&p), hash(depth + 2, 7));
        assert_eq!(*replaced.lock().unwrap(), vec![(2..=depth + 1).collect::<Vec<_>>()]);

        let (source, rpc, replaced) = restart_on_fake_fork(depth + 1).await;
        for _ in 0..2 {
            // Distinct error on every retry (the ERROR log is once per block).
            let err = source.get_payload_by_number(depth + 3).await.unwrap_err();
            assert!(
                matches!(err, RemoteL2ClientError::ReorgTooDeep { block, searched }
                    if block == depth + 3 && searched == MAX_REORG_REPAIR_DEPTH),
                "{err:?}"
            );
        }
        assert!(replaced.lock().unwrap().is_empty(), "nothing is inserted without an ancestor");
        assert_eq!(rpc.calls().len(), 2 * (1 + MAX_REORG_REPAIR_DEPTH), "new block plus the walk");
    }

    #[tokio::test]
    async fn gossip_fork_stops_being_served_after_the_unconfirmed_cap() {
        let cap = MAX_UNCONFIRMED_RUN as u64;
        let rpc = Rpc::with(fake_chain(1..=cap + 4, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let replaced = Arc::new(Mutex::new(Vec::new()));
        let source = primary(&rpc, &cache)
            .with_repair_engine(Arc::new(recording_engine(Arc::clone(&replaced))));
        source.get_payload_by_number(1).await.unwrap();
        // A second sequencer keeps extending its fork on gossip; nothing confirms it.
        for n in 2..=cap + 4 {
            cache.insert(payload(n, 1, if n == 2 { 7 } else { 1 }), Instant::now());
        }
        for n in 2..=cap + 1 {
            assert_eq!(served_hash(&source.get_payload_by_number(n).await.unwrap()), hash(n, 1));
        }
        // At the cap the next block comes from RPC, which shows the fork and repairs it.
        let p = source.get_payload_by_number(cap + 2).await.unwrap();
        assert_eq!(served_hash(&p), hash(cap + 2, 7));
        assert_eq!(*replaced.lock().unwrap(), vec![(2..=cap + 1).collect::<Vec<_>>()]);
        // No gossip at repaired heights; the fork's next block does not extend the RPC chain.
        assert_eq!(
            served_hash(&source.get_payload_by_number(cap + 3).await.unwrap()),
            hash(cap + 3, 7)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn rpc_confirmation_mismatch_sends_the_next_fetch_to_rpc_and_repairs() {
        let rpc = Rpc::with(fake_chain(1..=4, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let replaced = Arc::new(Mutex::new(Vec::new()));
        let source = primary(&rpc, &cache)
            .with_repair_engine(Arc::new(recording_engine(Arc::clone(&replaced))));
        source.get_payload_by_number(1).await.unwrap();
        cache.insert(payload(2, 1, 7), Instant::now());
        assert_eq!(served_hash(&source.get_payload_by_number(2).await.unwrap()), hash(2, 1));
        assert!(matches!(source.confirm_next().await, Confirm::NotDue(_)), "too young");
        tokio::time::advance(CONFIRM_AGE).await;
        assert_eq!(source.confirm_next().await, Confirm::Disagrees);
        assert_eq!(source.confirm_next().await, Confirm::Idle, "one read per disagreement");

        // The fork goes on, but the next block is read from RPC and repairs 2' -> 2.
        cache.insert(payload(3, 1, 1), Instant::now());
        let p3 = source.get_payload_by_number(3).await.unwrap();
        assert_eq!(served_hash(&p3), hash(3, 7));
        assert_eq!(*replaced.lock().unwrap(), vec![vec![2]]);
        assert_eq!(rpc.calls(), vec![1, 3, 2]);
        assert_eq!(rpc.info_calls(), vec![2]);
    }

    #[tokio::test(start_paused = true)]
    async fn confirmation_reads_only_the_newest_block_old_enough() {
        let rpc = Rpc::with(fake_chain(1..=8, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let source = primary(&rpc, &cache).with_repair_engine(Arc::new(MockFollowEngine::new()));
        source.get_payload_by_number(1).await.unwrap();
        for n in 2..=6 {
            cache.insert(payload(n, 7, 7), Instant::now());
            source.get_payload_by_number(n).await.unwrap();
        }
        tokio::time::advance(Duration::from_millis(150)).await;
        for n in 7..=8 {
            cache.insert(payload(n, 7, 7), Instant::now());
            source.get_payload_by_number(n).await.unwrap();
        }
        tokio::time::advance(CONFIRM_AGE - Duration::from_millis(150)).await;
        // One read of 6, the newest block old enough, confirms 2..=6; 7 and 8 are not due yet.
        assert_eq!(source.confirm_next().await, Confirm::Progress);
        assert_eq!(rpc.info_calls(), vec![6]);
        assert_eq!(source.lock_state().unconfirmed_run(), 2);
        assert!(matches!(source.confirm_next().await, Confirm::NotDue(_)));
        tokio::time::advance(Duration::from_millis(150)).await;
        assert_eq!(source.confirm_next().await, Confirm::Progress);
        assert_eq!(rpc.info_calls(), vec![6, 8]);
        assert_eq!(source.confirm_next().await, Confirm::Idle);
        assert_eq!(rpc.calls(), vec![1], "every block after 1 came from gossip");
    }

    #[tokio::test(start_paused = true)]
    async fn confirmation_waits_for_blocks_the_rpc_sources_lack() {
        let rpc = Rpc::with(fake_chain(1..=1, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let source = primary(&rpc, &cache);
        source.get_payload_by_number(1).await.unwrap();
        cache.insert(payload(2, 7, 7), Instant::now());
        source.get_payload_by_number(2).await.unwrap();
        tokio::time::advance(CONFIRM_AGE).await;
        assert_eq!(source.confirm_next().await, Confirm::NotYet);
        rpc.set(fake_chain(2..=2, 7));
        // The age grew after "not found": not due again until 100 ms later.
        assert!(matches!(source.confirm_next().await, Confirm::NotDue(_)));
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(source.confirm_next().await, Confirm::Progress);
    }

    /// RPC sources whose blocks appear `lag` after their slot (block `n` at `n * block_time`).
    /// Hand-rolled: answers depend on the (paused) clock, and the header path must not be used.
    #[derive(Debug, Clone)]
    struct LaggingRpc {
        start: tokio::time::Instant,
        block_time: Duration,
        lag: Duration,
        payloads: Arc<Mutex<Vec<u64>>>,
        reads: Arc<Mutex<Vec<(u64, bool)>>>,
    }

    #[async_trait]
    impl RemoteClient for LaggingRpc {
        async fn get_block_number(&self, _: BlockNumberOrTag) -> Result<u64, RemoteL2ClientError> {
            Ok(0)
        }

        async fn get_block_info(
            &self,
            _: BlockNumberOrTag,
        ) -> Result<BlockInfo, RemoteL2ClientError> {
            panic!("confirmations use confirm_block_hash, which never fans out on not found");
        }

        async fn get_payload_by_number(
            &self,
            number: u64,
        ) -> Result<BaseExecutionPayloadEnvelope, RemoteL2ClientError> {
            self.payloads.lock().unwrap().push(number);
            Ok(payload(number, 7, 7))
        }

        async fn confirm_block_hash(&self, number: u64) -> Result<B256, RemoteL2ClientError> {
            let available = self.start + self.block_time * number as u32 + self.lag;
            let found = tokio::time::Instant::now() >= available;
            self.reads.lock().unwrap().push((number, found));
            if found {
                Ok(hash(number, 7))
            } else {
                Err(RemoteL2ClientError::BlockNotFound(number.to_string()))
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_lagging_400_ms_at_200_ms_blocks_never_hits_the_cap() {
        let block_time = Duration::from_millis(200);
        let rpc = LaggingRpc {
            start: tokio::time::Instant::now(),
            block_time,
            lag: Duration::from_millis(400),
            payloads: Arc::default(),
            reads: Arc::default(),
        };
        let cache = Arc::new(GossipPayloadCache::default());
        let source = Arc::new(GossipFirstClient::new(
            rpc.clone(),
            Arc::clone(&cache),
            FollowP2pMode::Primary,
        ));
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(Arc::clone(&source).run_confirmations(cancellation.clone()));
        source.get_payload_by_number(1).await.unwrap();
        let blocks = 150u64; // 30 s
        let mut max_run = 0;
        for n in 2..=blocks {
            tokio::time::sleep_until(rpc.start + block_time * n as u32).await;
            max_run = max_run.max(source.lock_state().unconfirmed_run());
            cache.insert(payload(n, 7, 7), Instant::now());
            assert_eq!(served_hash(&source.get_payload_by_number(n).await.unwrap()), hash(n, 7));
        }
        cancellation.cancel();
        task.await.unwrap().unwrap();
        assert_eq!(*rpc.payloads.lock().unwrap(), vec![1], "all gossip, cap never hit");
        assert!(max_run < MAX_UNCONFIRMED_RUN / 2, "max unconfirmed run {max_run}");
        let reads = rpc.reads.lock().unwrap().clone();
        let seconds = (block_time * blocks as u32).as_secs_f64();
        let rate = reads.len() as f64 / seconds;
        assert!(rate <= 4.0, "{} reads in {seconds} s", reads.len());
        let misses = reads.iter().filter(|(_, found)| !found).count();
        assert!(misses * 4 <= reads.len(), "the age adapts to the lag: {misses} misses");
        eprintln!("lag test: {} reads, {misses} misses, max run {max_run}", reads.len());
    }

    #[tokio::test]
    async fn rpc_only_reorg_needs_a_second_read_that_agrees() {
        let rpc = Rpc::with(fake_chain(1..=3, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let replaced = Arc::new(Mutex::new(Vec::new()));
        let source = primary(&rpc, &cache)
            .with_repair_engine(Arc::new(recording_engine(Arc::clone(&replaced))));
        for n in 1..=3 {
            source.get_payload_by_number(n).await.unwrap();
        }
        // The RPC chain itself moves at 3 (no gossip involved); one header read flaps.
        rpc.set([payload(3, 8, 7), payload(4, 7, 8)]);
        rpc.info_override.lock().unwrap().insert(3, hash(3, 9));
        let err = source.get_payload_by_number(4).await.unwrap_err();
        assert!(matches!(err, RemoteL2ClientError::ReorgRepair { block: 4, .. }), "{err:?}");
        assert!(replaced.lock().unwrap().is_empty());

        rpc.info_override.lock().unwrap().clear();
        assert_eq!(served_hash(&source.get_payload_by_number(4).await.unwrap()), hash(4, 7));
        assert_eq!(*replaced.lock().unwrap(), vec![vec![3]]);
    }

    #[tokio::test]
    async fn primary_repair_end_to_end_through_prefetcher_and_insert_loop() {
        let canonical = sealed_chain(GENESIS, 1, 8, 0);
        let fork3 = sealed(3, canonical[1].execution_payload.block_hash(), 9);
        let rpc = Rpc::with(canonical.clone());
        let cache = Arc::new(GossipPayloadCache::default());
        cache.insert(fork3.clone(), Instant::now());
        let (engine, client) = real_engine();
        let source = Arc::new(
            primary(&rpc, &cache).with_repair_engine(Arc::clone(&engine) as Arc<dyn FollowEngine>),
        );
        let mut local = MockFollowLocalClient::new();
        local.expect_block_info().returning(|_| Ok(None));
        let cancellation = CancellationToken::new();
        let runtime = FollowRuntime::new(
            Arc::new(local),
            Arc::clone(&source),
            Arc::clone(&engine) as Arc<dyn FollowEngine>,
            cancellation.clone(),
            genesis(),
            NoopProofGate,
            Duration::ZERO,
        );
        let run = tokio::spawn(runtime.start(None));

        let tip = canonical[7].execution_payload.block_hash();
        let deadline = Instant::now() + Duration::from_secs(5);
        while head_hash(&engine).await != tip && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cancellation.cancel();
        run.await.unwrap().unwrap();
        assert_eq!(head_hash(&engine).await, tip, "the follower reached canonical 8");
        let calls = rpc.calls();
        // 3' came from gossip; 4 from RPC triggered the repair of 3 (3 read once; 4 re-read
        // while the engine had not reached block 2 yet).
        let middle = &calls[2..calls.len() - 4];
        assert_eq!(&calls[..2], &[1, 2], "{calls:?}");
        assert_eq!(&calls[calls.len() - 4..], &[5, 6, 7, 8], "{calls:?}");
        assert_eq!(&middle[..2], &[4, 3], "{calls:?}");
        assert!(middle[2..].iter().all(|&n| n == 4), "{calls:?}");
        // Every forkchoice head is on a chain the EL can serve: 3' at most, then canonical.
        let heads = forkchoice_heads(&client).await;
        let canonical_hashes: Vec<_> =
            canonical.iter().map(|p| p.execution_payload.block_hash()).collect();
        assert!(heads.iter().all(|h| canonical_hashes.contains(h) || *h == fork3.execution_payload.block_hash()));
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
        assert!(matches!(err, RemoteL2ClientError::ReorgTooDeep { block: 3, .. }), "{err:?}");
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
        source.seed_local_chain(&local, info_of(&fork3), &CancellationToken::new()).await;

        let p4 = source.get_payload_by_number(4).await.unwrap();
        assert_eq!(served_hash(&p4), canonical[3].execution_payload.block_hash());
        assert_eq!(head_hash(&engine).await, canonical[2].execution_payload.block_hash());
        assert_eq!(rpc.info_calls(), vec![3], "seeded blocks need a second read before a reorg");
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
        source.seed_local_chain(&local, info_of(&fork3), &CancellationToken::new()).await;

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
        source.seed_local_chain(&local, info_of(&canonical[2]), &CancellationToken::new()).await;
        let p4 = source.get_payload_by_number(4).await.unwrap();
        assert_eq!(served_hash(&p4), canonical[3].execution_payload.block_hash());
        assert_eq!(rpc.calls(), vec![4]);

        // Shadow never reads the local chain (no expectations: a call panics).
        let shadow = GossipFirstClient::new(rpc.client(), cache, FollowP2pMode::Shadow);
        shadow
            .seed_local_chain(
                &MockFollowLocalClient::new(),
                info_of(&canonical[2]),
                &CancellationToken::new(),
            )
            .await;
    }

    #[tokio::test]
    async fn confirmation_task_confirms_served_gossip_and_stops_on_cancellation() {
        let rpc = Rpc::with(fake_chain(1..=3, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let source = Arc::new(primary(&rpc, &cache));
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(Arc::clone(&source).run_confirmations(cancellation.clone()));
        source.get_payload_by_number(1).await.unwrap();
        for n in 2..=3 {
            cache.insert(payload(n, 7, 7), Instant::now());
            source.get_payload_by_number(n).await.unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(2);
        while source.lock_state().unconfirmed_run() > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(source.lock_state().unconfirmed_run(), 0);
        cancellation.cancel();
        tokio::time::timeout(Duration::from_secs(1), task).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn seeding_stops_on_cancellation() {
        let rpc = Rpc::with(fake_chain(1..=2, 7));
        let source = primary(&rpc, &Arc::new(GossipPayloadCache::default()));
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        // No expectations: a local read panics.
        source
            .seed_local_chain(
                &MockFollowLocalClient::new(),
                info_of(&payload(1, 7, 7)),
                &cancellation,
            )
            .await;
        assert!(source.lock_state().last().is_none());
    }

    #[tokio::test]
    async fn partial_repair_with_a_queued_three_block_fork_never_reaches_the_el() {
        let canonical = sealed_chain(GENESIS, 1, 7, 0);
        let fork = sealed_chain(canonical[1].execution_payload.block_hash(), 3, 5, 9);
        let rpc = Rpc::with(canonical.clone());
        let cache = Arc::new(GossipPayloadCache::default());
        let (engine, client) = real_engine();
        let source =
            primary(&rpc, &cache).with_repair_engine(Arc::clone(&engine) as Arc<dyn FollowEngine>);
        for n in 1..=2 {
            engine.insert_payload(source.get_payload_by_number(n).await.unwrap()).await.unwrap();
        }
        // Gossip 3', 4', 5' served and still queued for the insert loop.
        for p in &fork {
            cache.insert(p.clone(), Instant::now());
        }
        for n in 3..=5 {
            source.get_payload_by_number(n).await.unwrap();
        }
        // A repair that applied canonical 3 and then failed (timeout, engine error).
        assert_eq!(
            engine.replace_unsafe_chain(vec![canonical[2].clone()]).await.unwrap(),
            ChainReplace::Applied
        );
        let hash = |i: usize| canonical[i].execution_payload.block_hash();
        // The insert loop drains the queued fork: 3' (height), 4' (parent) and 5' (gap) are
        // all skipped; nothing of the fork reaches newPayload or forkchoice.
        for p in &fork {
            engine.insert_payload(p.clone()).await.unwrap();
        }
        assert_eq!(head_hash(&engine).await, hash(2));
        assert_eq!(last_new_payload(&client).await, hash(2));
        let fork_hashes: Vec<_> = fork.iter().map(|p| p.execution_payload.block_hash()).collect();
        assert!(!forkchoice_heads(&client).await.iter().any(|h| fork_hashes.contains(h)));

        // The prefetcher's retry of 6 completes the repair; the insert loop's next block is 6.
        let p6 = source.get_payload_by_number(6).await.unwrap();
        assert_eq!(head_hash(&engine).await, hash(4));
        engine.insert_payload(p6).await.unwrap();
        assert_eq!(head_hash(&engine).await, hash(5));
    }

    #[tokio::test]
    async fn confirmed_gossip_needs_a_second_read_before_a_reorg() {
        // RPC first agrees with gossip 2', so the confirmation marks it confirmed.
        let rpc = Rpc::with([payload(1, 7, 7), payload(2, 1, 7)]);
        let cache = Arc::new(GossipPayloadCache::default());
        let replaced = Arc::new(Mutex::new(Vec::new()));
        let source = primary(&rpc, &cache)
            .with_repair_engine(Arc::new(recording_engine(Arc::clone(&replaced))));
        source.get_payload_by_number(1).await.unwrap();
        cache.insert(payload(2, 1, 7), Instant::now());
        source.get_payload_by_number(2).await.unwrap();
        tokio::time::pause();
        tokio::time::advance(CONFIRM_AGE).await;
        assert_eq!(source.confirm_next().await, Confirm::Progress);
        // Then one backend shows another 2 and 3; the second read of 2 disagrees.
        rpc.set([payload(2, 7, 7), payload(3, 7, 7)]);
        rpc.info_override.lock().unwrap().insert(2, hash(2, 9));
        let err = source.get_payload_by_number(3).await.unwrap_err();
        assert!(matches!(err, RemoteL2ClientError::ReorgRepair { block: 3, .. }), "{err:?}");
        assert!(replaced.lock().unwrap().is_empty(), "no reorg on one odd read");
        rpc.info_override.lock().unwrap().clear();
        assert_eq!(served_hash(&source.get_payload_by_number(3).await.unwrap()), hash(3, 7));
        assert_eq!(*replaced.lock().unwrap(), vec![vec![2]]);
    }

    #[tokio::test]
    async fn seeding_without_a_linked_local_ancestor_still_allows_a_depth_one_repair() {
        let Restart { canonical, fork3, rpc, engine, source, .. } = restart_on_fork();
        // The local node answers, but its block 2 is not 3''s parent.
        let unrelated = sealed(2, GENESIS, 7);
        let local = local_chain(vec![info_of(&unrelated), info_of(&fork3)]);
        source.seed_local_chain(&local, info_of(&fork3), &CancellationToken::new()).await;
        let p4 = source.get_payload_by_number(4).await.unwrap();
        assert_eq!(served_hash(&p4), canonical[3].execution_payload.block_hash());
        assert_eq!(head_hash(&engine).await, canonical[2].execution_payload.block_hash());
        assert_eq!(rpc.calls(), vec![4, 3]);
    }

    #[tokio::test]
    async fn depth_cap_and_end_of_served_blocks_are_separate_limits() {
        // 30 RPC blocks served, then the RPC chain reorgs from block 3 (28 blocks deep).
        let rpc = Rpc::with(fake_chain(1..=30, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let replaced = Arc::new(Mutex::new(Vec::new()));
        let source = primary(&rpc, &cache)
            .with_repair_engine(Arc::new(recording_engine(Arc::clone(&replaced))));
        for n in 1..=30 {
            source.get_payload_by_number(n).await.unwrap();
        }
        let mut reorg = vec![payload(3, 8, 7)];
        reorg.extend(fake_chain(4..=31, 8));
        rpc.set(reorg);
        let err = source.get_payload_by_number(31).await.unwrap_err();
        assert!(
            matches!(err, RemoteL2ClientError::ReorgTooDeep { block: 31, searched }
                if searched == MAX_REORG_REPAIR_DEPTH),
            "the depth cap stops the walk while served blocks remain: {err:?}"
        );
        assert!(source.lock_state().hash_at(30 - MAX_REORG_REPAIR_DEPTH as u64).is_some());
        assert!(replaced.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn gossip_continues_through_an_rpc_outage_up_to_the_cap_then_resumes() {
        let cap = MAX_UNCONFIRMED_RUN as u64;
        let rpc = Rpc::with(fake_chain(1..=cap + 6, 7));
        let cache = Arc::new(GossipPayloadCache::default());
        let source = Arc::new(primary(&rpc, &cache));
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(Arc::clone(&source).run_confirmations(cancellation.clone()));
        source.get_payload_by_number(1).await.unwrap();
        rpc.down.store(true, std::sync::atomic::Ordering::SeqCst);
        for n in 2..=cap + 6 {
            cache.insert(payload(n, 7, 7), Instant::now());
        }
        for n in 2..=cap + 1 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            assert_eq!(served_hash(&source.get_payload_by_number(n).await.unwrap()), hash(n, 7));
        }
        // At the cap the follower needs RPC, which is down: it waits (the prefetcher retries).
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(source.get_payload_by_number(cap + 2).await.is_err());
        }
        // RPC returns: the next block comes from RPC, confirms the run, and gossip resumes.
        rpc.down.store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(
            served_hash(&source.get_payload_by_number(cap + 2).await.unwrap()),
            hash(cap + 2, 7)
        );
        let calls_before = rpc.calls().len();
        for n in cap + 3..=cap + 6 {
            assert_eq!(served_hash(&source.get_payload_by_number(n).await.unwrap()), hash(n, 7));
        }
        assert_eq!(rpc.calls().len(), calls_before, "gossip again after RPC returned");
        cancellation.cancel();
        task.await.unwrap().unwrap();
    }

    /// RPC sources whose confirmation read never answers (a legacy source without timeouts).
    #[derive(Debug)]
    struct HangingRpc;

    #[async_trait]
    impl RemoteClient for HangingRpc {
        async fn get_block_number(&self, _: BlockNumberOrTag) -> Result<u64, RemoteL2ClientError> {
            Ok(0)
        }

        async fn get_block_info(
            &self,
            _: BlockNumberOrTag,
        ) -> Result<BlockInfo, RemoteL2ClientError> {
            std::future::pending().await
        }

        async fn get_payload_by_number(
            &self,
            number: u64,
        ) -> Result<BaseExecutionPayloadEnvelope, RemoteL2ClientError> {
            Ok(payload(number, 7, 7))
        }

        async fn confirm_block_hash(&self, _: u64) -> Result<B256, RemoteL2ClientError> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_does_not_wait_for_a_hanging_confirmation_read() {
        let cache = Arc::new(GossipPayloadCache::default());
        let source = Arc::new(GossipFirstClient::new(
            HangingRpc,
            Arc::clone(&cache),
            FollowP2pMode::Primary,
        ));
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(Arc::clone(&source).run_confirmations(cancellation.clone()));
        source.get_payload_by_number(1).await.unwrap();
        cache.insert(payload(2, 7, 7), Instant::now());
        source.get_payload_by_number(2).await.unwrap();
        tokio::time::sleep(CONFIRM_AGE * 2).await; // the read is in flight
        cancellation.cancel();
        tokio::time::timeout(Duration::from_millis(10), task).await.unwrap().unwrap().unwrap();
    }
}
