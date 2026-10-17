//! Delivery-based peer selection: keep the peers that deliver blocks first, prune the rest down to a
//! target count, and keep a few slots for new peers on probation so better ones keep being found.
//!
//! ## What is measured
//!
//! Gossipsub hands the application only the first copy of each message (`propagation_source`).
//! Later copies are dropped by message id inside gossipsub and, with gossipsub v1.2, mostly never
//! sent at all: our IDONTWANT reaches the other mesh peers first. So the observable per-peer
//! delivery signal is "did this peer deliver the block first?". For every accepted block each peer
//! that could have delivered it (in our mesh for that topic, or the source itself) gets one
//! *eligible* observation, and the source gets one *win*. The delivery score is the EWMA (by block
//! count, `half_life_blocks`) of wins over eligible blocks, shrunk towards an average-peer prior
//! for peers with few observations, and divided by `1 + recent self-disconnects` (reliability).
//! A peer that is in the mesh for a long time and never delivers first is both useless for speed
//! and our most expensive peer: we forward every block to it.
//!
//! ## What is done with it
//!
//! Every `interval`:
//! - peers that sent invalid blocks are pruned (never the static ones);
//! - when more than `keep` non-probation peers are connected (static and stored-protected peers
//!   count towards `keep`), the lowest-scored prunable ones are disconnected, at most
//!   `max_prunes` per interval;
//! - when more than `keep + explore` peers are connected, the youngest probation peers beyond the
//!   cap are disconnected;
//! - a pruned peer is blocked for `cooldown`, so discovery does not redial it at once;
//! - static peers that are not connected are dialed;
//! - the best peers (id + dialable address) are written to `store`, and dialed first after a
//!   restart.
//!
//! New outbound dials are skipped while `keep + explore` peers are connected or being dialed.
//! Inbound connections beyond that cap are closed unless the peer is static.

use std::{
    collections::{HashMap, HashSet},
    io::Write,
    net::IpAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use libp2p::{Multiaddr, PeerId, multiaddr::Protocol};

/// Weight (in eligible blocks) of the average-peer prior in the delivery score.
const PRIOR_BLOCKS: f64 = 30.0;

/// Eligible weight of a block for a connected peer outside our mesh for its topic.
pub const NON_MESH_WEIGHT: f64 = 0.25;

/// Peers kept in the stats table after they disconnected (for reconnect reliability).
const FORGET_AFTER: Duration = Duration::from_secs(2 * 60 * 60);

/// Upper bound on the stats table.
const MAX_TRACKED: usize = 1024;

/// Invalid blocks after which a peer is pruned.
const INVALID_LIMIT: u32 = 3;

/// Half-life of the self-disconnect count used for reliability.
const DISCONNECT_HALF_LIFE: Duration = Duration::from_secs(60 * 60);

/// A connection shorter than this that the remote ends counts as a self-disconnect.
const SHORT_CONNECTION: Duration = Duration::from_secs(10 * 60);

/// Minimum time between two writes of the store file.
const STORE_EVERY: Duration = Duration::from_secs(5 * 60);

/// Peer selection settings. Disabled unless a [`PeerSelectConfig`] is given to the driver.
#[derive(Debug, Clone, PartialEq)]
pub struct PeerSelectConfig {
    /// Peers kept for their delivery score (static and protected stored peers count here).
    pub keep: usize,
    /// Extra slots for new peers on probation.
    pub explore: usize,
    /// How long a new peer is on probation (never pruned for its score).
    pub probation: Duration,
    /// How often the selection runs.
    pub interval: Duration,
    /// Most peers pruned for their score in one run.
    pub max_prunes: usize,
    /// How long a pruned peer is blocked from reconnecting.
    pub cooldown: Duration,
    /// Half-life of the delivery score, in accepted blocks.
    pub half_life_blocks: f64,
    /// Best peers written to and read from the store.
    pub store_peers: usize,
    /// Optional file with the best peers, read at start and rewritten while running.
    pub store: Option<PathBuf>,
    /// Whether peers read from the store are protected (never pruned) until they disconnect.
    pub protect_stored: bool,
    /// Peers that are always kept and redialed (multiaddrs ending in `/p2p/<peer id>`).
    pub static_peers: Vec<Multiaddr>,
}

impl Default for PeerSelectConfig {
    fn default() -> Self {
        Self {
            keep: 8,
            explore: 4,
            probation: Duration::from_secs(10 * 60),
            interval: Duration::from_secs(60),
            max_prunes: 2,
            cooldown: Duration::from_secs(60 * 60),
            half_life_blocks: 900.0,
            store_peers: 8,
            store: None,
            protect_stored: false,
            static_peers: Vec::new(),
        }
    }
}

impl PeerSelectConfig {
    /// The connected-peer target: `keep + explore`.
    pub const fn target(&self) -> usize {
        self.keep + self.explore
    }

    /// Checks the settings.
    pub fn validate(&self) -> Result<(), String> {
        if self.keep < 2 {
            return Err(format!("peer selection keep must be at least 2, got {}", self.keep));
        }
        if self.interval.is_zero() {
            return Err("peer selection interval must be positive".into());
        }
        if !(self.half_life_blocks.is_finite() && self.half_life_blocks >= 1.0) {
            return Err(format!(
                "peer selection half-life must be at least 1 block, got {}",
                self.half_life_blocks
            ));
        }
        for addr in &self.static_peers {
            if peer_id_of(addr).is_none() {
                return Err(format!("static peer {addr} has no /p2p/<peer id>"));
            }
        }
        Ok(())
    }
}

/// Why a peer is disconnected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneReason {
    /// Lowest delivery score while above `keep`.
    Score,
    /// Sent invalid blocks.
    Invalid,
    /// Above `keep + explore` (youngest probation peers first).
    Excess,
}

impl PruneReason {
    /// Metric label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Score => "prune_score",
            Self::Invalid => "prune_invalid",
            Self::Excess => "prune_excess",
        }
    }
}

/// What one selection run asks the driver to do.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SelectActions {
    /// Peers to disconnect and block for the cooldown.
    pub prune: Vec<(PeerId, PruneReason)>,
    /// Peers whose cooldown ended: unblock them in the connection gate.
    pub unblock: Vec<PeerId>,
    /// Addresses to dial (static peers that are not connected).
    pub dial: Vec<Multiaddr>,
}

/// Per-peer delivery statistics.
#[derive(Debug, Clone)]
pub struct PeerDelivery {
    /// When the current connection was established (`None`: not connected).
    pub connected_since: Option<Instant>,
    /// When the last connection closed.
    pub disconnected_at: Option<Instant>,
    /// Decayed first deliveries.
    pub wins: f64,
    /// Decayed blocks the peer could have delivered (in our mesh, or the source).
    pub eligible: f64,
    /// Block sequence the two counters were last decayed to.
    seq: u64,
    /// Lifetime first deliveries (not decayed; for logs).
    pub total_wins: u64,
    /// Invalid blocks received from this peer.
    pub invalid: u32,
    /// Decayed count of connections the remote ended early.
    pub disconnects: f64,
    /// When `disconnects` was last decayed.
    disconnects_at: Instant,
    /// Last ping round trip.
    pub rtt: Option<Duration>,
    /// A dialable address (`.../p2p/<id>`): the one we dialed, or a public identify listen address.
    pub addr: Option<Multiaddr>,
    /// Whether the current connection was dialed by us.
    pub outbound: bool,
}

impl PeerDelivery {
    const fn new(now: Instant, seq: u64) -> Self {
        Self {
            connected_since: None,
            disconnected_at: None,
            wins: 0.0,
            eligible: 0.0,
            seq,
            total_wins: 0,
            invalid: 0,
            disconnects: 0.0,
            disconnects_at: now,
            rtt: None,
            addr: None,
            outbound: false,
        }
    }

    fn decay_to(&mut self, seq: u64, half_life: f64) {
        if seq > self.seq {
            let f = 0.5f64.powf((seq - self.seq) as f64 / half_life);
            self.wins *= f;
            self.eligible *= f;
            self.seq = seq;
        }
    }

    fn decayed_disconnects(&self, now: Instant) -> f64 {
        let age = now.saturating_duration_since(self.disconnects_at).as_secs_f64();
        self.disconnects * 0.5f64.powf(age / DISCONNECT_HALF_LIFE.as_secs_f64())
    }
}

/// Delivery-based peer selection state. Pure bookkeeping: the driver feeds it events and applies
/// the returned [`SelectActions`].
#[derive(Debug)]
pub struct PeerSelector {
    config: PeerSelectConfig,
    peers: HashMap<PeerId, PeerDelivery>,
    /// Accepted blocks seen (the decay clock).
    seq: u64,
    /// Static peers (always protected).
    static_ids: HashSet<PeerId>,
    /// Stored peers protected until their first disconnect (`protect_stored`).
    stored_protected: HashSet<PeerId>,
    /// Pruned peers and when their block ends.
    cooldown: HashMap<PeerId, Instant>,
    /// When the store was last written.
    stored_at: Option<Instant>,
}

impl PeerSelector {
    /// Creates the selector.
    pub fn new(config: PeerSelectConfig) -> Self {
        let static_ids = config.static_peers.iter().filter_map(peer_id_of).collect();
        Self {
            config,
            peers: HashMap::new(),
            seq: 0,
            static_ids,
            stored_protected: HashSet::new(),
            cooldown: HashMap::new(),
            stored_at: None,
        }
    }

    /// The settings.
    pub const fn config(&self) -> &PeerSelectConfig {
        &self.config
    }

    /// Static peer ids.
    pub const fn static_ids(&self) -> &HashSet<PeerId> {
        &self.static_ids
    }

    /// Whether the peer is never pruned (static, or a protected stored peer).
    pub fn is_protected(&self, peer: &PeerId) -> bool {
        self.static_ids.contains(peer) || self.stored_protected.contains(peer)
    }

    /// Whether the peer is blocked by a prune cooldown.
    pub fn in_cooldown(&self, peer: &PeerId, now: Instant) -> bool {
        self.cooldown.get(peer).is_some_and(|until| *until > now)
    }

    /// Delivery stats of one peer.
    pub fn stats(&self, peer: &PeerId) -> Option<&PeerDelivery> {
        self.peers.get(peer)
    }

    fn entry(&mut self, peer: PeerId, now: Instant) -> &mut PeerDelivery {
        let seq = self.seq;
        self.peers.entry(peer).or_insert_with(|| PeerDelivery::new(now, seq))
    }

    /// Records an accepted block that `source` delivered first. Every `connected` peer gets an
    /// eligible observation: a full one when it is in our mesh for the block's topic (`in_mesh`),
    /// [`NON_MESH_WEIGHT`] otherwise, so peers that never join the mesh slowly lose their prior.
    pub fn on_block(
        &mut self,
        source: PeerId,
        connected: &[PeerId],
        in_mesh: impl Fn(&PeerId) -> bool,
        now: Instant,
    ) {
        self.seq += 1;
        let (seq, hl) = (self.seq, self.config.half_life_blocks);
        for peer in connected {
            if *peer == source {
                continue;
            }
            let w = if in_mesh(peer) { 1.0 } else { NON_MESH_WEIGHT };
            let p = self.entry(*peer, now);
            p.decay_to(seq, hl);
            p.eligible += w;
        }
        let p = self.entry(source, now);
        p.decay_to(seq, hl);
        p.eligible += 1.0;
        p.wins += 1.0;
        p.total_wins += 1;
    }

    /// Records an invalid block from `peer`.
    pub fn on_invalid(&mut self, peer: PeerId, now: Instant) {
        let p = self.entry(peer, now);
        p.invalid = p.invalid.saturating_add(1);
    }

    /// Records a new connection. `addr` is the dialed address for outbound connections.
    pub fn on_connected(&mut self, peer: PeerId, outbound: Option<Multiaddr>, now: Instant) {
        let p = self.entry(peer, now);
        if p.connected_since.is_none() {
            p.connected_since = Some(now);
        }
        p.outbound = outbound.is_some();
        if let Some(addr) = outbound.and_then(|a| with_peer_id(a, peer)) {
            p.addr = Some(addr);
        }
    }

    /// Records that the last connection to `peer` closed. `by_us` when we pruned it.
    pub fn on_disconnected(&mut self, peer: PeerId, by_us: bool, now: Instant) {
        self.stored_protected.remove(&peer);
        let Some(p) = self.peers.get_mut(&peer) else { return };
        let lived = p.connected_since.map(|t| now.saturating_duration_since(t));
        p.connected_since = None;
        p.disconnected_at = Some(now);
        if !by_us && lived.is_some_and(|d| d < SHORT_CONNECTION) {
            p.disconnects = p.decayed_disconnects(now) + 1.0;
            p.disconnects_at = now;
        }
    }

    /// Records a ping round trip.
    pub fn on_rtt(&mut self, peer: PeerId, rtt: Duration) {
        if let Some(p) = self.peers.get_mut(&peer) {
            p.rtt = Some(rtt);
        }
    }

    /// Records identify listen addresses; keeps the first public TCP one as the dialable address
    /// for peers that connected to us.
    pub fn on_identify(&mut self, peer: PeerId, listen_addrs: &[Multiaddr]) {
        let Some(p) = self.peers.get_mut(&peer) else { return };
        if p.outbound && p.addr.is_some() {
            return;
        }
        if let Some(addr) = listen_addrs
            .iter()
            .find(|a| is_public_tcp(a))
            .cloned()
            .and_then(|a| with_peer_id(a, peer))
        {
            p.addr = Some(addr);
        }
    }

    /// The delivery score: higher is better. Average peers score about `1 / target`.
    ///
    /// The prior is scaled by [`rtt_weight`]: on trader-va every peer that ever delivered a block
    /// first was within 20 ms, and none of the peers 50 ms or more away did, so a far newcomer
    /// starts low and a near one at the average.
    pub fn score(&self, peer: &PeerId, now: Instant) -> f64 {
        let Some(p) = self.peers.get(peer) else { return self.prior() };
        let f = 0.5f64.powf(self.seq.saturating_sub(p.seq) as f64 / self.config.half_life_blocks);
        let (wins, eligible) = (p.wins * f, p.eligible * f);
        let prior = self.prior() * rtt_weight(p.rtt);
        let rate = PRIOR_BLOCKS.mul_add(prior, wins) / (eligible + PRIOR_BLOCKS);
        rate / (1.0 + p.decayed_disconnects(now))
    }

    fn prior(&self) -> f64 {
        1.0 / self.config.target().max(1) as f64
    }

    /// Whether a new outbound dial is allowed with `connected` peers and `pending` dials.
    pub fn allow_dial(
        &self,
        peer: &PeerId,
        connected: usize,
        pending: usize,
        now: Instant,
    ) -> bool {
        if self.static_ids.contains(peer) {
            return true;
        }
        !self.in_cooldown(peer, now) && connected + pending < self.config.target()
    }

    /// Whether to keep a just-established inbound connection when `connected` peers (including
    /// it) are connected.
    pub fn allow_inbound(&self, peer: &PeerId, connected: usize, now: Instant) -> bool {
        if self.is_protected(peer) {
            return true;
        }
        !self.in_cooldown(peer, now) && connected <= self.config.target()
    }

    /// Marks stored peers as protected (with `protect_stored`) before they are dialed.
    pub fn protect_stored(&mut self, peers: impl IntoIterator<Item = PeerId>) {
        if self.config.protect_stored {
            self.stored_protected.extend(peers);
        }
    }

    /// One selection run over the currently `connected` peers.
    pub fn select(&mut self, connected: &[PeerId], now: Instant) -> SelectActions {
        let mut actions = SelectActions::default();

        // Cooldowns that ended.
        self.cooldown.retain(|peer, until| {
            if *until <= now {
                actions.unblock.push(*peer);
                false
            } else {
                true
            }
        });

        let connected_set: HashSet<PeerId> = connected.iter().copied().collect();
        let mut pruned: HashSet<PeerId> = HashSet::new();

        // Invalid senders first.
        for peer in connected {
            if self.is_protected(peer) {
                continue;
            }
            if self.peers.get(peer).is_some_and(|p| p.invalid >= INVALID_LIMIT) {
                actions.prune.push((*peer, PruneReason::Invalid));
                pruned.insert(*peer);
            }
        }

        let on_probation = |p: Option<&PeerDelivery>| {
            p.and_then(|p| p.connected_since)
                .is_none_or(|t| now.saturating_duration_since(t) < self.config.probation)
        };
        let remaining: Vec<PeerId> =
            connected.iter().copied().filter(|p| !pruned.contains(p)).collect();
        let protected = remaining.iter().filter(|p| self.is_protected(p)).count();
        let mut established: Vec<PeerId> = remaining
            .iter()
            .copied()
            .filter(|p| !self.is_protected(p) && !on_probation(self.peers.get(p)))
            .collect();
        let mut probation: Vec<PeerId> = remaining
            .iter()
            .copied()
            .filter(|p| !self.is_protected(p) && on_probation(self.peers.get(p)))
            .collect();

        // Lowest scores beyond `keep`.
        let excess = (protected + established.len()).saturating_sub(self.config.keep);
        if excess > 0 {
            established.sort_by(|a, b| {
                self.score(a, now)
                    .total_cmp(&self.score(b, now))
                    .then_with(|| self.rtt_rank(b).cmp(&self.rtt_rank(a)))
            });
            for peer in established.iter().take(excess.min(self.config.max_prunes)) {
                actions.prune.push((*peer, PruneReason::Score));
                pruned.insert(*peer);
            }
        }

        // Hard cap: youngest probation peers beyond `keep + explore`.
        let left = remaining.len() - remaining.iter().filter(|p| pruned.contains(p)).count();
        let over = left.saturating_sub(self.config.target());
        if over > 0 {
            probation.sort_by_key(|p| {
                std::cmp::Reverse(self.peers.get(p).and_then(|s| s.connected_since))
            });
            let excess: Vec<PeerId> =
                probation.iter().filter(|p| !pruned.contains(p)).take(over).copied().collect();
            for peer in excess {
                actions.prune.push((peer, PruneReason::Excess));
                pruned.insert(peer);
            }
        }

        for (peer, _) in &actions.prune {
            self.cooldown.insert(*peer, now + self.config.cooldown);
        }

        // Static peers that are not connected.
        for addr in &self.config.static_peers {
            if let Some(id) = peer_id_of(addr)
                && !connected_set.contains(&id)
            {
                actions.dial.push(addr.clone());
            }
        }

        self.forget(&connected_set, now);
        actions
    }

    /// Lower rank = lower RTT (unknown RTT ranks last).
    fn rtt_rank(&self, peer: &PeerId) -> u128 {
        self.peers.get(peer).and_then(|p| p.rtt).map_or(u128::MAX, |d| d.as_micros())
    }

    fn forget(&mut self, connected: &HashSet<PeerId>, now: Instant) {
        self.peers.retain(|peer, p| {
            connected.contains(peer)
                || p.disconnected_at.is_none_or(|t| now.saturating_duration_since(t) < FORGET_AFTER)
        });
        if self.peers.len() > MAX_TRACKED {
            let mut old: Vec<(PeerId, Instant)> = self
                .peers
                .iter()
                .filter(|(id, _)| !connected.contains(id))
                .map(|(id, p)| (*id, p.disconnected_at.unwrap_or(now)))
                .collect();
            old.sort_by_key(|(_, t)| *t);
            for (id, _) in old.into_iter().take(self.peers.len() - MAX_TRACKED) {
                self.peers.remove(&id);
            }
        }
    }

    /// The best connected peers with a dialable address, best first: `(addr, score, wins)`.
    pub fn best(&self, connected: &[PeerId], n: usize, now: Instant) -> Vec<(Multiaddr, f64, u64)> {
        let mut v: Vec<(Multiaddr, f64, u64)> = connected
            .iter()
            .filter_map(|id| {
                let p = self.peers.get(id)?;
                Some((p.addr.clone()?, self.score(id, now), p.total_wins))
            })
            .filter(|(_, _, wins)| *wins > 0)
            .collect();
        v.sort_by(|a, b| b.1.total_cmp(&a.1));
        v.truncate(n);
        v
    }

    /// Writes the best peers to the store when it is due. Returns how many were written.
    pub fn maybe_store(
        &mut self,
        connected: &[PeerId],
        now: Instant,
    ) -> std::io::Result<Option<usize>> {
        let Some(path) = self.config.store.clone() else { return Ok(None) };
        if self.stored_at.is_some_and(|t| now.saturating_duration_since(t) < STORE_EVERY) {
            return Ok(None);
        }
        let best = self.best(connected, self.config.store_peers, now);
        if best.is_empty() {
            return Ok(None);
        }
        write_store(&path, &best)?;
        self.stored_at = Some(now);
        Ok(Some(best.len()))
    }

    /// A one-line summary of the connected peers for logs: `(peer, score, wins, eligible, rtt_ms)`.
    pub fn summary(
        &self,
        connected: &[PeerId],
        now: Instant,
    ) -> Vec<(PeerId, f64, u64, f64, Option<u128>)> {
        let mut v: Vec<_> = connected
            .iter()
            .map(|id| {
                let p = self.peers.get(id);
                let f = p.map_or(1.0, |p| {
                    0.5f64
                        .powf(self.seq.saturating_sub(p.seq) as f64 / self.config.half_life_blocks)
                });
                (
                    *id,
                    self.score(id, now),
                    p.map_or(0, |p| p.total_wins),
                    p.map_or(0.0, |p| p.eligible * f),
                    p.and_then(|p| p.rtt).map(|d| d.as_millis()),
                )
            })
            .collect();
        v.sort_by(|a, b| b.1.total_cmp(&a.1));
        v
    }
}

/// Round trip at or below which a peer's prior is the full average-peer prior.
pub const RTT_NEAR: Duration = Duration::from_millis(25);

/// Round trip at or above which a peer's prior is [`RTT_FAR_WEIGHT`] of it.
pub const RTT_FAR: Duration = Duration::from_millis(100);

/// Prior weight of a peer at or beyond [`RTT_FAR`].
pub const RTT_FAR_WEIGHT: f64 = 0.1;

/// Prior weight for a ping round trip: 1 up to [`RTT_NEAR`], falling linearly to
/// [`RTT_FAR_WEIGHT`] at [`RTT_FAR`]. Unknown (not pinged yet): 1.
pub fn rtt_weight(rtt: Option<Duration>) -> f64 {
    let Some(rtt) = rtt else { return 1.0 };
    let (near, far) = (RTT_NEAR.as_secs_f64(), RTT_FAR.as_secs_f64());
    let x = ((rtt.as_secs_f64() - near) / (far - near)).clamp(0.0, 1.0);
    (RTT_FAR_WEIGHT - 1.0).mul_add(x, 1.0)
}

/// Reads the store: one `<multiaddr> [score] [wins]` per line, `#` comments. Bad lines are
/// skipped; a missing file is an empty list.
pub fn read_store(path: &Path) -> Vec<Multiaddr> {
    let Ok(text) = std::fs::read_to_string(path) else { return Vec::new() };
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| l.split_whitespace().next()?.parse::<Multiaddr>().ok())
        .filter(|a| peer_id_of(a).is_some())
        .collect()
}

/// Writes the store atomically (temporary file + rename).
pub fn write_store(path: &Path, best: &[(Multiaddr, f64, u64)]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        writeln!(
            f,
            "# best gossip peers by first-delivery score: multiaddr score lifetime_first_deliveries"
        )?;
        for (addr, score, wins) in best {
            writeln!(f, "{addr} {score:.4} {wins}")?;
        }
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

/// The `/p2p/<peer id>` of a multiaddr.
pub fn peer_id_of(addr: &Multiaddr) -> Option<PeerId> {
    addr.iter().find_map(|p| match p {
        Protocol::P2p(id) => Some(id),
        _ => None,
    })
}

fn with_peer_id(mut addr: Multiaddr, peer: PeerId) -> Option<Multiaddr> {
    match peer_id_of(&addr) {
        Some(id) if id == peer => Some(addr),
        Some(_) => None,
        None => {
            addr.push(Protocol::P2p(peer));
            Some(addr)
        }
    }
}

fn is_public_tcp(addr: &Multiaddr) -> bool {
    let mut ip = None;
    let mut tcp = false;
    for p in addr {
        match p {
            Protocol::Ip4(a) => ip = Some(IpAddr::V4(a)),
            Protocol::Ip6(a) => ip = Some(IpAddr::V6(a)),
            Protocol::Tcp(_) => tcp = true,
            _ => {}
        }
    }
    tcp && ip.is_some_and(|ip| match ip {
        IpAddr::V4(a) => {
            !(a.is_private()
                || a.is_loopback()
                || a.is_link_local()
                || a.is_unspecified()
                || a.is_broadcast()
                || a.is_documentation()
                || a.octets()[0] == 100 && a.octets()[1] & 0xC0 == 64)
        }
        IpAddr::V6(a) => {
            !(a.is_loopback()
                || a.is_unspecified()
                || (a.segments()[0] & 0xfe00) == 0xfc00
                || (a.segments()[0] & 0xffc0) == 0xfe80)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> PeerSelectConfig {
        PeerSelectConfig {
            keep: 3,
            explore: 2,
            probation: Duration::from_secs(600),
            interval: Duration::from_secs(60),
            max_prunes: 2,
            cooldown: Duration::from_secs(3600),
            half_life_blocks: 900.0,
            ..Default::default()
        }
    }

    fn connect_all(s: &mut PeerSelector, peers: &[PeerId], at: Instant) {
        for p in peers {
            s.on_connected(*p, None, at);
        }
    }

    #[test]
    fn config_validation() {
        assert!(cfg().validate().is_ok());
        assert!(PeerSelectConfig { keep: 1, ..cfg() }.validate().is_err());
        assert!(PeerSelectConfig { half_life_blocks: 0.0, ..cfg() }.validate().is_err());
        let no_id: Multiaddr = "/ip4/1.2.3.4/tcp/9222".parse().unwrap();
        assert!(PeerSelectConfig { static_peers: vec![no_id], ..cfg() }.validate().is_err());
        assert_eq!(PeerSelectConfig::default().target(), 12);
    }

    #[test]
    fn winners_score_higher_and_never_winning_mesh_peers_lowest() {
        let t0 = Instant::now();
        let mut s = PeerSelector::new(cfg());
        let peers: Vec<PeerId> = (0..4).map(|_| PeerId::random()).collect();
        connect_all(&mut s, &peers, t0);
        // peers[0] wins 90 %, peers[1] 10 %, peers[2] is in the mesh and never wins, peers[3] is
        // connected but never in the mesh.
        let mesh = [peers[0], peers[1], peers[2]];
        for i in 0..300 {
            let src = if i % 10 == 0 { peers[1] } else { peers[0] };
            s.on_block(src, &peers, |p| mesh.contains(p), t0);
        }
        let sc: Vec<f64> = peers.iter().map(|p| s.score(p, t0)).collect();
        assert!(sc[0] > sc[1] && sc[1] > sc[3] && sc[3] > sc[2], "{sc:?}");
        assert!((sc[0] - 0.83).abs() < 0.03, "{sc:?}");
    }

    #[test]
    fn score_decays_old_wins() {
        let t0 = Instant::now();
        let mut s = PeerSelector::new(PeerSelectConfig { half_life_blocks: 100.0, ..cfg() });
        let (a, b) = (PeerId::random(), PeerId::random());
        connect_all(&mut s, &[a, b], t0);
        let mesh = [a, b];
        for _ in 0..500 {
            s.on_block(a, &mesh, |_| true, t0);
        }
        for _ in 0..500 {
            s.on_block(b, &mesh, |_| true, t0);
        }
        assert!(s.score(&b, t0) > 0.8 && s.score(&a, t0) < 0.1);
    }

    #[test]
    fn prunes_lowest_beyond_keep_respecting_probation_and_max_prunes() {
        let t0 = Instant::now();
        // keep 3 + explore 4: the 7 peers fit under the cap, only scores prune.
        let mut s = PeerSelector::new(PeerSelectConfig { explore: 4, ..cfg() });
        let peers: Vec<PeerId> = (0..7).map(|_| PeerId::random()).collect();
        connect_all(&mut s, &peers[..6], t0);
        for i in 0..600u32 {
            // 0 wins most, 1 and 2 sometimes, 3..5 never.
            let src = match i % 6 {
                0 => peers[1],
                1 => peers[2],
                _ => peers[0],
            };
            s.on_block(src, &peers[..6], |_| true, t0);
        }
        // Before the probation ends nothing is pruned for its score.
        let early = s.select(&peers[..6], t0 + Duration::from_secs(60));
        assert!(early.prune.is_empty(), "{early:?}");

        // A seventh peer joins later and is on probation.
        let t1 = t0 + Duration::from_secs(700);
        s.on_connected(peers[6], None, t1);
        let a = s.select(&peers, t1);
        // 6 established > keep 3: excess 3, capped at max_prunes 2.
        assert_eq!(a.prune.len(), 2, "{a:?}");
        for (p, r) in &a.prune {
            assert_eq!(*r, PruneReason::Score);
            assert!(peers[3..6].contains(p), "pruned a winner: {p}");
        }
        assert!(!a.prune.iter().any(|(p, _)| *p == peers[6]), "probation peer pruned");
        // Pruned peers are in cooldown: no dial, no inbound.
        let (p, _) = a.prune[0];
        assert!(s.in_cooldown(&p, t1));
        assert!(!s.allow_dial(&p, 0, 0, t1));
        assert!(!s.allow_inbound(&p, 1, t1));
        // After the cooldown they are unblocked.
        let later = s.select(&[], t1 + Duration::from_secs(3601));
        assert_eq!(later.unblock.len(), 2);
        assert!(!s.in_cooldown(&p, t1 + Duration::from_secs(3601)));
    }

    #[test]
    fn hard_cap_prunes_youngest_probation_peers() {
        let t0 = Instant::now();
        let mut s = PeerSelector::new(cfg());
        let peers: Vec<PeerId> = (0..8).map(|_| PeerId::random()).collect();
        for (i, p) in peers.iter().enumerate() {
            s.on_connected(*p, None, t0 + Duration::from_secs(i as u64));
        }
        let a = s.select(&peers, t0 + Duration::from_secs(10));
        // target 5, 8 connected, all on probation: the 3 youngest go.
        let pruned: HashSet<PeerId> = a.prune.iter().map(|(p, _)| *p).collect();
        assert_eq!(pruned, peers[5..].iter().copied().collect());
        assert!(a.prune.iter().all(|(_, r)| *r == PruneReason::Excess));
    }

    #[test]
    fn invalid_senders_are_pruned_but_static_peers_never() {
        let t0 = Instant::now();
        let st: Multiaddr =
            format!("/ip4/1.2.3.4/tcp/9222/p2p/{}", PeerId::random()).parse().unwrap();
        let st_id = peer_id_of(&st).unwrap();
        let mut s = PeerSelector::new(PeerSelectConfig { static_peers: vec![st.clone()], ..cfg() });
        let bad = PeerId::random();
        connect_all(&mut s, &[bad, st_id], t0);
        for _ in 0..INVALID_LIMIT {
            s.on_invalid(bad, t0);
            s.on_invalid(st_id, t0);
        }
        let a = s.select(&[bad, st_id], t0);
        assert_eq!(a.prune, vec![(bad, PruneReason::Invalid)]);
        // Static peer disconnected: redial it.
        let a = s.select(&[], t0);
        assert_eq!(a.dial, vec![st]);
        assert!(s.allow_dial(&st_id, 100, 100, t0));
        assert!(s.allow_inbound(&st_id, 100, t0));
    }

    #[test]
    fn protected_peers_count_towards_keep_and_are_not_pruned() {
        let t0 = Instant::now();
        let mut s = PeerSelector::new(PeerSelectConfig { protect_stored: true, ..cfg() });
        let peers: Vec<PeerId> = (0..5).map(|_| PeerId::random()).collect();
        s.protect_stored([peers[0], peers[1]]);
        connect_all(&mut s, &peers, t0);
        // Nobody ever wins: the protected ones would be lowest-RTT-ranked equals otherwise.
        for _ in 0..100 {
            s.on_block(peers[4], &peers, |_| true, t0);
        }
        let a = s.select(&peers, t0 + Duration::from_secs(700));
        // 2 protected + 3 established = 5 > keep 3: prune 2, never 0 or 1, and not the winner.
        let pruned: Vec<PeerId> = a.prune.iter().map(|(p, _)| *p).collect();
        assert_eq!(pruned.len(), 2);
        assert!(
            !pruned.contains(&peers[0])
                && !pruned.contains(&peers[1])
                && !pruned.contains(&peers[4])
        );
        // A stored peer loses protection once it disconnects.
        s.on_disconnected(peers[0], false, t0 + Duration::from_secs(800));
        assert!(!s.is_protected(&peers[0]));
    }

    #[test]
    fn early_self_disconnects_lower_reliability() {
        let t0 = Instant::now();
        let mut s = PeerSelector::new(cfg());
        let (a, b) = (PeerId::random(), PeerId::random());
        connect_all(&mut s, &[a, b], t0);
        for _ in 0..200 {
            s.on_block(a, &[a, b], |_| true, t0);
            s.on_block(b, &[a, b], |_| true, t0);
        }
        let t1 = t0 + Duration::from_secs(60);
        s.on_disconnected(a, false, t1);
        s.on_connected(a, None, t1);
        assert!(s.score(&a, t1) < s.score(&b, t1) * 0.6);
        // A prune by us is not the peer's fault.
        s.on_disconnected(b, true, t1);
        s.on_connected(b, None, t1);
        assert!((s.score(&b, t1) - 0.5).abs() < 0.05);
    }

    #[test]
    fn dial_and_inbound_caps() {
        let t0 = Instant::now();
        let s = PeerSelector::new(cfg());
        let p = PeerId::random();
        assert!(s.allow_dial(&p, 3, 1, t0));
        assert!(!s.allow_dial(&p, 4, 1, t0));
        assert!(s.allow_inbound(&p, 5, t0));
        assert!(!s.allow_inbound(&p, 6, t0));
    }

    #[test]
    fn addresses_and_store_round_trip() {
        let t0 = Instant::now();
        let mut s = PeerSelector::new(cfg());
        let (inb, outb, none) = (PeerId::random(), PeerId::random(), PeerId::random());
        let dialed: Multiaddr = "/ip4/5.6.7.8/tcp/9222".parse().unwrap();
        s.on_connected(inb, None, t0);
        s.on_connected(outb, Some(dialed), t0);
        s.on_connected(none, None, t0);
        s.on_identify(
            inb,
            &["/ip4/10.0.0.1/tcp/9222".parse().unwrap(), "/ip4/9.9.9.9/tcp/9003".parse().unwrap()],
        );
        s.on_identify(none, &["/ip4/127.0.0.1/tcp/9222".parse().unwrap()]);
        for _ in 0..10 {
            s.on_block(inb, &[inb, outb, none], |_| true, t0);
            s.on_block(outb, &[inb, outb, none], |_| true, t0);
        }
        s.on_block(none, &[inb, outb, none], |_| true, t0);
        let best = s.best(&[inb, outb, none], 8, t0);
        assert_eq!(best.len(), 2, "no dialable address for the third: {best:?}");
        assert_eq!(
            best.iter()
                .filter(|b| b.0.to_string().starts_with("/ip4/9.9.9.9/tcp/9003/p2p/"))
                .count(),
            1
        );
        assert_eq!(
            best.iter()
                .filter(|b| b.0.to_string().starts_with("/ip4/5.6.7.8/tcp/9222/p2p/"))
                .count(),
            1
        );

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("best-peers.txt");
        let mut s2 = PeerSelector::new(PeerSelectConfig { store: Some(path.clone()), ..cfg() });
        assert_eq!(s2.maybe_store(&[], t0).unwrap(), None, "nothing to store");
        write_store(&path, &best).unwrap();
        std::fs::write(
            &path,
            format!(
                "{}\nnot-a-multiaddr\n/ip4/1.1.1.1/tcp/1\n",
                std::fs::read_to_string(&path).unwrap()
            ),
        )
        .unwrap();
        let read = read_store(&path);
        assert_eq!(read, best.iter().map(|b| b.0.clone()).collect::<Vec<_>>());
        assert!(read_store(&dir.path().join("missing")).is_empty());
    }

    #[test]
    fn far_newcomers_start_low() {
        assert_eq!(rtt_weight(None), 1.0);
        assert_eq!(rtt_weight(Some(Duration::from_millis(10))), 1.0);
        assert!((rtt_weight(Some(Duration::from_micros(62_500))) - 0.55).abs() < 1e-9);
        assert_eq!(rtt_weight(Some(Duration::from_millis(180))), RTT_FAR_WEIGHT);
        let t0 = Instant::now();
        let mut s = PeerSelector::new(cfg());
        let (near, far) = (PeerId::random(), PeerId::random());
        s.on_connected(near, None, t0);
        s.on_connected(far, None, t0);
        s.on_rtt(near, Duration::from_millis(12));
        s.on_rtt(far, Duration::from_millis(150));
        assert!(s.score(&near, t0) > 5.0 * s.score(&far, t0));
        // Wins outweigh the prior: a far peer that delivers first is kept.
        for _ in 0..200 {
            s.on_block(far, &[near, far], |_| true, t0);
        }
        assert!(s.score(&far, t0) > s.score(&near, t0));
    }

    #[test]
    fn public_tcp_filter() {
        for (a, ok) in [
            ("/ip4/9.9.9.9/tcp/1", true),
            ("/ip4/10.1.2.3/tcp/1", false),
            ("/ip4/100.64.0.1/tcp/1", false),
            ("/ip4/9.9.9.9/udp/1", false),
            ("/ip6/2600:1f18::1/tcp/1", true),
            ("/ip6/fd00::1/tcp/1", false),
            ("/ip6/::1/tcp/1", false),
        ] {
            assert_eq!(is_public_tcp(&a.parse().unwrap()), ok, "{a}");
        }
    }

    #[test]
    fn forgets_long_disconnected_peers() {
        let t0 = Instant::now();
        let mut s = PeerSelector::new(cfg());
        let p = PeerId::random();
        s.on_connected(p, None, t0);
        s.on_disconnected(p, false, t0);
        s.select(&[], t0 + Duration::from_secs(60));
        assert!(s.stats(&p).is_some());
        s.select(&[], t0 + FORGET_AFTER + Duration::from_secs(1));
        assert!(s.stats(&p).is_none());
    }

    /// Replays recorded per-peer deliveries from trader-va (`peerstats.py export`: socket-level
    /// first copies, observed duplicate offsets and our forwarded bytes per block) through the
    /// selector, and compares what a node that applied its prunes would have seen with what the
    /// node with 30 peers saw. Prints one JSON line per `keep:explore` setting. Run with
    /// `BASE_P2P_SELECT_REPLAY=events.txt [BASE_P2P_SELECT_REPLAY_KE=8:4,5:3]
    /// cargo test -p base-consensus-gossip --lib replay_recorded -- --ignored --nocapture`.
    #[test]
    #[ignore = "needs a recorded replay file (BASE_P2P_SELECT_REPLAY)"]
    fn replay_recorded() {
        let Ok(path) = std::env::var("BASE_P2P_SELECT_REPLAY") else { return };
        let text = std::fs::read_to_string(&path).expect("replay file");
        let settings = std::env::var("BASE_P2P_SELECT_REPLAY_KE").unwrap_or_else(|_| "8:4".into());
        let seeds: u64 = std::env::var("BASE_P2P_SELECT_REPLAY_SEEDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        for ke in settings.split(',') {
            let v: Vec<u64> = ke.split(':').map(|x| x.parse().unwrap()).collect();
            let config = PeerSelectConfig {
                keep: v[0] as usize,
                explore: v[1] as usize,
                probation: Duration::from_secs(v.get(2).copied().unwrap_or(600)),
                ..PeerSelectConfig::default()
            };
            for seed in 0..seeds {
                println!("{}", replay(&text, config.clone(), seed));
            }
        }
    }

    /// Censored blocks (no kept peer's copy was observed before our IDONTWANT stopped it) are
    /// assumed to arrive this much after the actual first copy.
    const CENSORED_MS: f64 = 100.0;

    /// `seed` shuffles the order in which simultaneous connections are admitted (0: file order).
    fn replay(text: &str, config: PeerSelectConfig, seed: u64) -> String {
        let t0 = Instant::now();
        let mut rng = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        let mut rtts: HashMap<PeerId, Duration> = HashMap::new();
        let mut ids: HashMap<String, PeerId> = HashMap::new();
        let mut id = |s: &str| *ids.entry(s.to_string()).or_insert_with(PeerId::random);
        let mut s = PeerSelector::new(config.clone());
        let mut real: Vec<PeerId> = Vec::new(); // connected in the recording, in connect order
        let mut admitted: Vec<PeerId> = Vec::new(); // connected in the replayed node
        let mut next_tick = config.interval;
        let (mut blocks, mut kept_first, mut censored, mut prunes) = (0u64, 0u64, 0u64, 0u64);
        let (mut extra, mut lead_real, mut lead_sel): (Vec<f64>, Vec<f64>, Vec<f64>) =
            (Vec::new(), Vec::new(), Vec::new());
        let (mut out_all, mut out_kept, mut admitted_sum) = (0u64, 0u64, 0u64);
        fn admit_now(
            s: &mut PeerSelector,
            real: &[PeerId],
            admitted: &mut Vec<PeerId>,
            rtts: &HashMap<PeerId, Duration>,
            now: Instant,
        ) {
            for p in real {
                if !admitted.contains(p) && s.allow_dial(p, admitted.len(), 0, now) {
                    admitted.push(*p);
                    s.on_connected(*p, None, now);
                    if let Some(r) = rtts.get(p) {
                        s.on_rtt(*p, *r);
                    }
                }
            }
        }
        for line in text.lines() {
            let f: Vec<&str> = line.split(' ').collect();
            let now = t0 + Duration::from_millis(f[1].parse().unwrap());
            while t0 + next_tick <= now {
                let at = t0 + next_tick;
                let a = s.select(&admitted, at);
                for (p, _) in &a.prune {
                    admitted.retain(|x| x != p);
                    s.on_disconnected(*p, true, at);
                    prunes += 1;
                }
                next_tick += config.interval;
                admit_now(&mut s, &real, &mut admitted, &rtts, at);
            }
            if f[0] == "B" {
                admit_now(&mut s, &real, &mut admitted, &rtts, now);
            }
            match f[0] {
                "C" => {
                    let p = id(f[2]);
                    if let Some(ms) = f.get(3).and_then(|v| v.parse::<f64>().ok()) {
                        rtts.insert(p, Duration::from_secs_f64(ms / 1000.0));
                    }
                    if !real.contains(&p) {
                        // Insert at a random position among the connections of the same instant.
                        let pos = if seed == 0 {
                            real.len()
                        } else {
                            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                            (rng >> 33) as usize % (real.len() + 1)
                        };
                        real.insert(pos, p);
                    }
                }
                "D" => {
                    let p = id(f[2]);
                    real.retain(|x| *x != p);
                    if admitted.contains(&p) {
                        admitted.retain(|x| *x != p);
                        s.on_disconnected(p, false, now);
                    }
                }
                "B" => {
                    blocks += 1;
                    admitted_sum += admitted.len() as u64;
                    let first = id(f[2]);
                    let mesh: HashSet<PeerId> = f[3].split(',').map(&mut id).collect();
                    let dups: Vec<(PeerId, f64)> = if f[4] == "-" {
                        Vec::new()
                    } else {
                        f[4].split(',')
                            .map(|kv| {
                                let (k, v) = kv.rsplit_once('=').unwrap();
                                (id(k), v.parse().unwrap())
                            })
                            .collect()
                    };
                    if f[5] != "-" {
                        for kv in f[5].split(',') {
                            let (k, v) = kv.rsplit_once('=').unwrap();
                            let nb: u64 = v.parse().unwrap();
                            out_all += nb;
                            if admitted.contains(&id(k)) {
                                out_kept += nb;
                            }
                        }
                    }
                    let lead: Option<f64> = f[6].parse().ok();
                    let (src, off) = if admitted.contains(&first) {
                        kept_first += 1;
                        (Some(first), 0.0)
                    } else if let Some((p, ms)) = dups
                        .iter()
                        .filter(|(p, _)| admitted.contains(p))
                        .min_by(|a, b| a.1.total_cmp(&b.1))
                    {
                        (Some(*p), *ms)
                    } else {
                        censored += 1;
                        (None, CENSORED_MS)
                    };
                    extra.push(off);
                    if let Some(l) = lead {
                        lead_real.push(l);
                        lead_sel.push(l - off);
                    }
                    if let Some(src) = src {
                        s.on_block(src, &admitted, |p| mesh.contains(p), now);
                    }
                }
                _ => {}
            }
        }
        let q = |v: &mut Vec<f64>, p: f64| {
            if v.is_empty() {
                return f64::NAN;
            }
            v.sort_by(f64::total_cmp);
            v[((v.len() - 1) as f64 * p).round() as usize]
        };
        let mean = |v: &[f64]| v.iter().sum::<f64>() / v.len().max(1) as f64;
        format!(
            "{{\"keep\":{},\"explore\":{},\"probation_s\":{},\"seed\":{},\"blocks\":{},\"first_from_kept_pct\":{:.2},\"censored\":{},\
             \"extra_ms_mean\":{:.2},\"extra_ms_p99\":{:.1},\"lead_ms_p50_real\":{:.1},\"lead_ms_p50_selected\":{:.1},\
             \"lead_ms_mean_real\":{:.1},\"lead_ms_mean_selected\":{:.1},\"peers_mean\":{:.1},\"prunes\":{},\
             \"egress_kept_pct\":{:.1}}}",
            config.keep,
            config.explore,
            config.probation.as_secs(),
            seed,
            blocks,
            100.0 * kept_first as f64 / blocks.max(1) as f64,
            censored,
            mean(&extra),
            q(&mut extra, 0.99),
            q(&mut lead_real.clone(), 0.5),
            q(&mut lead_sel.clone(), 0.5),
            mean(&lead_real),
            mean(&lead_sel),
            admitted_sum as f64 / blocks.max(1) as f64,
            prunes,
            100.0 * out_kept as f64 / out_all.max(1) as f64,
        )
    }
}
