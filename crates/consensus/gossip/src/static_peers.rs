//! Operator-configured static peers that the gossip driver keeps connected.
//!
//! Static peers are dialed at start-up, protected in the connection gater (no dial thresholds or
//! redial limits), exempt from the libp2p connection limits, and redialed after a disconnect or a
//! failed dial with a bounded exponential backoff. They remain ordinary gossipsub mesh
//! candidates: they are not gossipsub "explicit" peers, because gossipsub refuses to GRAFT
//! explicit peers into the mesh (and PRUNEs their GRAFTs), so a remote that treats us as a normal
//! peer would then only reach us through lazy `IHAVE` gossip.

use std::{collections::HashMap, str::FromStr, time::Duration};

use base_consensus_peers::{BootNode, BootNodes, PeerUtils};
use discv5::Enr;
use libp2p::{Multiaddr, PeerId, multiaddr::Protocol};
use tokio::time::Instant;

use crate::{ConnectionGater, Metrics};

/// Delay before the first redial of a static peer after a failed dial or a disconnect.
pub const STATIC_PEER_INITIAL_BACKOFF: Duration = Duration::from_secs(5);

/// Upper bound on the delay between two dials of the same static peer.
pub const STATIC_PEER_MAX_BACKOFF: Duration = Duration::from_secs(5 * 60);

/// A connection that lasted at least this long resets the static peer's backoff.
///
/// Shorter connections keep growing the backoff so a peer that accepts and immediately drops
/// us is not redialed every [`STATIC_PEER_INITIAL_BACKOFF`].
pub const STATIC_PEER_STABLE_CONNECTION: Duration = Duration::from_secs(60);

/// An error parsing a static peer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StaticPeerParseError {
    /// The ENR could not be decoded.
    #[error("invalid ENR '{raw}': {reason}")]
    InvalidEnr {
        /// The raw input.
        raw: String,
        /// The decoding error.
        reason: String,
    },
    /// The ENR has no TCP socket or no secp256k1 key.
    #[error("ENR '{raw}' has no TCP socket or no secp256k1 key")]
    EnrNotDialable {
        /// The raw input.
        raw: String,
    },
    /// The multiaddr could not be parsed.
    #[error("invalid multiaddr '{raw}': {reason}")]
    InvalidMultiaddr {
        /// The raw input.
        raw: String,
        /// The parse error.
        reason: String,
    },
    /// The multiaddr does not name the peer.
    #[error("multiaddr '{raw}' has no /p2p/<peer-id> component")]
    MissingPeerId {
        /// The raw input.
        raw: String,
    },
}

/// Parses a static peer given as an ENR (`enr:-...`) or as a libp2p multiaddr ending in
/// `/p2p/<peer-id>`, returning a dialable multiaddr.
pub fn parse_static_peer(raw: &str) -> Result<Multiaddr, StaticPeerParseError> {
    let raw = raw.trim();
    if raw.starts_with("enr:") {
        let enr = Enr::from_str(raw)
            .map_err(|reason| StaticPeerParseError::InvalidEnr { raw: raw.to_string(), reason })?;
        return PeerUtils::enr_to_multiaddr(&enr)
            .ok_or_else(|| StaticPeerParseError::EnrNotDialable { raw: raw.to_string() });
    }
    let addr = Multiaddr::from_str(raw).map_err(|e| StaticPeerParseError::InvalidMultiaddr {
        raw: raw.to_string(),
        reason: e.to_string(),
    })?;
    if ConnectionGater::peer_id_from_addr(&addr).is_none() {
        return Err(StaticPeerParseError::MissingPeerId { raw: raw.to_string() });
    }
    Ok(addr)
}

/// Returns the chain's built-in consensus bootnodes as dialable TCP multiaddrs
/// (`/ip4/<ip>/tcp/<tcp>/p2p/<peer-id>`). Unknown chains have none.
pub fn chain_bootnode_static_peers(chain_id: u64) -> Vec<Multiaddr> {
    BootNodes::from_chain_id(chain_id)
        .iter()
        .filter_map(|bootnode| match bootnode {
            BootNode::Enr(enr) => PeerUtils::enr_to_multiaddr(enr),
            // Node records carry a UDP discovery port that libp2p cannot dial over TCP.
            BootNode::Enode(addr) => {
                Some(addr.iter().filter(|p| !matches!(p, Protocol::Udp(_))).collect())
            }
        })
        .collect()
}

#[derive(Debug, Clone)]
struct StaticPeerState {
    addr: Multiaddr,
    /// When the current connection was established, if connected.
    connected_since: Option<Instant>,
    /// When to dial next, if not connected.
    next_dial: Option<Instant>,
    /// The delay to wait after the next dial before dialing again.
    backoff: Duration,
}

/// Tracks the configured static peers and when each should be (re)dialed.
#[derive(Debug, Clone)]
pub struct StaticPeers {
    peers: HashMap<PeerId, StaticPeerState>,
}

impl StaticPeers {
    /// Creates the tracker. Every peer is due for a dial immediately. Addresses without a
    /// `/p2p/<peer-id>` component are skipped; a repeated peer id keeps its first address.
    pub fn new(addrs: impl IntoIterator<Item = Multiaddr>) -> Self {
        let now = Instant::now();
        let mut peers = HashMap::new();
        for addr in addrs {
            let Some(peer_id) = ConnectionGater::peer_id_from_addr(&addr) else {
                warn!(target: "gossip", %addr, "Ignoring static peer without a /p2p/<peer-id>");
                continue;
            };
            peers.entry(peer_id).or_insert_with(|| {
                Metrics::static_peer_connected(peer_id.to_string()).set(0.0);
                StaticPeerState {
                    addr,
                    connected_since: None,
                    next_dial: Some(now),
                    backoff: STATIC_PEER_INITIAL_BACKOFF,
                }
            });
        }
        let this = Self { peers };
        this.record_connected_count();
        this
    }

    /// Returns the number of static peers.
    pub fn len(&self) -> usize {
        self.peers.len()
    }

    /// Returns true if there are no static peers.
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// Returns true if `peer_id` is a static peer.
    pub fn contains(&self, peer_id: &PeerId) -> bool {
        self.peers.contains_key(peer_id)
    }

    /// Returns the static peer ids.
    pub fn peer_ids(&self) -> impl Iterator<Item = &PeerId> {
        self.peers.keys()
    }

    /// Returns the configured address of a static peer.
    pub fn addr(&self, peer_id: &PeerId) -> Option<&Multiaddr> {
        self.peers.get(peer_id).map(|state| &state.addr)
    }

    /// Returns true if `peer_id` is a connected static peer.
    pub fn is_connected(&self, peer_id: &PeerId) -> bool {
        self.peers.get(peer_id).is_some_and(|state| state.connected_since.is_some())
    }

    /// Returns the number of connected static peers.
    pub fn connected(&self) -> usize {
        self.peers.values().filter(|state| state.connected_since.is_some()).count()
    }

    /// Returns the earliest time a disconnected static peer is due for a dial.
    pub fn next_due(&self) -> Option<Instant> {
        self.peers.values().filter_map(|state| state.next_dial).min()
    }

    /// Returns the addresses of disconnected static peers that are due for a dial at `now`, and
    /// schedules each one's next dial `backoff` later, doubling the backoff up to
    /// [`STATIC_PEER_MAX_BACKOFF`]. A dial that fails or never resolves is retried then; a
    /// connection cancels the retry.
    pub fn take_due(&mut self, now: Instant) -> Vec<Multiaddr> {
        let mut due = Vec::new();
        for state in self.peers.values_mut() {
            if state.connected_since.is_some() || state.next_dial.is_none_or(|at| at > now) {
                continue;
            }
            state.next_dial = Some(now + state.backoff);
            state.backoff = (state.backoff * 2).min(STATIC_PEER_MAX_BACKOFF);
            due.push(state.addr.clone());
        }
        due
    }

    /// Records a connection to `peer_id`. Returns true if it is a static peer that was not
    /// already connected.
    pub fn on_connected(&mut self, peer_id: &PeerId, now: Instant) -> bool {
        let Some(state) = self.peers.get_mut(peer_id) else {
            return false;
        };
        if state.connected_since.is_some() {
            return false;
        }
        state.connected_since = Some(now);
        state.next_dial = None;
        Metrics::static_peer_connected(peer_id.to_string()).set(1.0);
        self.record_connected_count();
        true
    }

    /// Records that the last connection to `peer_id` closed. Returns the redial delay if it is a
    /// connected static peer. A connection that lasted [`STATIC_PEER_STABLE_CONNECTION`] resets
    /// the backoff to [`STATIC_PEER_INITIAL_BACKOFF`].
    pub fn on_disconnected(&mut self, peer_id: &PeerId, now: Instant) -> Option<Duration> {
        let state = self.peers.get_mut(peer_id)?;
        let since = state.connected_since.take()?;
        if now.saturating_duration_since(since) >= STATIC_PEER_STABLE_CONNECTION {
            state.backoff = STATIC_PEER_INITIAL_BACKOFF;
        }
        state.next_dial = Some(now + state.backoff);
        Metrics::static_peer_connected(peer_id.to_string()).set(0.0);
        let redial_in = state.backoff;
        self.record_connected_count();
        Some(redial_in)
    }

    /// Returns when a disconnected static peer will next be dialed.
    pub fn next_dial(&self, peer_id: &PeerId) -> Option<Instant> {
        self.peers.get(peer_id).and_then(|state| state.next_dial)
    }

    fn record_connected_count(&self) {
        Metrics::static_peers_connected().set(self.connected() as f64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Base mainnet consensus bootnodes, derived independently of this crate from the ENRs in
    /// the chain config (secp256k1 key -> libp2p protobuf public key -> identity multihash ->
    /// base58).
    const MAINNET_BOOTNODES: [&str; 5] = [
        "/ip4/3.231.138.188/tcp/9222/p2p/16Uiu2HAm4wN4epAyuj82ix2QC1Uc2vQd833szwvsNpLqhubPz8xa",
        "/ip4/184.72.129.189/tcp/9222/p2p/16Uiu2HAm3qwgs4Bvfx1GJ5qnY4LjPbYCc394kvdjpxuJBsP3SQnW",
        "/ip4/3.220.145.177/tcp/9222/p2p/16Uiu2HAm31LCBALXkJfNGmK1tVkgzQDw9QQoNcFznzBBCYUPUhAD",
        "/ip4/3.231.11.52/tcp/9222/p2p/16Uiu2HAkvLBTPqfA9HHXgnLUQiEJg7EKPb7gu7qMXfJFS79Goy3b",
        "/ip4/54.198.153.150/tcp/9222/p2p/16Uiu2HAkwSnpTCP4f1ttHa2hKgiwZaa8TFJYWHtNNhYbqKwaYMRu",
    ];

    const MAINNET_BOOTNODE_ENR: &str = "enr:-J24QNz9lbrKbN4iSmmjtnr7SjUMk4zB7f1krHZcTZx-JRKZd0kA2gjufUROD6T3sOWDVDnFJRvqBBo62zuF-hYCohOGAYiOoEyEgmlkgnY0gmlwhAPniryHb3BzdGFja4OFQgCJc2VjcDI1NmsxoQKNVFlCxh_B-716tTs-h1vMzZkSs1FTu_OYTNjgufplG4N0Y3CCJAaDdWRwgiQG";

    fn addr(peer: &PeerId, port: u16) -> Multiaddr {
        format!("/ip4/127.0.0.1/tcp/{port}/p2p/{peer}").parse().unwrap()
    }

    #[test]
    fn parses_multiaddr_with_peer_id() {
        let raw = MAINNET_BOOTNODES[0];
        assert_eq!(parse_static_peer(raw).unwrap(), raw.parse::<Multiaddr>().unwrap());
        let dns = "/dns4/node.example.com/tcp/9222/p2p/16Uiu2HAm4wN4epAyuj82ix2QC1Uc2vQd833szwvsNpLqhubPz8xa";
        assert_eq!(parse_static_peer(&format!(" {dns} ")).unwrap().to_string(), dns);
    }

    #[test]
    fn parses_enr_to_tcp_multiaddr() {
        let addr = parse_static_peer(MAINNET_BOOTNODE_ENR).unwrap();
        assert_eq!(addr.to_string(), MAINNET_BOOTNODES[0]);
    }

    #[test]
    fn rejects_unusable_static_peers() {
        assert!(matches!(
            parse_static_peer("/ip4/127.0.0.1/tcp/9222"),
            Err(StaticPeerParseError::MissingPeerId { .. })
        ));
        assert!(matches!(
            parse_static_peer("not-a-multiaddr"),
            Err(StaticPeerParseError::InvalidMultiaddr { .. })
        ));
        assert!(matches!(
            parse_static_peer("enr:-invalid"),
            Err(StaticPeerParseError::InvalidEnr { .. })
        ));
        // A valid ENR without a TCP socket cannot be dialed over libp2p.
        let key = discv5::enr::CombinedKey::generate_secp256k1();
        let enr = Enr::builder().ip4(std::net::Ipv4Addr::LOCALHOST).udp4(9223).build(&key).unwrap();
        assert!(matches!(
            parse_static_peer(&enr.to_base64()),
            Err(StaticPeerParseError::EnrNotDialable { .. })
        ));
    }

    #[test]
    fn expands_base_mainnet_bootnodes() {
        let addrs = chain_bootnode_static_peers(8453);
        let got: Vec<String> = addrs.iter().map(ToString::to_string).collect();
        assert_eq!(got, MAINNET_BOOTNODES);
        for addr in &addrs {
            let peer_id = ConnectionGater::peer_id_from_addr(addr).unwrap();
            assert!(peer_id.to_string().starts_with("16Uiu2HA"), "secp256k1 peer id {peer_id}");
        }
        // The peer ids match the ones discv5 derives from the same records.
        let bootnodes = BootNodes::mainnet();
        assert_eq!(bootnodes.len(), 5);
        for (bootnode, addr) in bootnodes.iter().zip(&addrs) {
            let BootNode::Enr(enr) = bootnode else { panic!("mainnet bootnodes are ENRs") };
            let local_id = base_consensus_peers::AnyNode::from(enr.clone()).peer_id();
            assert_eq!(
                PeerUtils::local_id_to_p2p_id(local_id).unwrap(),
                ConnectionGater::peer_id_from_addr(addr).unwrap()
            );
        }
        assert!(chain_bootnode_static_peers(0).is_empty());
    }

    #[test]
    fn dedupes_and_skips_addresses_without_peer_id() {
        let peer = PeerId::random();
        let peers = StaticPeers::new([
            addr(&peer, 1),
            addr(&peer, 2),
            "/ip4/127.0.0.1/tcp/3".parse().unwrap(),
        ]);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers.addr(&peer), Some(&addr(&peer, 1)));
    }

    /// Dials of a peer that never connects are spaced 5 s, 10 s, 20 s, ... capped at 5 min, and
    /// the scheduler's deadline fires exactly then on a paused clock.
    #[tokio::test(start_paused = true)]
    async fn redial_backoff_doubles_to_cap_on_paused_clock() {
        let start = Instant::now();
        let peer = PeerId::random();
        let mut peers = StaticPeers::new([addr(&peer, 1)]);
        let mut dial_times = Vec::new();
        for _ in 0..10 {
            let due = peers.next_due().expect("disconnected peer is always scheduled");
            tokio::time::sleep_until(due).await;
            let now = Instant::now();
            assert_eq!(peers.take_due(now), vec![addr(&peer, 1)]);
            // Nothing else is due until the next scheduled dial.
            assert!(peers.take_due(now).is_empty());
            dial_times.push((now - start).as_secs());
        }
        assert_eq!(dial_times, [0, 5, 15, 35, 75, 155, 315, 615, 915, 1215]);
    }

    #[tokio::test(start_paused = true)]
    async fn connection_cancels_redial_and_stable_disconnect_resets_backoff() {
        let peer = PeerId::random();
        let other = PeerId::random();
        let mut peers = StaticPeers::new([addr(&peer, 1)]);
        let now = Instant::now();
        assert_eq!(peers.take_due(now).len(), 1);
        assert_eq!(peers.take_due(now + Duration::from_secs(5)).len(), 1);
        // Next backoff is now 20 s.

        assert!(!peers.on_connected(&other, now), "non-static peers are ignored");
        assert!(peers.on_connected(&peer, now));
        assert!(!peers.on_connected(&peer, now), "second connection is not a new connect");
        assert_eq!(peers.connected(), 1);
        assert_eq!(peers.next_due(), None, "connected peers are not redialed");
        assert!(peers.take_due(now + Duration::from_secs(3600)).is_empty());

        // A short-lived connection keeps the grown backoff.
        let closed = now + Duration::from_secs(10);
        assert_eq!(peers.on_disconnected(&peer, closed), Some(Duration::from_secs(20)));
        assert_eq!(peers.next_due(), Some(closed + Duration::from_secs(20)));
        assert_eq!(peers.on_disconnected(&peer, closed), None, "already disconnected");
        assert_eq!(peers.on_disconnected(&other, closed), None);

        // A stable connection resets it.
        assert!(peers.on_connected(&peer, closed));
        let closed = closed + STATIC_PEER_STABLE_CONNECTION;
        assert_eq!(peers.on_disconnected(&peer, closed), Some(STATIC_PEER_INITIAL_BACKOFF));
        assert_eq!(peers.next_due(), Some(closed + STATIC_PEER_INITIAL_BACKOFF));
        assert!(peers.take_due(closed + Duration::from_secs(4)).is_empty());
        assert_eq!(peers.take_due(closed + STATIC_PEER_INITIAL_BACKOFF).len(), 1);
    }
}
