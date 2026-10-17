//! Consensus-layer gossipsub driver for Base.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_primitives::{Address, hex};
use base_common_genesis::RollupConfig;
use base_common_rpc_types_engine::NetworkPayloadEnvelope;
use base_consensus_peers::{EnrValidation, PeerMonitoring, PeerUtils};
use derive_more::Debug;
use discv5::Enr;
use futures::{AsyncWriteExt, stream::StreamExt};
use libp2p::{
    Multiaddr, PeerId, Swarm, TransportError,
    core::ConnectedPoint,
    gossipsub::{IdentTopic, MessageId},
    swarm::{
        SwarmEvent,
        dial_opts::{DialOpts, PeerCondition},
    },
};
use libp2p_identity::Keypair;
use libp2p_stream::IncomingStreams;
use lru::LruCache;
use tokio::sync::Mutex;

use crate::{
    Behaviour, BlockHandler, ConnectionGate, ConnectionGater, ConnectionLimitsConfig, Event,
    GossipDriverBuilder, Handler, Metrics, PublishError, StaticPeers,
};

/// Configuration applied when constructing a [`GossipDriver`].
#[derive(Debug, Clone)]
pub struct GossipDriverConfig {
    /// Maximum number of peers to retain identify metadata for.
    pub max_identify_peerstore_peers: NonZeroUsize,
    /// Peer score monitoring config.
    pub peer_monitoring: Option<PeerMonitoring>,
    /// The configured libp2p connection limits enforced by the swarm.
    pub connection_limits_config: ConnectionLimitsConfig,
    /// Static peers to keep connected. Empty disables the feature.
    pub static_peers: Vec<Multiaddr>,
}

/// A driver for a [`Swarm`] instance.
///
/// Connects the swarm to the given [`Multiaddr`]
/// and handles events using the [`BlockHandler`].
#[derive(Debug)]
pub struct GossipDriver<G: ConnectionGate> {
    /// The [`Swarm`] instance.
    #[debug(skip)]
    pub swarm: Swarm<Behaviour>,
    /// A [`Multiaddr`] to listen on.
    pub addr: Multiaddr,
    /// The [`BlockHandler`].
    pub handler: BlockHandler,
    /// A [`libp2p_stream::Control`] instance. Can be used to control the sync request/response
    #[debug(skip)]
    pub sync_handler: libp2p_stream::Control,
    /// The inbound streams for the sync request/response protocol.
    ///
    /// This is an option to allow to take the underlying value when the gossip driver gets
    /// activated.
    ///
    /// TODO: remove the sync-req-resp protocol once it is fully deprecated upstream.
    #[debug(skip)]
    pub sync_protocol: Option<IncomingStreams>,
    /// LRU cache of identify metadata keyed by [`PeerId`].
    pub peerstore: LruCache<PeerId, libp2p::identify::Info>,
    /// If set, the gossip layer will monitor peer scores and ban peers that are below a given
    /// threshold.
    pub peer_monitoring: Option<PeerMonitoring>,
    /// Tracks connection start time for peers
    pub peer_connection_start: HashMap<PeerId, Instant>,
    /// The connection gate.
    pub connection_gate: G,
    /// The configured libp2p connection limits enforced by the swarm.
    pub connection_limits_config: ConnectionLimitsConfig,
    /// Tracks ping times for peers.
    pub ping: Arc<Mutex<HashMap<PeerId, Duration>>>,
    /// Static peers kept connected with a bounded redial backoff. `None` when none are
    /// configured, which leaves every connection path unchanged.
    pub static_peers: Option<StaticPeers>,
}

impl<G> GossipDriver<G>
where
    G: ConnectionGate,
{
    /// Returns the [`GossipDriverBuilder`] that can be used to construct the [`GossipDriver`].
    pub const fn builder(
        rollup_config: RollupConfig,
        signer: Address,
        gossip_addr: Multiaddr,
        keypair: Keypair,
    ) -> GossipDriverBuilder {
        GossipDriverBuilder::new(rollup_config, signer, gossip_addr, keypair)
    }

    /// Creates a new [`GossipDriver`] instance.
    pub fn new(
        mut swarm: Swarm<Behaviour>,
        addr: Multiaddr,
        handler: BlockHandler,
        sync_handler: libp2p_stream::Control,
        sync_protocol: IncomingStreams,
        mut gate: G,
        config: GossipDriverConfig,
    ) -> Self {
        let static_peers = (!config.static_peers.is_empty()).then(|| {
            let static_peers = StaticPeers::new(config.static_peers);
            for peer_id in static_peers.peer_ids() {
                // No dial thresholds or redial limits, and not disconnectable via the gate.
                gate.protect_peer(*peer_id);
                // Never refused by our own connection limits; bypassed connections are also not
                // counted against them.
                swarm.behaviour_mut().connection_limits.bypass_peer_id(peer_id);
            }
            info!(target: "gossip", count = static_peers.len(), "Configured static peers");
            static_peers
        });
        Self {
            swarm,
            addr,
            handler,
            peerstore: LruCache::new(config.max_identify_peerstore_peers),
            peer_monitoring: config.peer_monitoring,
            peer_connection_start: Default::default(),
            sync_handler,
            sync_protocol: Some(sync_protocol),
            connection_gate: gate,
            connection_limits_config: config.connection_limits_config,
            ping: Arc::new(Mutex::new(Default::default())),
            static_peers,
        }
    }

    /// Returns true if `peer_id` is a configured static peer.
    pub fn is_static_peer(&self, peer_id: &PeerId) -> bool {
        self.static_peers.as_ref().is_some_and(|peers| peers.contains(peer_id))
    }

    /// Returns when the next static peer dial is due, if any static peer is disconnected.
    pub fn static_peers_next_due(&self) -> Option<tokio::time::Instant> {
        self.static_peers.as_ref().and_then(StaticPeers::next_due)
    }

    /// Dials every disconnected static peer whose redial backoff has elapsed.
    pub fn dial_due_static_peers(&mut self) {
        let Some(static_peers) = self.static_peers.as_mut() else {
            return;
        };
        for addr in static_peers.take_due(tokio::time::Instant::now()) {
            debug!(target: "gossip", %addr, "Dialing static peer");
            self.dial_multiaddr(addr);
        }
    }

    fn on_static_peer_connected(&mut self, peer_id: PeerId, endpoint: &ConnectedPoint) {
        let Some(static_peers) = self.static_peers.as_mut() else {
            return;
        };
        if static_peers.on_connected(&peer_id, tokio::time::Instant::now()) {
            info!(
                target: "gossip",
                peer_id = %peer_id,
                remote = %endpoint.get_remote_address(),
                inbound = endpoint.is_listener(),
                connected = static_peers.connected(),
                configured = static_peers.len(),
                "Static peer connected"
            );
        }
    }

    fn on_static_peer_disconnected(
        &mut self,
        peer_id: PeerId,
        cause: Option<&libp2p::swarm::ConnectionError>,
    ) {
        let Some(static_peers) = self.static_peers.as_mut() else {
            return;
        };
        if let Some(redial_in) = static_peers.on_disconnected(&peer_id, tokio::time::Instant::now())
        {
            info!(
                target: "gossip",
                peer_id = %peer_id,
                cause = ?cause,
                redial_in_secs = redial_in.as_secs(),
                connected = static_peers.connected(),
                configured = static_peers.len(),
                "Static peer disconnected"
            );
        }
    }

    /// Publishes an unsafe block to gossip.
    ///
    /// ## Arguments
    ///
    /// * `topic_selector` - A function that selects the topic for the block. This is expected to be
    ///   a closure that takes the [`BlockHandler`] and returns the [`IdentTopic`] for the block.
    /// * `payload` - The payload to be published.
    ///
    /// ## Returns
    ///
    /// Returns the [`MessageId`] of the published message or a [`PublishError`]
    /// if the message could not be published.
    pub fn publish(
        &mut self,
        selector: impl FnOnce(&BlockHandler) -> IdentTopic,
        payload: Option<NetworkPayloadEnvelope>,
    ) -> Result<Option<MessageId>, PublishError> {
        let Some(payload) = payload else {
            return Ok(None);
        };
        let topic = selector(&self.handler);
        let topic_hash = topic.hash();
        let data = self.handler.encode(topic, payload)?;
        let id = self.swarm.behaviour_mut().gossipsub.publish(topic_hash, data)?;
        Metrics::unsafe_block_published().increment(1.0);
        Ok(Some(id))
    }

    /// Handles the sync request/response protocol.
    ///
    /// This is a mock handler that supports the `payload_by_number` protocol.
    /// It always returns: not found (1), version (0). `<https://specs.base.org/protocol/consensus/p2p#payload_by_number>`
    ///
    /// ## Note
    ///
    /// This is used to ensure peer nodes are not penalizing base-nodes for not supporting it.
    /// This feature is being deprecated upstream. Once it is fully removed we will remove this handler.
    pub(super) fn sync_protocol_handler(&mut self) {
        let Some(mut sync_protocol) = self.sync_protocol.take() else {
            return;
        };

        // Spawn a single task to handle all inbound sync substreams serially.
        // The response is a constant 2-byte write — no I/O wait benefits from per-stream
        // concurrency. Inlining eliminates per-substream task allocation and bounds
        // heap exposure.
        tokio::spawn(async move {
            loop {
                let Some((peer_id, mut inbound_stream)) = sync_protocol.next().await else {
                    warn!(target: "gossip", "The sync protocol stream has ended");
                    return;
                };

                trace!(target: "gossip", peer_id = %peer_id, "Received sync request");
                Metrics::sync_requests().increment(1);

                // We return: not found (1), version (0). `<https://specs.base.org/protocol/consensus/p2p#payload_by_number>`
                // Response format: <response> = <res><version><payload>
                // No payload is returned.
                const OUTPUT: [u8; 2] = hex!("0100");
                const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

                match tokio::time::timeout(WRITE_TIMEOUT, inbound_stream.write_all(&OUTPUT)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        error!(target: "gossip", error = %e, peer_id = %peer_id, "Failed to write sync response");
                    }
                    Err(_) => {
                        warn!(target: "gossip", peer_id = %peer_id, "Sync response write timed out");
                    }
                }
            }
        });
    }

    /// Starts the libp2p Swarm.
    ///
    /// - Starts the sync request/response protocol handler.
    /// - Tells the swarm to listen on the given [`Multiaddr`].
    ///
    /// Waits for the swarm to start listen before returning and connecting to peers.
    pub async fn start(&mut self) -> Result<Multiaddr, TransportError<std::io::Error>> {
        // Start the sync request/response protocol handler.
        self.sync_protocol_handler();

        match self.swarm.listen_on(self.addr.clone()) {
            Ok(id) => loop {
                if let SwarmEvent::NewListenAddr { address, listener_id } =
                    self.swarm.select_next_some().await
                    && id == listener_id
                {
                    info!(target: "gossip", address = %address, "Listening on address");

                    self.addr = address.clone();

                    return Ok(address);
                }
            },
            Err(err) => {
                error!(target: "gossip", address = %self.addr, error = %err, "Failed to listen on address");
                Err(err)
            }
        }
    }

    /// Returns the local peer id.
    pub fn local_peer_id(&self) -> &libp2p::PeerId {
        self.swarm.local_peer_id()
    }

    /// Returns a mutable reference to the Swarm's behaviour.
    pub fn behaviour_mut(&mut self) -> &mut Behaviour {
        self.swarm.behaviour_mut()
    }

    /// Attempts to select the next event from the Swarm.
    pub async fn next(&mut self) -> Option<SwarmEvent<Event>> {
        self.swarm.next().await
    }

    /// Returns the number of connected peers.
    pub fn connected_peers(&self) -> usize {
        self.swarm.connected_peers().count()
    }

    /// Aborts pending outbound dials that exceeded the connection gate timeout.
    ///
    /// Established peers are removed from pending bookkeeping but are not disconnected.
    pub fn clear_expired_pending_connections(&mut self) -> usize {
        let expired = self.connection_gate.expired_pending_dials();
        let mut cleared = 0;

        for (peer_id, age) in expired {
            if self.swarm.connected_peers().any(|connected| connected == &peer_id) {
                self.connection_gate.remove_dial(&peer_id);
                debug!(
                    target: "gossip",
                    peer_id = %peer_id,
                    pending_dial_age_seconds = age.as_secs_f64(),
                    "Cleared pending dial bookkeeping for connected peer"
                );
                cleared += 1;
                continue;
            }

            let _ = self.swarm.disconnect_peer_id(peer_id);
            self.connection_gate.remove_dial(&peer_id);
            debug!(
                target: "gossip",
                peer_id = %peer_id,
                pending_dial_age_seconds = age.as_secs_f64(),
                "Aborted expired pending outbound dial"
            );
            cleared += 1;
        }

        cleared
    }

    /// Dials the given [`Enr`].
    pub fn dial(&mut self, enr: Enr) {
        let validation = EnrValidation::validate(&enr, self.handler.rollup_config.l2_chain_id.id());
        if validation.is_invalid() {
            trace!(target: "gossip", chain_id = %self.handler.rollup_config.l2_chain_id.id(), validation = %validation, "Invalid Base ENR");
            return;
        }
        let Some(multiaddr) = PeerUtils::enr_to_multiaddr(&enr) else {
            debug!(target: "gossip", enr = ?enr, "Failed to extract tcp socket from enr");
            Metrics::dial_peer_error("invalid_enr").increment(1.0);
            return;
        };
        if self.static_peers.is_some()
            && ConnectionGater::peer_id_from_addr(&multiaddr)
                .is_some_and(|peer_id| self.is_static_peer(&peer_id))
        {
            // Static peers bypass the gate's redial limits, so leave their dials to the static
            // peer backoff instead of redialing on every discovery hit.
            trace!(target: "gossip", %multiaddr, "Discovered static peer; dialed by its own schedule");
            return;
        }
        self.dial_multiaddr(multiaddr);
    }

    /// Dials the given [`Multiaddr`].
    pub fn dial_multiaddr(&mut self, addr: Multiaddr) {
        // Check if we're allowed to dial the address.
        if let Err(connect_error) = self.connection_gate.can_connect_outbound(&addr) {
            debug!(target: "gossip", ?connect_error, "unable to dial peer");
            return;
        }

        // Extract the peer ID from the address.
        let Some(peer_id) = ConnectionGater::peer_id_from_addr(&addr) else {
            warn!(target: "gossip", peer=?addr, "Failed to extract PeerId from Multiaddr");
            return;
        };

        if self.swarm.connected_peers().any(|p| p == &peer_id) {
            debug!(target: "gossip", peer=?addr, "Already connected to peer, not dialing");
            Metrics::dial_peer_error("already_connected").increment(1.0);
            return;
        }

        // Let the gate know we are dialing the address.
        // Note: libp2p-dns will automatically resolve DNS multiaddrs at the transport layer.
        self.connection_gate.dialing(&addr);

        let dial_opts = DialOpts::peer_id(peer_id)
            .addresses(vec![addr.clone()])
            .condition(PeerCondition::DisconnectedAndNotDialing)
            .build();

        match self.swarm.dial(dial_opts) {
            Ok(_) => {
                trace!(target: "gossip", peer=?addr, "Dialed peer");
                self.connection_gate.dialed(&addr);
                Metrics::dial_peer().increment(1.0);
            }
            Err(e) => {
                error!(target: "gossip", error = ?e, "Failed to connect to peer");
                self.connection_gate.remove_dial(&peer_id);
                Metrics::dial_peer_error("connection_error").increment(1.0);
            }
        }
    }

    /// Aborts every outbound connection attempt currently tracked as pending.
    pub fn clear_pending_connections(&mut self) -> usize {
        let pending_dials = self.connection_gate.pending_dials();
        let count = pending_dials.len();

        for peer_id in pending_dials {
            let _ = self.swarm.disconnect_peer_id(peer_id);
            self.connection_gate.remove_dial(&peer_id);
        }

        if count > 0 {
            info!(
                target: "gossip",
                count,
                "Cleared pending outgoing connections"
            );
        }

        count
    }

    fn handle_gossip_event(&mut self, event: Event) -> Option<NetworkPayloadEnvelope> {
        match event {
            Event::Gossipsub(e) => return self.handle_gossipsub_event(*e),
            Event::Ping(libp2p::ping::Event { peer, result, .. }) => {
                trace!(target: "gossip", ?peer, ?result, "Ping received");

                // If the peer is connected to gossip, record the connection duration.
                if let Some(start_time) = self.peer_connection_start.get(&peer) {
                    let _ping_duration = start_time.elapsed();
                    Metrics::gossip_peer_connection_duration_seconds()
                        .record(_ping_duration.as_secs_f64());
                }

                // Record the peer score in the metrics if available.
                if let Some(_peer_score) = self.behaviour_mut().gossipsub.peer_score(&peer) {
                    Metrics::peer_scores().record(_peer_score);
                }

                let pings = Arc::clone(&self.ping);
                tokio::spawn(async move {
                    if let Ok(time) = result {
                        pings.lock().await.insert(peer, time);
                    }
                });
            }
            Event::Identify(e) => self.handle_identify_event(*e),
            // Don't do anything with stream events as this should be unreachable code.
            Event::Stream => {
                error!(target: "gossip", "Stream events should not be emitted!");
            }
        };

        None
    }

    fn handle_identify_event(&mut self, event: libp2p::identify::Event) {
        match event {
            libp2p::identify::Event::Received { connection_id, peer_id, info } => {
                debug!(target: "gossip", ?connection_id, peer_id = %peer_id, ?info, "Received identify info from peer");
                self.prune_peerstore_for_new_peer(peer_id);
                self.peerstore.put(peer_id, info);
            }
            libp2p::identify::Event::Sent { connection_id, peer_id } => {
                debug!(target: "gossip", ?connection_id, peer_id = %peer_id, "Sent identify info to peer");
            }
            libp2p::identify::Event::Pushed { connection_id, peer_id, info } => {
                debug!(target: "gossip", ?connection_id, peer_id = %peer_id, ?info, "Pushed identify info to peer");
            }
            libp2p::identify::Event::Error { connection_id, peer_id, error } => {
                error!(target: "gossip", ?connection_id, peer_id = %peer_id, ?error, "Error raised while attempting to identify remote");
            }
        }
    }

    fn prune_peerstore_for_new_peer(&mut self, peer_id: PeerId) {
        if self.peerstore.contains(&peer_id) || self.peerstore.len() < self.peerstore.cap().get() {
            return;
        }

        let connected_peers = self.swarm.connected_peers().copied().collect::<HashSet<_>>();
        let peer_to_remove = peerstore_eviction_candidate(&self.peerstore, &connected_peers)
            .expect("peerstore is non-empty when at capacity");

        self.peerstore.pop(&peer_to_remove);

        // This is a cache-level cap, not Lighthouse's full peer lifecycle model. Lighthouse
        // keeps explicit connection state and evicts excess disconnected, untrusted peers; Base
        // currently only has identify metadata here, so prefer disconnected entries and bound
        // the cache until we model peer lifecycle state directly.
        debug!(
            target: "gossip",
            peer_id = %peer_to_remove,
            peerstore_size = self.peerstore.len(),
            peerstore_limit = self.peerstore.cap().get(),
            "Evicted identify info from peerstore"
        );
    }

    /// Handles a [`libp2p::gossipsub::Event`].
    fn handle_gossipsub_event(
        &mut self,
        event: libp2p::gossipsub::Event,
    ) -> Option<NetworkPayloadEnvelope> {
        match event {
            libp2p::gossipsub::Event::Message {
                propagation_source: src,
                message_id: id,
                message,
            } => {
                trace!(target: "gossip", topic = %message.topic, "Received message");
                Metrics::gossip_event("message").increment(1.0);
                if self.handler.topics().contains(&message.topic) {
                    let (status, payload) = self.handler.handle(message);
                    _ = self
                        .swarm
                        .behaviour_mut()
                        .gossipsub
                        .report_message_validation_result(&id, &src, status);
                    return payload;
                }
            }
            libp2p::gossipsub::Event::Subscribed { peer_id, topic } => {
                trace!(target: "gossip", peer_id = %peer_id, topic = ?topic, "Peer subscribed");
                Metrics::gossip_event("subscribed").increment(1.0);
            }
            libp2p::gossipsub::Event::Unsubscribed { peer_id, topic } => {
                trace!(target: "gossip", peer_id = %peer_id, topic = ?topic, "Peer unsubscribed");
                Metrics::gossip_event("unsubscribed").increment(1.0);
            }
            libp2p::gossipsub::Event::SlowPeer { peer_id, .. } => {
                trace!(target: "gossip", peer_id = %peer_id, "Slow peer");
                Metrics::gossip_event("slow_peer").increment(1.0);
            }
            libp2p::gossipsub::Event::GossipsubNotSupported { peer_id } => {
                trace!(target: "gossip", peer_id = %peer_id, "Peer does not support gossipsub");
                Metrics::gossip_event("not_supported").increment(1.0);
            }
        }
        None
    }

    /// Handles the [`SwarmEvent<Event>`].
    pub fn handle_event(&mut self, event: SwarmEvent<Event>) -> Option<NetworkPayloadEnvelope> {
        match event {
            SwarmEvent::Behaviour(behavior_event) => {
                return self.handle_gossip_event(behavior_event);
            }
            SwarmEvent::ConnectionEstablished {
                peer_id,
                connection_id,
                endpoint,
                num_established,
                ..
            } => {
                self.connection_gate.remove_dial(&peer_id);

                if endpoint.is_listener() {
                    let addr = endpoint.get_remote_address();
                    if let Err(error) = self.connection_gate.can_connect_inbound(&peer_id, addr) {
                        debug!(target: "gossip", peer_id = %peer_id, addr = %addr, error = ?error, "Closing blocked inbound connection");
                        self.swarm.close_connection(connection_id);
                        Metrics::gossipsub_connection("blocked_inbound").increment(1.0);
                        return None;
                    }
                }

                let peer_count = self.swarm.connected_peers().count();
                debug!(target: "gossip", peer_id = %peer_id, peer_count, "Connection established");
                Metrics::gossipsub_connection("connected").increment(1.0);
                Metrics::gossip_peer_count().set(peer_count as f64);

                self.peer_connection_start.insert(peer_id, Instant::now());

                if num_established.get() == 1 {
                    self.on_static_peer_connected(peer_id, &endpoint);
                }
            }
            SwarmEvent::OutgoingConnectionError { peer_id: _peer_id, error, .. } => {
                debug!(target: "gossip", error = ?error, "Outgoing connection error");
                // Remove the peer from current_dials so it can be dialed again
                if let Some(peer_id) = _peer_id {
                    self.connection_gate.remove_dial(&peer_id);
                    if let Some(static_peers) = self.static_peers.as_ref()
                        && let Some(next_dial) = static_peers.next_dial(&peer_id)
                    {
                        debug!(
                            target: "gossip",
                            peer_id = %peer_id,
                            error = %error,
                            redial_in_secs = next_dial
                                .saturating_duration_since(tokio::time::Instant::now())
                                .as_secs(),
                            "Static peer dial failed"
                        );
                    }
                }
                Metrics::gossipsub_connection("outgoing_error").increment(1.0);
            }
            SwarmEvent::IncomingConnectionError {
                error, connection_id: _connection_id, ..
            } => {
                debug!(target: "gossip", error = ?error, "Incoming connection error");
                Metrics::gossipsub_connection("incoming_error").increment(1.0);
            }
            SwarmEvent::ConnectionClosed { peer_id, cause, num_established, .. } => {
                let peer_count = self.swarm.connected_peers().count();
                debug!(target: "gossip", ?peer_id, ?cause, peer_count, "Connection closed");
                Metrics::gossipsub_connection("closed").increment(1.0);
                Metrics::gossip_peer_count().set(peer_count as f64);

                // Record the total connection duration.
                if let Some(start_time) = self.peer_connection_start.remove(&peer_id) {
                    Metrics::gossip_peer_connection_duration_seconds()
                        .record(start_time.elapsed().as_secs_f64());
                }

                // Record the peer score in the metrics if available.
                if let Some(_peer_score) = self.behaviour_mut().gossipsub.peer_score(&peer_id) {
                    Metrics::peer_scores().record(_peer_score);
                }

                let pings = Arc::clone(&self.ping);
                tokio::spawn(async move {
                    pings.lock().await.remove(&peer_id);
                });

                // If the connection was initiated by us, remove the peer from the current dials
                // set so that we can dial it again.
                self.connection_gate.remove_dial(&peer_id);

                if num_established == 0 {
                    self.on_static_peer_disconnected(peer_id, cause.as_ref());
                }
            }
            SwarmEvent::NewListenAddr { listener_id, address } => {
                debug!(target: "gossip", reporter_id = ?listener_id, new_address = ?address, "New listen address");
            }
            SwarmEvent::Dialing { peer_id, connection_id } => {
                debug!(target: "gossip", ?peer_id, ?connection_id, "Dialing peer");
            }
            SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {
                debug!(target: "gossip", ?peer_id, ?address, "New external address of peer");
            }
            _ => {
                debug!(target: "gossip", ?event, "Ignoring non-behaviour in event handler");
            }
        };

        None
    }
}

fn peerstore_eviction_candidate<T>(
    peerstore: &LruCache<PeerId, T>,
    connected_peers: &HashSet<PeerId>,
) -> Option<PeerId> {
    peerstore
        .iter()
        .rev()
        .map(|(peer_id, _)| *peer_id)
        .find(|peer_id| !connected_peers.contains(peer_id))
        .or_else(|| peerstore.iter().next_back().map(|(peer_id, _)| *peer_id))
}

#[cfg(test)]
mod tests {
    use alloy_chains::Chain;

    use super::*;

    fn test_driver() -> GossipDriver<ConnectionGater> {
        test_driver_with_static_peers(Vec::new())
    }

    fn test_driver_with_static_peers(
        static_peers: Vec<Multiaddr>,
    ) -> GossipDriver<ConnectionGater> {
        let rollup_config = RollupConfig {
            l2_chain_id: Chain::base_mainnet(),
            block_time: 2,
            ..Default::default()
        };

        let (driver, _signer) = GossipDriver::<ConnectionGater>::builder(
            rollup_config,
            Address::repeat_byte(0x11),
            "/ip4/127.0.0.1/tcp/0".parse().unwrap(),
            Keypair::generate_secp256k1(),
        )
        .with_static_peers(static_peers)
        .build()
        .unwrap();

        driver
    }

    /// Drives both swarms until `done(dialer)` holds or 5 s pass.
    async fn drive_until(
        dialer: &mut GossipDriver<ConnectionGater>,
        listener: &mut GossipDriver<ConnectionGater>,
        done: impl Fn(&GossipDriver<ConnectionGater>, &GossipDriver<ConnectionGater>) -> bool,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !done(dialer, listener) && tokio::time::Instant::now() < deadline {
            tokio::select! {
                Some(event) = dialer.next() => { dialer.handle_event(event); }
                Some(event) = listener.next() => { listener.handle_event(event); }
                _ = tokio::time::sleep_until(deadline) => break,
            }
        }
    }

    #[tokio::test]
    async fn static_peer_is_dialed_at_start_and_redial_scheduled_on_disconnect() {
        let mut listener = test_driver();
        let listener_peer_id = *listener.local_peer_id();
        let mut listener_addr = listener.start().await.unwrap();
        listener_addr.push(libp2p::multiaddr::Protocol::P2p(listener_peer_id));

        let mut dialer = test_driver_with_static_peers(vec![listener_addr]);
        let dialer_peer_id = *dialer.local_peer_id();
        let started = tokio::time::Instant::now();
        assert!(dialer.static_peers_next_due().is_some_and(|due| due <= started));

        dialer.dial_due_static_peers();
        assert!(dialer.connection_gate.current_dials.contains_key(&listener_peer_id));
        drive_until(&mut dialer, &mut listener, |d, l| {
            d.static_peers.as_ref().unwrap().connected() == 1
                && l.swarm.is_connected(&dialer_peer_id)
        })
        .await;
        assert_eq!(dialer.static_peers.as_ref().unwrap().connected(), 1);
        assert_eq!(dialer.static_peers_next_due(), None, "connected static peers are not redialed");

        // The remote drops us: a redial is scheduled after the grown backoff (10 s after the
        // first dial), not immediately.
        assert!(listener.swarm.disconnect_peer_id(dialer_peer_id).is_ok());
        drive_until(&mut dialer, &mut listener, |d, _| {
            d.static_peers.as_ref().unwrap().connected() == 0
        })
        .await;
        assert_eq!(dialer.static_peers.as_ref().unwrap().connected(), 0);
        let due = dialer.static_peers_next_due().expect("redial scheduled");
        let now = tokio::time::Instant::now();
        assert!(due > now + Duration::from_secs(5) && due <= now + Duration::from_secs(10));
        dialer.dial_due_static_peers();
        assert!(
            !dialer.connection_gate.current_dials.contains_key(&listener_peer_id),
            "not redialed before the backoff elapses"
        );
    }

    #[tokio::test]
    async fn discovery_does_not_dial_static_peers() {
        use base_consensus_peers::BaseEnr;
        let key = discv5::enr::CombinedKey::generate_secp256k1();
        let mut enr =
            Enr::builder().ip4(std::net::Ipv4Addr::LOCALHOST).tcp4(1).build(&key).unwrap();
        let opstack = alloy_rlp::encode(BaseEnr::from_chain_id(8453));
        enr.insert_raw_rlp(BaseEnr::OPSTACK_ENR_KEY, opstack.into(), &key).unwrap();
        let addr = PeerUtils::enr_to_multiaddr(&enr).unwrap();
        let peer_id = ConnectionGater::peer_id_from_addr(&addr).unwrap();

        let mut plain = test_driver();
        plain.dial(enr.clone());
        assert!(plain.connection_gate.current_dials.contains_key(&peer_id));

        let mut with_static = test_driver_with_static_peers(vec![addr]);
        with_static.dial(enr);
        assert!(!with_static.connection_gate.current_dials.contains_key(&peer_id));
    }

    #[tokio::test]
    async fn clear_pending_connections_aborts_known_peer_dials() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_task = tokio::spawn(async move {
            if let Ok((_socket, _addr)) = listener.accept().await {
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        let peer_id = PeerId::random();
        let addr = format!("/ip4/127.0.0.1/tcp/{port}/p2p/{peer_id}").parse().unwrap();
        let mut driver = test_driver();

        driver.dial_multiaddr(addr);
        assert!(driver.connection_gate.current_dials.contains_key(&peer_id));

        assert_eq!(driver.clear_pending_connections(), 1);
        assert!(!driver.connection_gate.current_dials.contains_key(&peer_id));

        let event = tokio::time::timeout(Duration::from_secs(1), driver.next())
            .await
            .expect("clearing a known-peer dial should emit an event")
            .expect("swarm should emit the aborted dial event");

        match event {
            SwarmEvent::OutgoingConnectionError {
                peer_id: Some(aborted_peer),
                error: libp2p::swarm::DialError::Aborted,
                ..
            } => assert_eq!(aborted_peer, peer_id),
            event => panic!("expected aborted outgoing connection event, got {event:?}"),
        }

        accept_task.abort();
    }

    #[test]
    fn clear_pending_connections_returns_zero_when_empty() {
        let mut driver = test_driver();

        assert_eq!(driver.clear_pending_connections(), 0);
    }

    #[tokio::test]
    async fn established_connections_are_removed_from_pending_dials() {
        let mut dialer = test_driver();
        let mut listener = test_driver();
        let listener_peer_id = *listener.local_peer_id();
        let mut listener_addr = listener.start().await.unwrap();
        listener_addr.push(libp2p::multiaddr::Protocol::P2p(listener_peer_id));

        dialer.dial_multiaddr(listener_addr);
        assert!(dialer.connection_gate.current_dials.contains_key(&listener_peer_id));

        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < deadline
            && dialer.connection_gate.current_dials.contains_key(&listener_peer_id)
        {
            tokio::select! {
                event = dialer.next() => {
                    if let Some(event) = event {
                        dialer.handle_event(event);
                    }
                }
                event = listener.next() => {
                    if let Some(event) = event {
                        listener.handle_event(event);
                    }
                }
                _ = tokio::time::sleep_until(deadline) => break,
            }
        }

        assert!(!dialer.connection_gate.current_dials.contains_key(&listener_peer_id));
    }

    #[test]
    fn test_peerstore_eviction_candidate_prefers_disconnected_peer() {
        let connected_peer = PeerId::random();
        let disconnected_peer = PeerId::random();
        let mut peerstore = LruCache::new(NonZeroUsize::new(1024).unwrap());
        peerstore.put(disconnected_peer, ());
        peerstore.put(connected_peer, ());
        let connected_peers = HashSet::from([connected_peer]);

        let candidate = peerstore_eviction_candidate(&peerstore, &connected_peers);

        assert_eq!(candidate, Some(disconnected_peer));
    }

    #[test]
    fn test_peerstore_eviction_candidate_uses_lru_disconnected_peer() {
        let oldest_disconnected_peer = PeerId::random();
        let newest_disconnected_peer = PeerId::random();
        let connected_peer = PeerId::random();
        let mut peerstore = LruCache::new(NonZeroUsize::new(1024).unwrap());
        peerstore.put(oldest_disconnected_peer, ());
        peerstore.put(newest_disconnected_peer, ());
        peerstore.put(connected_peer, ());
        let connected_peers = HashSet::from([connected_peer]);

        let candidate = peerstore_eviction_candidate(&peerstore, &connected_peers);

        assert_eq!(candidate, Some(oldest_disconnected_peer));
    }

    #[test]
    fn test_peerstore_eviction_candidate_falls_back_to_lru_connected_peer() {
        let least_recent_peer = PeerId::random();
        let most_recent_peer = PeerId::random();
        let mut peerstore = LruCache::new(NonZeroUsize::new(1024).unwrap());
        peerstore.put(least_recent_peer, ());
        peerstore.put(most_recent_peer, ());
        let connected_peers = HashSet::from([least_recent_peer, most_recent_peer]);

        let candidate = peerstore_eviction_candidate(&peerstore, &connected_peers);

        assert_eq!(candidate, Some(least_recent_peer));
    }
}
