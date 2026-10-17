use std::{fmt::Debug, sync::Arc, time::Duration};

use alloy_eips::BlockNumberOrTag;
use alloy_provider::RootProvider;
use base_common_genesis::RollupConfig;
use base_common_network::Base;
use base_consensus_engine::{BaseEngineClient, EngineClient};
use base_consensus_rpc::RpcBuilder;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    NetworkActor, NetworkBuilder, NodeActor, ShutdownSignal,
    follow::{
        engine::EngineApiFollowEngine,
        error::FollowError,
        gossip::{
            FollowP2pMode, GossipCacheEngineClient, GossipFirstClient, GossipPayloadCache,
            log_network_exit,
        },
        local::{FollowLocalClient, LocalL2Client},
        proof_gate::{ActiveProofGate, NoopProofGate, ProofGate},
        rpc::FollowRpcActor,
        runtime::FollowRuntime,
        source::RemoteL2Client,
    },
};

/// A lightweight node that follows another L2 node by fetching source L2
/// payloads and inserting them into the local execution engine.
#[derive(Debug)]
pub struct FollowNode<E = BaseEngineClient<RootProvider, RootProvider<Base>>>
where
    E: EngineClient + Debug + 'static,
{
    config: Arc<RollupConfig>,
    engine_client: Arc<E>,
    l1_provider: RootProvider,
    local_l2_provider: RootProvider<Base>,
    l2_source: RemoteL2Client,
    proofs_enabled: bool,
    proofs_max_blocks_ahead: u64,
    insert_delay: Duration,
    stall_exit: Option<Duration>,
    rpc_builder: Option<RpcBuilder>,
    p2p: Option<FollowP2pConfig>,
}

/// Upper bound on starting the libp2p network. Past it the follower runs on RPC only, so a
/// listener that never reports its address cannot hold back the follower or its stall watchdog.
pub const P2P_START_TIMEOUT: Duration = Duration::from_secs(15);

/// A background lookup of the current unsafe-block signer (e.g. from L1 `SystemConfig`).
pub type SignerLookup =
    std::pin::Pin<Box<dyn std::future::Future<Output = Option<alloy_primitives::Address>> + Send>>;

/// Optional P2P unsafe-block gossip for the follow node.
pub struct FollowP2pConfig {
    /// How gossip payloads are used. [`FollowP2pMode::Off`] is the same as no config.
    pub mode: FollowP2pMode,
    /// The libp2p network (discovery + gossip), built from the standard P2P flags.
    pub network: std::sync::Mutex<Option<NetworkBuilder>>,
    /// Resolves the current signer after the network started; a different address replaces
    /// the one the network was built with.
    pub signer_lookup: std::sync::Mutex<Option<SignerLookup>>,
}

impl Debug for FollowP2pConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FollowP2pConfig").field("mode", &self.mode).finish_non_exhaustive()
    }
}

impl FollowP2pConfig {
    /// Creates the config.
    pub const fn new(mode: FollowP2pMode, network: NetworkBuilder) -> Self {
        Self {
            mode,
            network: std::sync::Mutex::new(Some(network)),
            signer_lookup: std::sync::Mutex::new(None),
        }
    }

    /// Adds a background signer lookup.
    pub fn with_signer_lookup(self, lookup: SignerLookup) -> Self {
        Self { signer_lookup: std::sync::Mutex::new(Some(lookup)), ..self }
    }
}

/// Runtime dependencies and options for a [`FollowNode`].
#[derive(Debug)]
pub struct FollowNodeConfig<E = BaseEngineClient<RootProvider, RootProvider<Base>>>
where
    E: EngineClient + Debug + 'static,
{
    /// The rollup configuration for the L2 chain.
    pub rollup_config: Arc<RollupConfig>,
    /// Client used to insert payloads into the local execution engine.
    pub engine_client: Arc<E>,
    /// Provider for reading local L2 state.
    pub local_l2_provider: RootProvider<Base>,
    /// Provider used to check L2 block origins against canonical L1.
    pub l1_provider: RootProvider,
    /// Source L2 client used to fetch payloads to follow.
    pub l2_source: RemoteL2Client,
    /// Optional RPC server configuration.
    pub rpc_builder: Option<RpcBuilder>,
    /// Whether to gate sync behind proofs progress.
    pub proofs_enabled: bool,
    /// Maximum blocks the follow node may advance beyond proofs progress.
    pub proofs_max_blocks_ahead: u64,
    /// Delay after each successful source payload insert.
    pub insert_delay: Duration,
    /// Exit with an error when no block is imported for this long (`None`: never).
    pub stall_exit: Option<Duration>,
    /// Optional P2P unsafe-block gossip (`None`: today's RPC-only follower).
    pub p2p: Option<FollowP2pConfig>,
}

impl<E> FollowNode<E>
where
    E: EngineClient + Debug + 'static,
{
    /// Creates a new [`FollowNode`].
    pub fn new(config: FollowNodeConfig<E>) -> Self {
        Self {
            config: config.rollup_config,
            engine_client: config.engine_client,
            l1_provider: config.l1_provider,
            local_l2_provider: config.local_l2_provider,
            l2_source: config.l2_source,
            rpc_builder: config.rpc_builder,
            proofs_enabled: config.proofs_enabled,
            proofs_max_blocks_ahead: config.proofs_max_blocks_ahead,
            insert_delay: config.insert_delay,
            stall_exit: config.stall_exit,
            p2p: config.p2p,
        }
    }

    /// Starts the follow node, creating a fresh cancellation token.
    pub async fn start(&self) -> Result<(), FollowError> {
        self.start_with_cancellation(CancellationToken::new()).await
    }

    /// Starts the follow node, driven by an externally supplied cancellation token.
    ///
    /// Cancelling the token stops the follow runtime, which lets an embedding caller (e.g. the
    /// unified binary running execution in-process) coordinate a clean shutdown across both
    /// services. The follow node also stops on an internal [`ShutdownSignal`].
    pub async fn start_with_cancellation(
        &self,
        cancellation: CancellationToken,
    ) -> Result<(), FollowError> {
        let local = Arc::new(LocalL2Client::new(
            self.local_l2_provider.clone(),
            self.l1_provider.clone(),
            Arc::clone(&self.config),
        ));
        let latest = local
            .block_info(BlockNumberOrTag::Latest)
            .await?
            .ok_or(FollowError::LocalBlockUnavailable(BlockNumberOrTag::Latest))?;
        let safe = local.block_info(BlockNumberOrTag::Safe).await?.unwrap_or_default();
        let finalized = local.block_info(BlockNumberOrTag::Finalized).await?.unwrap_or_default();
        let engine = Arc::new(EngineApiFollowEngine::new(
            Arc::clone(&self.engine_client),
            Arc::clone(&self.config),
            latest,
            safe,
            finalized,
        ));
        let rpc = self
            .rpc_builder
            .clone()
            .map(|rpc_builder| FollowRpcActor::new(rpc_builder, Arc::clone(&local)));

        if self.proofs_enabled {
            let proof_gate =
                ActiveProofGate::new(Arc::clone(&local), self.proofs_max_blocks_ahead).await?;
            self.start_runtime(local, engine, latest, proof_gate, rpc, cancellation).await
        } else {
            self.start_runtime(local, engine, latest, NoopProofGate, rpc, cancellation).await
        }
    }

    async fn start_runtime<Gate>(
        &self,
        local: Arc<LocalL2Client>,
        engine: Arc<EngineApiFollowEngine<E>>,
        latest: base_protocol::L2BlockInfo,
        proof_gate: Gate,
        rpc: Option<FollowRpcActor<LocalL2Client>>,
        cancellation: CancellationToken,
    ) -> Result<(), FollowError>
    where
        Gate: ProofGate + 'static,
    {
        let mut tasks = JoinSet::new();
        let (head_sender, head_notifications) = tokio::sync::watch::channel(0);

        let p2p_mode = self.p2p.as_ref().map_or(FollowP2pMode::Off, |p2p| p2p.mode);
        let network = self
            .p2p
            .as_ref()
            .filter(|_| p2p_mode.enabled())
            .and_then(|p2p| p2p.network.lock().unwrap_or_else(|e| e.into_inner()).take());
        let cache = Arc::new(GossipPayloadCache::default());
        if let Some(network) = network {
            // The network runs beside the follower, never inside its task set: a P2P failure
            // only leaves the follower on its RPC sources.
            let client = GossipCacheEngineClient::new(
                Arc::clone(&cache),
                p2p_mode,
                Some(head_sender.clone()),
            );
            let signer_lookup = self
                .p2p
                .as_ref()
                .and_then(|p2p| p2p.signer_lookup.lock().unwrap_or_else(|e| e.into_inner()).take());
            let started = tokio::time::timeout(
                P2P_START_TIMEOUT,
                NetworkActor::new(client, cancellation.child_token(), network),
            )
            .await;
            match started {
                Ok(Ok((inbound, actor))) => {
                    info!(target: "follow::p2p", mode = %p2p_mode, "P2P gossip network started");
                    if let Some(lookup) = signer_lookup {
                        let signer_tx = inbound.signer.clone();
                        tokio::spawn(async move {
                            if let Some(signer) = lookup.await
                                && signer_tx.send(signer).await.is_err()
                            {
                                warn!(target: "follow::p2p", "P2P network gone before the signer update");
                            }
                        });
                    }
                    tokio::spawn(async move {
                        // Holding the inbound senders keeps the actor's signer channel open.
                        let _inbound = inbound;
                        log_network_exit(actor.start(()).await);
                    });
                }
                Ok(Err(error)) => {
                    warn!(target: "follow::p2p", %error,
                        "P2P gossip network failed to start; following RPC sources only");
                }
                Err(_) => {
                    warn!(target: "follow::p2p", timeout_secs = P2P_START_TIMEOUT.as_secs(),
                        "P2P gossip network did not start in time; following RPC sources only");
                }
            }
        }

        let runtime = FollowRuntime::new(
            Arc::clone(&local),
            Arc::new(GossipFirstClient::new(self.l2_source.clone(), cache, p2p_mode)),
            engine,
            cancellation.clone(),
            latest,
            proof_gate,
            self.insert_delay,
        )
        .with_stall_exit(self.stall_exit);

        tasks.spawn(self.l2_source.clone().follow_heads(head_sender, cancellation.clone()));
        tasks.spawn(runtime.start(Some(head_notifications)));
        if let Some(rpc) = rpc {
            tasks.spawn(rpc.start(cancellation.clone()));
        }

        tokio::select! {
            result = tasks.join_next() => {
                cancellation.cancel();
                if let Some(result) = result {
                    result??;
                }
                while let Some(result) = tasks.join_next().await {
                    result??;
                }
            }
            _ = ShutdownSignal::wait() => {
                cancellation.cancel();
                while let Some(result) = tasks.join_next().await {
                    result??;
                }
            }
        }
        Ok(())
    }
}
