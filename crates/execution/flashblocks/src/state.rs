//! Flashblocks state management.

use std::{sync::Arc, time::Instant};

use alloy_consensus::Header;
use arc_swap::{ArcSwapOption, Guard};
use base_common_chains::Upgrades;
use base_common_consensus::BaseBlock;
use base_common_flashblocks::Flashblock;
use reth_chainspec::{ChainSpecProvider, EthChainSpec};
use reth_primitives_traits::RecoveredBlock;
use reth_provider::{BlockReaderIdExt, StateProviderFactory};
use tokio::sync::{
    Mutex,
    broadcast::{self, Sender},
    mpsc,
};

use crate::{
    FlashblocksAPI, FlashblocksReceiver, PendingBlocks,
    processor::{StateProcessor, StateUpdate},
};

// Buffer 4s of live Flashblocks; recovery never republishes its backlog into this channel.
const BUFFER_SIZE: usize = 20;
const RESET_BUFFER_SIZE: usize = 16;

/// Identifies a speculative Flashblock lineage that was invalidated locally.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlashblocksReset {
    /// Block whose speculative execution diverged.
    pub block_number: u64,
    /// Flashblock index whose execution exposed the divergence.
    pub flashblock_index: u64,
}

#[derive(Debug)]
pub(crate) struct TimedStateUpdate {
    pub(crate) received_at: Instant,
    pub(crate) update: StateUpdate,
}

impl TimedStateUpdate {
    fn new(update: StateUpdate) -> Self {
        Self { received_at: Instant::now(), update }
    }
}

/// Manages the pending flashblock state and processes incoming updates.
#[derive(Debug)]
pub struct FlashblocksState {
    pending_blocks: Arc<ArcSwapOption<PendingBlocks>>,
    canonical_queue: mpsc::UnboundedSender<TimedStateUpdate>,
    canonical_rx: Arc<Mutex<mpsc::UnboundedReceiver<TimedStateUpdate>>>,
    flashblock_queue: mpsc::UnboundedSender<TimedStateUpdate>,
    flashblock_rx: Arc<Mutex<mpsc::UnboundedReceiver<TimedStateUpdate>>>,
    flashblock_sender: Sender<Arc<PendingBlocks>>,
    reset_sender: Sender<FlashblocksReset>,
    max_pending_blocks_depth: u64,
}

impl FlashblocksState {
    /// Creates a new flashblocks state manager.
    ///
    /// The state is created without a client. Call [`start`](Self::start) with a client
    /// to spawn the state processor after the node is launched.
    pub fn new(max_pending_blocks_depth: u64) -> Self {
        let (canonical_queue, canonical_rx) = mpsc::unbounded_channel::<TimedStateUpdate>();
        let (flashblock_queue, flashblock_rx) = mpsc::unbounded_channel::<TimedStateUpdate>();
        let pending_blocks: Arc<ArcSwapOption<PendingBlocks>> = Arc::new(ArcSwapOption::new(None));
        let (flashblock_sender, _) = broadcast::channel(BUFFER_SIZE);
        let (reset_sender, _) = broadcast::channel(RESET_BUFFER_SIZE);

        Self {
            pending_blocks,
            canonical_queue,
            canonical_rx: Arc::new(Mutex::new(canonical_rx)),
            flashblock_queue,
            flashblock_rx: Arc::new(Mutex::new(flashblock_rx)),
            flashblock_sender,
            reset_sender,
            max_pending_blocks_depth,
        }
    }

    /// Starts the flashblocks state processor with the given client.
    ///
    /// This spawns a background task that processes canonical blocks and flashblocks.
    /// Should be called after the node is launched and the provider is available.
    pub fn start<Client>(&self, client: Client)
    where
        Client: StateProviderFactory
            + ChainSpecProvider<ChainSpec: EthChainSpec<Header = Header> + Upgrades>
            + BlockReaderIdExt<Header = Header>
            + Clone
            + 'static,
    {
        let state_processor = StateProcessor::new(
            client,
            Arc::clone(&self.pending_blocks),
            self.max_pending_blocks_depth,
            Arc::clone(&self.canonical_rx),
            Arc::clone(&self.flashblock_rx),
            self.flashblock_sender.clone(),
            self.reset_sender.clone(),
        );

        tokio::spawn(async move {
            state_processor.start().await;
        });
    }

    /// Handles a canonical block being received.
    pub fn on_canonical_block_received(&self, block: RecoveredBlock<BaseBlock>) {
        let block_number = block.number;
        match self.canonical_queue.send(TimedStateUpdate::new(StateUpdate::Canonical(block))) {
            Ok(_) => {
                info!(message = "added canonical block to processing queue", block_number)
            }
            Err(e) => {
                error!(message = "could not add canonical block to processing queue", block_number, error = %e);
            }
        }
    }

    /// Subscribes to pending-state invalidations that require consumers to fail closed.
    pub fn subscribe_to_resets(&self) -> broadcast::Receiver<FlashblocksReset> {
        self.reset_sender.subscribe()
    }
}

impl FlashblocksReceiver for FlashblocksState {
    fn on_flashblock_received(&self, flashblock: Flashblock) {
        let flashblock_index = flashblock.index;
        let block_number = flashblock.metadata.block_number;
        match self.flashblock_queue.send(TimedStateUpdate::new(StateUpdate::Flashblock(flashblock)))
        {
            Ok(_) => {
                debug!(
                    message = "added flashblock to processing queue",
                    block_number, flashblock_index,
                );
            }
            Err(e) => {
                error!(message = "could not add flashblock to processing queue", block_number, flashblock_index, error = %e);
            }
        }
    }
}

impl Default for FlashblocksState {
    fn default() -> Self {
        Self::new(10)
    }
}

impl FlashblocksAPI for FlashblocksState {
    fn get_pending_blocks(&self) -> Guard<Option<Arc<PendingBlocks>>> {
        self.pending_blocks.load()
    }

    fn subscribe_to_flashblocks(&self) -> broadcast::Receiver<Arc<PendingBlocks>> {
        self.flashblock_sender.subscribe()
    }
}

impl FlashblocksState {
    /// Sets the pending blocks directly for testing purposes.
    ///
    /// This bypasses the normal flashblock processing pipeline and allows
    /// tests to inject a pre-built `PendingBlocks` state.
    pub fn set_pending_blocks_for_testing(&self, pending_blocks: Option<PendingBlocks>) {
        self.pending_blocks.store(pending_blocks.map(Arc::new));
    }
}
