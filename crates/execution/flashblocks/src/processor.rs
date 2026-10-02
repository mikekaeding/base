//! Flashblocks state processor.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex as StdMutex, MutexGuard as StdMutexGuard},
    time::Instant,
};

use alloy_consensus::{
    Block, BlockBody, Header,
    transaction::{Recovered, SignerRecoverable},
};
use alloy_network::TransactionResponse;
use alloy_primitives::{Address, BlockNumber};
use alloy_rpc_types_eth::state::StateOverride;
use arc_swap::ArcSwapOption;
use base_common_chains::Upgrades;
use base_common_consensus::{BaseBlock, BaseTxEnvelope};
use base_common_flashblocks::Flashblock;
use base_execution_evm::{BaseEvmConfig, BaseNextBlockEnvAttributes};
use rayon::prelude::*;
use reth_chainspec::{ChainSpecProvider, EthChainSpec};
use reth_evm::ConfigureEvm;
use reth_primitives_traits::RecoveredBlock;
use reth_provider::{BlockReaderIdExt, StateProviderBox, StateProviderFactory};
use reth_revm::{State, database::StateProviderDatabase};
use revm_database::states::bundle_state::BundleRetention;
use tokio::sync::{Mutex, broadcast::Sender, mpsc::UnboundedReceiver};

use crate::FlashblocksReset;
use crate::{
    AssembledBlock, BlockAssembler, ExecutionError, FlashblockCache, PendingBlocks,
    PendingBlocksBuilder, PendingStateBuilder, ProtocolError, ProviderError, Result,
    StateProcessorError,
    metrics::Metrics,
    validation::{
        CanonicalBlockReconciler, FlashblockSequenceValidator, ReconciliationStrategy,
        ReorgDetector, SequenceValidationResult,
    },
};

type PendingExecutionDb = State<StateProviderDatabase<StateProviderBox>>;

#[derive(Debug)]
struct LivePendingState {
    db: PendingExecutionDb,
    state_overrides: StateOverride,
}

/// Messages consumed by the state processor.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum StateUpdate {
    /// New canonical block to reconcile against pending state.
    Canonical(RecoveredBlock<BaseBlock>),
    /// Incoming flashblock payload to extend pending state.
    Flashblock(Flashblock),
}

/// Processes flashblocks and canonical blocks to keep pending state updated.
#[derive(Debug)]
pub struct StateProcessor<Client> {
    rx: Arc<Mutex<UnboundedReceiver<StateUpdate>>>,
    pending_blocks: Arc<ArcSwapOption<PendingBlocks>>,
    max_depth: u64,
    client: Client,
    sender: Sender<Arc<PendingBlocks>>,
    reset_sender: Sender<FlashblocksReset>,
    cache: Arc<Mutex<FlashblockCache>>,
    live_state: StdMutex<Option<LivePendingState>>,
}

impl<Client> StateProcessor<Client>
where
    Client: StateProviderFactory
        + ChainSpecProvider<ChainSpec: EthChainSpec<Header = Header> + Upgrades>
        + BlockReaderIdExt<Header = Header>
        + Clone
        + 'static,
{
    fn lock_live_state(&self) -> StdMutexGuard<'_, Option<LivePendingState>> {
        self.live_state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn clear_live_state(&self) {
        *self.lock_live_state() = None;
    }

    fn invalidate_pending_state(&self, block_number: u64, flashblock_index: u64) {
        self.clear_live_state();
        self.pending_blocks.store(None);
        // Subscribers may not exist during startup; never retain an invalid overlay for them.
        let _ = self.reset_sender.send(FlashblocksReset { block_number, flashblock_index });
    }

    fn set_live_state(&self, db: PendingExecutionDb, state_overrides: StateOverride) {
        *self.lock_live_state() = Some(LivePendingState { db, state_overrides });
    }

    /// Returns the node's canonical tip, or `None` when the provider cannot report it.
    ///
    /// Every tip check treats `None` as unknown and leaves pending state as it is, so a
    /// provider hiccup degrades to the behaviour from before tip anchoring instead of
    /// dropping a snapshot that may well be current. This is the only place that decides that
    /// policy, so the checks below cannot disagree about it.
    fn canonical_tip(&self) -> Option<BlockNumber> {
        match self.client.best_block_number() {
            Ok(best) => Some(best),
            Err(e) => {
                warn!(
                    message = "could not read canonical tip, leaving pending state unchanged",
                    error = %e,
                );
                None
            }
        }
    }

    /// Returns `true` when the node has already canonicalized the flashblock's block.
    fn is_superseded(&self, flashblock: &Flashblock) -> bool {
        self.canonical_tip().is_some_and(|best| flashblock.metadata.block_number <= best)
    }

    /// Returns the canonical height to reconcile against.
    ///
    /// Canonical and flashblock updates share one unbounded queue applied FIFO, so the height
    /// of a queued notification can be far behind the height the node has actually reached.
    /// Taking the greater of the two keeps reconciliation anchored to the real chain.
    fn effective_canonical_number(&self, notified: BlockNumber) -> BlockNumber {
        self.canonical_tip().map_or(notified, |best| notified.max(best))
    }

    /// Returns `true` when `pending_blocks`' tip is within `max_depth` of canonical height `best`.
    ///
    /// Measured from latest, not earliest. [`PendingBlocksBuilder::from_previous`] freezes the
    /// earliest header, so `best - earliest` is snapshot width; the reconciler rebuilds that.
    fn is_tip_near(&self, pending_blocks: &PendingBlocks, best: BlockNumber) -> bool {
        best.saturating_sub(pending_blocks.latest_block_number()) <= self.max_depth
    }

    /// Returns `true` when `pending_blocks` is usable as live pending state, meaning its tip
    /// is still near the canonical tip and still extends past it.
    ///
    /// This deliberately compares heights only. Detecting that the anchor itself was reorged
    /// out means comparing its hash against canonical history, which is a statement about
    /// whether an incoming payload's declared parent is real, so it belongs in payload
    /// validation rather than in a staleness check on already-published state. Fork-stranded
    /// pending is caught here when [`ReorgDetector`] sees the replaced block's transactions,
    /// and by consumers, which must compare their parent hash against
    /// [`PendingBlocks::parent_hash`] before reusing cached execution results.
    fn extends_canonical_tip(&self, pending_blocks: &PendingBlocks) -> bool {
        let Some(best) = self.canonical_tip() else { return true };

        if !self.is_tip_near(pending_blocks, best) {
            debug!(
                message = "pending snapshot tip too far behind canonical tip, dropping",
                canonical_tip = best,
                latest_pending_block = pending_blocks.latest_block_number(),
                max_depth = self.max_depth,
            );
            return false;
        }

        if pending_blocks.latest_block_number() <= best {
            debug!(
                message = "pending snapshot no longer extends canonical tip, dropping",
                canonical_tip = best,
                latest_pending_block = pending_blocks.latest_block_number(),
            );
            return false;
        }

        true
    }

    /// Returns the published snapshot, dropping it first if its tip is too far behind the
    /// canonical tip to become usable again.
    ///
    /// `FlashblocksState` drops stranded overlays as canonical notifications arrive, which is
    /// what keeps staleness bounded by chain progress rather than by how long a single update
    /// takes to apply. This check covers the advances that path did not report: notifications
    /// carry the committed height, while the provider may already be further along, and the
    /// two differ during the window between a notification and the block becoming visible.
    /// Snapshots that merely stopped extending the tip are left for
    /// [`Self::process_canonical_block`] to reconcile, so its catch-up path keeps reporting.
    fn load_pending_or_drop_stale(&self) -> Option<Arc<PendingBlocks>> {
        let pending_blocks = self.pending_blocks.load_full()?;

        let Some(best) = self.canonical_tip() else { return Some(pending_blocks) };
        if self.is_tip_near(&pending_blocks, best) {
            return Some(pending_blocks);
        }

        debug!(
            message = "pending snapshot tip too far behind canonical tip, dropping",
            canonical_tip = best,
            latest_pending_block = pending_blocks.latest_block_number(),
            max_depth = self.max_depth,
        );
        Metrics::pending_drop_stale().increment(1);
        self.invalidate_pending_state(
            pending_blocks.latest_block_number(),
            pending_blocks.latest_flashblock_index(),
        );
        None
    }

    /// Publishes a freshly built snapshot, unless it no longer extends the canonical tip.
    ///
    /// Every build path funnels through here, so this is the one place that has to enforce
    /// the invariant: pending state either tracks flashblocks on the node's current canonical
    /// tip or is absent. That holds no matter which path produced the snapshot, and it makes
    /// recovery automatic, because the next tip-rooted flashblock rebuilds from `None`.
    fn publish_pending_blocks(
        &self,
        mut pending_blocks_builder: PendingBlocksBuilder,
        mut db: PendingExecutionDb,
        state_overrides: StateOverride,
    ) -> Result<Option<Arc<PendingBlocks>>> {
        db.merge_transitions(BundleRetention::Reverts);
        pending_blocks_builder.with_bundle_state(db.bundle_state.clone());
        pending_blocks_builder.with_state_overrides(state_overrides.clone());

        let pending_blocks = Arc::new(pending_blocks_builder.build()?);

        if !self.extends_canonical_tip(&pending_blocks) {
            Metrics::pending_drop_stale().increment(1);
            self.clear_live_state();
            return Ok(None);
        }

        self.set_live_state(db, state_overrides);

        Ok(Some(pending_blocks))
    }

    /// Creates a new state processor wired to the provided channels and state.
    pub fn new(
        client: Client,
        pending_blocks: Arc<ArcSwapOption<PendingBlocks>>,
        max_depth: u64,
        rx: Arc<Mutex<UnboundedReceiver<StateUpdate>>>,
        sender: Sender<Arc<PendingBlocks>>,
        reset_sender: Sender<FlashblocksReset>,
    ) -> Self {
        let cache = client
            .best_block_number()
            .map_or_else(|_| FlashblockCache::new(0), FlashblockCache::new);

        Self {
            pending_blocks,
            client,
            max_depth,
            rx,
            sender,
            reset_sender,
            cache: Arc::new(Mutex::new(cache)),
            live_state: StdMutex::new(None),
        }
    }

    /// Processes updates from the queue until the channel closes.
    pub async fn start(&self) {
        while let Some(update) = self.rx.lock().await.recv().await {
            let prev_pending_blocks = self.load_pending_or_drop_stale();
            match update {
                StateUpdate::Canonical(block) => {
                    debug!(message = "processing canonical block", block_number = block.number);
                    match self.process_canonical_block(prev_pending_blocks, &block) {
                        Ok(new_pending_blocks) => {
                            self.pending_blocks.swap(new_pending_blocks);

                            let mut cache = self.cache.lock().await;
                            cache.update_canonical(block.number);
                            let cached = cache.drain(block.number + 1);
                            drop(cache);

                            if !cached.is_empty() {
                                debug!(
                                    message = "replaying cached flashblocks after canonical block",
                                    canonical_block = block.number,
                                    cached_count = cached.len(),
                                );
                                for flashblock in cached {
                                    let fb_prev = self.load_pending_or_drop_stale();
                                    self.apply_flashblock(fb_prev, flashblock).await;
                                }
                            }
                        }
                        Err(e) => {
                            self.invalidate_pending_state(block.number, 0);
                            error!(message = "could not process canonical block", error = %e);
                        }
                    }
                }
                StateUpdate::Flashblock(flashblock) => {
                    debug!(
                        message = "processing flashblock",
                        block_number = flashblock.metadata.block_number,
                        flashblock_index = flashblock.index
                    );
                    self.apply_flashblock(prev_pending_blocks, flashblock).await;
                }
            }
        }
    }

    /// Applies a flashblock, unless the node has already canonicalized its block.
    ///
    /// Executing a flashblock for an already-canonical block cannot produce a publishable
    /// snapshot, and doing it anyway is what let the queue lag sustain itself: the backlog
    /// of superseded payloads consumed the processor while fresh payloads waited behind them.
    /// `FlashblocksState` rejects payloads that are already superseded when they arrive, so
    /// what reaches here is a payload that was fresh when queued and went stale while waiting,
    /// or one replayed from the cache after its canonical block landed.
    async fn apply_flashblock(
        &self,
        prev_pending_blocks: Option<Arc<PendingBlocks>>,
        flashblock: Flashblock,
    ) {
        if self.is_superseded(&flashblock) {
            debug!(
                message = "skipping flashblock for already canonical block",
                block_number = flashblock.metadata.block_number,
                flashblock_index = flashblock.index,
            );
            Metrics::flashblock_superseded().increment(1);
            return;
        }

        let start_time = Instant::now();
        let block_number = flashblock.metadata.block_number;
        let flashblock_index = flashblock.index;
        match self.process_flashblock(prev_pending_blocks, &flashblock) {
            Ok(new_pending_blocks) => {
                if let Some(ref pb) = new_pending_blocks {
                    _ = self.sender.send(Arc::clone(pb));
                }
                self.pending_blocks.swap(new_pending_blocks);
                Metrics::block_processing_duration().record(start_time.elapsed());
            }
            Err(e) => {
                match e {
                    StateProcessorError::Provider(ProviderError::MissingCanonicalHeader {
                        ..
                    }) => {
                        let inserted = self.cache.lock().await.insert(flashblock);
                        if inserted {
                            debug!(message = "cached flashblock pending canonical block", error = %e);
                            return;
                        }
                    }
                    StateProcessorError::MissingFirstFlashblock => {
                        let mut cache = self.cache.lock().await;
                        // this error should only occur for non-zero index flashblocks, but check here for index safety
                        if flashblock.index > 0
                            && cache.has_flashblock(
                                flashblock.metadata.block_number,
                                flashblock.index - 1,
                            )
                            && cache.insert(flashblock)
                        {
                            return;
                        }
                        // we should ignore this error since it doesn't necessarily indicate a problem
                        return;
                    }
                    StateProcessorError::ParentUnverified { .. }
                    | StateProcessorError::ParentPrefixMismatch { .. }
                    | StateProcessorError::ParentHashMismatch { .. } => {
                        debug!(message = "holding Flashblock for authenticated canonical parent", error = %e);
                        self.cache.lock().await.insert(flashblock);
                        self.invalidate_pending_state(block_number, flashblock_index);
                        return;
                    }
                    _ => {}
                }

                self.invalidate_pending_state(block_number, flashblock_index);
                // skip logging expected caching case
                if !matches!(
                    e,
                    StateProcessorError::Provider(ProviderError::MissingCanonicalHeader { .. })
                ) {
                    error!(message = "could not process Flashblock", error = %e);
                    Metrics::block_processing_error().increment(1);
                }
            }
        }
    }

    #[instrument(level = "debug", skip_all, fields(block_number = block.number))]
    fn process_canonical_block(
        &self,
        prev_pending_blocks: Option<Arc<PendingBlocks>>,
        block: &RecoveredBlock<BaseBlock>,
    ) -> Result<Option<Arc<PendingBlocks>>> {
        let pending_blocks = match &prev_pending_blocks {
            Some(pb) => pb,
            None => {
                debug!(message = "no pending state to update with canonical block, skipping");
                self.clear_live_state();
                return Ok(None);
            }
        };

        let mut flashblocks = pending_blocks.get_flashblocks();
        let num_flashblocks_for_canon =
            flashblocks.iter().filter(|fb| fb.metadata.block_number == block.number).count();
        Metrics::flashblocks_in_block().record(num_flashblocks_for_canon as f64);
        Metrics::pending_snapshot_height().set(pending_blocks.latest_block_number() as f64);

        // Check for reorg by comparing transaction sets
        let tracked_txns = pending_blocks.get_transactions_for_block(block.number);
        let tracked_txn_hashes: Vec<_> = tracked_txns.map(|tx| tx.tx_hash()).collect();
        let block_txn_hashes: Vec<_> = block.body().transactions().map(|tx| tx.tx_hash()).collect();

        let reorg_result = ReorgDetector::detect(&tracked_txn_hashes, &block_txn_hashes);
        let reorg_detected = reorg_result.is_reorg();

        // Determine the reconciliation strategy. Reorg detection compares against the block
        // we were notified about, but reconciliation must use the node's real canonical height
        // so a lagging queue cannot hide that pending has drifted away from the tip.
        let canonical_number = self.effective_canonical_number(block.number);
        let strategy = CanonicalBlockReconciler::reconcile(
            Some(pending_blocks.earliest_block_number()),
            Some(pending_blocks.latest_block_number()),
            canonical_number,
            self.max_depth,
            reorg_detected,
        );

        match strategy {
            ReconciliationStrategy::CatchUp => {
                debug!(
                    message = "pending snapshot cleared because canonical caught up",
                    latest_pending_block = pending_blocks.latest_block_number(),
                    notified_block = block.number,
                    canonical_block = canonical_number,
                );
                Metrics::pending_clear_catchup().increment(1);
                Metrics::pending_snapshot_fb_index()
                    .set(pending_blocks.latest_flashblock_index() as f64);
                self.clear_live_state();
                Ok(None)
            }
            ReconciliationStrategy::HandleReorg => {
                warn!(
                    message = "reorg detected, recomputing pending flashblocks going ahead of reorg",
                    tracked_txn_hashes = ?tracked_txn_hashes,
                    block_txn_hashes = ?block_txn_hashes,
                );
                Metrics::pending_clear_reorg().increment(1);

                // Rebuild from the real tip, not the notified height: under queue lag those
                // two differ, and re-executing the already-canonical range cannot publish.
                flashblocks
                    .retain(|flashblock| flashblock.metadata.block_number > canonical_number);
                self.build_pending_state(None, &flashblocks)
            }
            ReconciliationStrategy::DepthLimitExceeded { depth, max_depth } => {
                debug!(
                    message = "pending blocks depth exceeds max depth, resetting pending blocks",
                    pending_blocks_depth = depth,
                    max_depth = max_depth,
                );

                flashblocks
                    .retain(|flashblock| flashblock.metadata.block_number > canonical_number);
                self.build_pending_state(None, &flashblocks)
            }
            ReconciliationStrategy::Continue => {
                debug!(
                    message = "canonical block behind latest pending block, continuing with existing pending state",
                    latest_pending_block = pending_blocks.latest_block_number(),
                    earliest_pending_block = pending_blocks.earliest_block_number(),
                    canonical_block = block.number,
                    pending_txns_for_block = ?tracked_txn_hashes.len(),
                    canonical_txns_for_block = ?block_txn_hashes.len(),
                );
                // If no reorg, we can continue building on top of the existing pending state
                // NOTE: We do not retain specific flashblocks here to avoid losing track of our "earliest" pending block number
                self.build_pending_state(prev_pending_blocks, &flashblocks)
            }
            ReconciliationStrategy::NoPendingState => {
                // This case is already handled above, but included for completeness
                debug!(message = "no pending state to update with canonical block, skipping");
                self.clear_live_state();
                Ok(None)
            }
        }
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            block_number = flashblock.metadata.block_number,
            flashblock_index = flashblock.index
        )
    )]
    fn process_flashblock(
        &self,
        prev_pending_blocks: Option<Arc<PendingBlocks>>,
        flashblock: &Flashblock,
    ) -> Result<Option<Arc<PendingBlocks>>> {
        let pending_blocks = match &prev_pending_blocks {
            Some(pb) => pb,
            None => {
                if flashblock.index == 0 {
                    return self.build_pending_state(None, std::slice::from_ref(flashblock));
                }

                return Err(StateProcessorError::MissingFirstFlashblock);
            }
        };

        let validation_result = FlashblockSequenceValidator::validate(
            pending_blocks.latest_block_number(),
            pending_blocks.latest_flashblock_index(),
            flashblock.metadata.block_number,
            flashblock.index,
            flashblock.metadata.prev_flashblock_id,
        );

        match validation_result {
            SequenceValidationResult::NextInSequence => {
                self.build_pending_state_for_same_block(pending_blocks, flashblock)
            }
            SequenceValidationResult::FirstOfNextBlock => {
                self.build_pending_state_for_next_block(pending_blocks, flashblock)
            }
            SequenceValidationResult::Duplicate => {
                // We have received a duplicate flashblock for the current block
                Metrics::unexpected_block_order().increment(1);
                warn!(
                    message = "Received duplicate Flashblock for current block, ignoring",
                    curr_block = %pending_blocks.latest_block_number(),
                    flashblock_index = %flashblock.index,
                );
                Ok(prev_pending_blocks)
            }
            SequenceValidationResult::InvalidNewBlockIndex { block_number, index: _ } => {
                // We have received a non-zero flashblock for a new block
                Metrics::unexpected_block_order().increment(1);
                error!(
                    message = "Received non-zero index Flashblock for new block, zeroing Flashblocks until we receive a base Flashblock",
                    curr_block = %pending_blocks.latest_block_number(),
                    new_block = %block_number,
                );
                self.clear_live_state();
                Ok(None)
            }
            SequenceValidationResult::NonSequentialGap { expected, actual } => {
                Metrics::unexpected_block_order().increment(1);
                error!(
                    curr_block = %pending_blocks.latest_block_number(),
                    expected_flashblock_index = %expected,
                    actual_flashblock_index = %actual,
                    "received non-sequential flashblock index for current block"
                );
                self.clear_live_state();
                Ok(None)
            }
            SequenceValidationResult::NonSequentialPredecessor { expected, actual } => {
                Metrics::unexpected_block_order().increment(1);
                error!(
                    curr_block = %pending_blocks.latest_block_number(),
                    curr_flashblock_index = %pending_blocks.latest_flashblock_index(),
                    new_block = %flashblock.metadata.block_number,
                    new_flashblock_index = %flashblock.index,
                    expected_prev_block = %expected.block_number,
                    expected_prev_index = %expected.index,
                    actual_prev_block = %actual.block_number,
                    actual_prev_index = %actual.index,
                    "received flashblock with non-sequential predecessor link"
                );
                self.clear_live_state();
                Ok(None)
            }
        }
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            block_number = flashblock.metadata.block_number,
            flashblock_index = flashblock.index
        )
    )]
    fn build_pending_state_for_same_block(
        &self,
        prev_pending_blocks: &Arc<PendingBlocks>,
        flashblock: &Flashblock,
    ) -> Result<Option<Arc<PendingBlocks>>> {
        let latest_block_base = prev_pending_blocks.latest_block_base().clone();
        let latest_block_l1_block_info = prev_pending_blocks.latest_block_l1_block_info().clone();
        let latest_flashblock_tx_start = prev_pending_blocks.pending_transaction_count();

        let mut live_state = self.lock_live_state();
        let Some(LivePendingState { mut db, state_overrides }) = live_state.take() else {
            warn!(
                message = "live pending state unavailable, falling back to full rebuild",
                block_number = flashblock.metadata.block_number,
                flashblock_index = flashblock.index,
                path = "same_block"
            );
            let mut flashblocks = prev_pending_blocks.get_flashblocks();
            flashblocks.push(flashblock.clone());
            return self.build_pending_state(Some(Arc::clone(prev_pending_blocks)), &flashblocks);
        };
        drop(live_state);

        let latest_header = prev_pending_blocks.latest_header();
        let mut latest_block_flashblocks = prev_pending_blocks.latest_block_flashblocks();
        latest_block_flashblocks.push(flashblock.clone());
        let latest_block_header =
            BlockAssembler::refresh_same_block_header(&latest_header, &latest_block_flashblocks)?;

        db.block_hashes.insert(latest_block_base.block_number - 1, latest_block_base.parent_hash);

        let evm_config = BaseEvmConfig::base(self.client.chain_spec());
        let evm_env = evm_config
            .evm_env(&latest_header)
            .map_err(|e| ExecutionError::EvmEnv(e.to_string()))?;
        let evm = evm_config.evm_with_env(db, evm_env);

        let previous_block_transaction_count = prev_pending_blocks.latest_block_transaction_count();
        let pending_block = Block {
            header: Header {
                parent_hash: latest_block_base.parent_hash,
                number: latest_block_base.block_number,
                timestamp: latest_block_base.timestamp,
                gas_limit: latest_block_base.gas_limit,
                base_fee_per_gas: Some(latest_block_base.base_fee_per_gas.saturating_to()),
                ..Default::default()
            },
            body: BlockBody {
                transactions: flashblock
                    .diff
                    .transactions
                    .iter()
                    .map(|tx| {
                        base_common_consensus::decode_2718_canonical::<BaseTxEnvelope>(tx.as_ref())
                    })
                    .collect::<std::result::Result<_, _>>()
                    .map_err(|e| ExecutionError::BlockConversion(e.to_string()))?,
                ..Default::default()
            },
        };
        let latest_block_transaction_count = prev_pending_blocks.latest_block_transaction_count()
            + pending_block.body.transactions.len();
        let recovery_start = Instant::now();
        let txs_with_senders: Vec<(BaseTxEnvelope, Address)> = pending_block
            .body
            .transactions
            .par_iter()
            .cloned()
            .map(|tx| -> Result<(BaseTxEnvelope, Address)> {
                let sender = tx.recover_signer()?;
                Ok((tx, sender))
            })
            .collect::<Result<_>>()?;
        let sender_recovery_elapsed = recovery_start.elapsed();
        Metrics::sender_recovery_duration().record(sender_recovery_elapsed);

        let mut pending_blocks_builder = PendingBlocksBuilder::from_previous(prev_pending_blocks);
        pending_blocks_builder.with_flashblocks([flashblock.clone()]);
        pending_blocks_builder.replace_latest_header(latest_block_header);

        let mut pending_state_builder = PendingStateBuilder::new(
            self.client.chain_spec(),
            evm,
            pending_block,
            None,
            latest_block_l1_block_info.clone(),
            state_overrides,
        );
        pending_state_builder.set_execution_offsets(
            prev_pending_blocks.latest_block_cumulative_gas_used(),
            prev_pending_blocks.latest_block_next_log_index(),
        );

        for (offset, (transaction, sender)) in txs_with_senders.into_iter().enumerate() {
            let tx_hash = transaction.tx_hash();
            let idx = previous_block_transaction_count + offset;

            pending_blocks_builder.with_transaction_sender(tx_hash, sender);
            pending_blocks_builder.increment_nonce(sender);

            let recovered_transaction = Recovered::new_unchecked(transaction, sender);
            let executed_transaction =
                pending_state_builder.execute_transaction(idx, recovered_transaction)?;

            if let Some(time_us) = executed_transaction.execution_time_us {
                pending_blocks_builder.with_execution_time(tx_hash, time_us);
            }

            for (address, account) in &executed_transaction.state {
                if account.is_touched() {
                    pending_blocks_builder.with_account_balance(*address, account.info.balance);
                }
            }

            pending_blocks_builder.with_transaction(executed_transaction.rpc_transaction);
            pending_blocks_builder.with_receipt(tx_hash, executed_transaction.receipt);
            pending_blocks_builder.with_transaction_state(tx_hash, executed_transaction.state);
            pending_blocks_builder.with_transaction_result(tx_hash, executed_transaction.result);
        }

        let latest_block_cumulative_gas_used = pending_state_builder.cumulative_gas_used();
        let latest_block_next_log_index = pending_state_builder.next_log_index();
        let (db, state_overrides) = pending_state_builder.into_db_and_state_overrides();
        pending_blocks_builder.with_latest_block_context(
            latest_flashblock_tx_start,
            latest_block_base,
            latest_block_l1_block_info,
            latest_block_transaction_count,
            latest_block_cumulative_gas_used,
            latest_block_next_log_index,
        );
        self.publish_pending_blocks(pending_blocks_builder, db, state_overrides)
    }

    #[instrument(
        level = "debug",
        skip_all,
        fields(
            block_number = flashblock.metadata.block_number,
            flashblock_index = flashblock.index
        )
    )]
    fn build_pending_state_for_next_block(
        &self,
        prev_pending_blocks: &Arc<PendingBlocks>,
        flashblock: &Flashblock,
    ) -> Result<Option<Arc<PendingBlocks>>> {
        let Some(base) = flashblock.base.clone() else {
            return Err(StateProcessorError::MissingFirstFlashblock);
        };

        // A public partial-block hash is not authority to reuse the final parent's state.
        let previous_header = prev_pending_blocks.latest_header();
        let canonical_parent = self
            .client
            .header_by_number(previous_header.number)
            .map_err(|error| ProviderError::StateProvider(error.to_string()))?
            .ok_or(StateProcessorError::ParentUnverified {
                parent_block: previous_header.number,
            })?;
        let calculated_parent_hash = canonical_parent.hash_slow();
        if calculated_parent_hash != base.parent_hash {
            return Err(StateProcessorError::ParentHashMismatch {
                parent_block: previous_header.number,
                calculated_parent_hash,
                declared_parent_hash: base.parent_hash,
            });
        }
        if !prev_pending_blocks.matches_canonical_parent(&canonical_parent) {
            return Err(StateProcessorError::ParentPrefixMismatch {
                parent_block: previous_header.number,
            });
        }

        let mut live_state = self.lock_live_state();
        let Some(LivePendingState { mut db, state_overrides }) = live_state.take() else {
            warn!(
                message = "live pending state unavailable, falling back to full rebuild",
                block_number = flashblock.metadata.block_number,
                flashblock_index = flashblock.index,
                path = "next_block"
            );
            let mut flashblocks = prev_pending_blocks.get_flashblocks();
            flashblocks.push(flashblock.clone());
            return self.build_pending_state(Some(Arc::clone(prev_pending_blocks)), &flashblocks);
        };
        drop(live_state);

        let current_block = BlockAssembler::assemble(std::slice::from_ref(flashblock))?;
        let l1_block_info = current_block.l1_block_info()?;
        let AssembledBlock { block: assembled_block, header: assembled_header, .. } = current_block;
        let pending_block = Block {
            header: Header {
                parent_hash: base.parent_hash,
                number: base.block_number,
                timestamp: base.timestamp,
                gas_limit: base.gas_limit,
                base_fee_per_gas: Some(base.base_fee_per_gas.saturating_to()),
                ..Default::default()
            },
            body: assembled_block.body,
        };

        db.block_hashes.insert(base.block_number - 1, base.parent_hash);

        let evm_config = BaseEvmConfig::base(self.client.chain_spec());
        let block_env_attributes = BaseNextBlockEnvAttributes {
            timestamp: base.timestamp,
            suggested_fee_recipient: base.fee_recipient,
            prev_randao: base.prev_randao,
            gas_limit: base.gas_limit,
            parent_beacon_block_root: Some(base.parent_beacon_block_root),
            extra_data: base.extra_data.clone(),
        };
        let evm_env = evm_config
            .next_evm_env(&previous_header, &block_env_attributes)
            .map_err(|e| ExecutionError::EvmEnv(e.to_string()))?;
        let evm = evm_config.evm_with_env(db, evm_env);

        let recovery_start = Instant::now();
        let txs_with_senders: Vec<(BaseTxEnvelope, Address)> = pending_block
            .body
            .transactions
            .par_iter()
            .cloned()
            .map(|tx| -> Result<(BaseTxEnvelope, Address)> {
                let sender = tx.recover_signer()?;
                Ok((tx, sender))
            })
            .collect::<Result<_>>()?;
        Metrics::sender_recovery_duration().record(recovery_start.elapsed());

        let mut pending_blocks_builder = PendingBlocksBuilder::from_previous(prev_pending_blocks);
        pending_blocks_builder.with_flashblocks([flashblock.clone()]);
        pending_blocks_builder.with_header(assembled_header);

        let mut pending_state_builder = PendingStateBuilder::new(
            self.client.chain_spec(),
            evm,
            pending_block,
            None,
            l1_block_info.clone(),
            state_overrides,
        );
        pending_state_builder
            .apply_pre_execution_changes(base.parent_hash, Some(base.parent_beacon_block_root))?;

        for (idx, (transaction, sender)) in txs_with_senders.into_iter().enumerate() {
            let tx_hash = transaction.tx_hash();

            pending_blocks_builder.with_transaction_sender(tx_hash, sender);
            pending_blocks_builder.increment_nonce(sender);

            let recovered_transaction = Recovered::new_unchecked(transaction, sender);
            let executed_transaction =
                pending_state_builder.execute_transaction(idx, recovered_transaction)?;

            if let Some(time_us) = executed_transaction.execution_time_us {
                pending_blocks_builder.with_execution_time(tx_hash, time_us);
            }

            for (address, account) in &executed_transaction.state {
                if account.is_touched() {
                    pending_blocks_builder.with_account_balance(*address, account.info.balance);
                }
            }

            pending_blocks_builder.with_transaction(executed_transaction.rpc_transaction);
            pending_blocks_builder.with_receipt(tx_hash, executed_transaction.receipt);
            pending_blocks_builder.with_transaction_state(tx_hash, executed_transaction.state);
            pending_blocks_builder.with_transaction_result(tx_hash, executed_transaction.result);
        }

        let latest_block_cumulative_gas_used = pending_state_builder.cumulative_gas_used();
        let latest_block_next_log_index = pending_state_builder.next_log_index();
        let (db, state_overrides) = pending_state_builder.into_db_and_state_overrides();
        pending_blocks_builder.with_latest_block_context(
            prev_pending_blocks.pending_transaction_count(),
            base,
            l1_block_info,
            flashblock.diff.transactions.len(),
            latest_block_cumulative_gas_used,
            latest_block_next_log_index,
        );

        self.publish_pending_blocks(pending_blocks_builder, db, state_overrides)
    }

    #[instrument(level = "debug", skip_all, fields(num_flashblocks = flashblocks.len()))]
    fn build_pending_state(
        &self,
        prev_pending_blocks: Option<Arc<PendingBlocks>>,
        flashblocks: &[Flashblock],
    ) -> Result<Option<Arc<PendingBlocks>>> {
        // BTreeMap guarantees ascending order of keys while iterating
        let mut flashblocks_per_block = BTreeMap::<BlockNumber, Vec<Flashblock>>::new();
        for flashblock in flashblocks {
            flashblocks_per_block
                .entry(flashblock.metadata.block_number)
                .or_default()
                .push(flashblock.clone());
        }

        let Some((&earliest_block_number, _)) = flashblocks_per_block.first_key_value() else {
            self.clear_live_state();
            return Ok(None);
        };
        let canonical_block =
            earliest_block_number.checked_sub(1).ok_or(ProtocolError::GenesisFlashblock)?;
        let mut last_block_header = self
            .client
            .header_by_number(canonical_block)
            .map_err(|e| ProviderError::StateProvider(e.to_string()))?
            .ok_or(ProviderError::MissingCanonicalHeader { block_number: canonical_block })?;

        let evm_config = BaseEvmConfig::base(self.client.chain_spec());
        let state_provider = self
            .client
            .state_by_block_hash(last_block_header.hash_slow())
            .map_err(|e| ProviderError::StateProvider(e.to_string()))?;
        let state_provider_db = StateProviderDatabase::new(state_provider);
        let mut pending_blocks_builder = PendingBlocksBuilder::new();

        // Track state changes across flashblocks, accumulating bundle state
        // from previous pending blocks if available.
        let mut db = State::builder().with_database(state_provider_db).with_bundle_update().build();

        let mut state_overrides =
            prev_pending_blocks.as_ref().map_or_else(StateOverride::default, |pending_blocks| {
                pending_blocks.get_state_overrides().unwrap_or_default()
            });

        let mut previous_executed_gas = last_block_header.gas_used;
        let mut total_transaction_count = 0usize;
        for (_block_number, flashblocks) in flashblocks_per_block {
            // Use BlockAssembler to reconstruct the block from flashblocks
            let assembled = BlockAssembler::assemble(&flashblocks)?;
            let canonical_parent = if last_block_header.number == canonical_block {
                last_block_header.clone()
            } else {
                self.client
                    .header_by_number(last_block_header.number)
                    .map_err(|error| ProviderError::StateProvider(error.to_string()))?
                    .ok_or(StateProcessorError::ParentUnverified {
                        parent_block: last_block_header.number,
                    })?
            };
            let calculated_parent_hash = canonical_parent.hash_slow();
            if calculated_parent_hash != assembled.base.parent_hash {
                return Err(StateProcessorError::ParentHashMismatch {
                    parent_block: last_block_header.number,
                    calculated_parent_hash,
                    declared_parent_hash: assembled.base.parent_hash,
                });
            }
            if last_block_header.state_root.is_zero() {
                last_block_header.state_root = canonical_parent.state_root;
            }
            if last_block_header != canonical_parent
                || previous_executed_gas != canonical_parent.gas_used
            {
                return Err(StateProcessorError::ParentPrefixMismatch {
                    parent_block: last_block_header.number,
                });
            }
            let latest_flashblock_tx_count =
                flashblocks.last().map(|latest| latest.diff.transactions.len()).unwrap_or_default();
            let latest_block_base = assembled.base.clone();

            pending_blocks_builder.with_flashblocks(assembled.flashblocks.clone());
            pending_blocks_builder.with_header(assembled.header.clone());

            // Extract L1 block info using the AssembledBlock method
            let l1_block_info = assembled.l1_block_info()?;
            let latest_block_l1_block_info = l1_block_info.clone();
            let latest_block_transaction_count = assembled.block.body.transactions.len();

            let block_env_attributes = BaseNextBlockEnvAttributes {
                timestamp: assembled.base.timestamp,
                suggested_fee_recipient: assembled.base.fee_recipient,
                prev_randao: assembled.base.prev_randao,
                gas_limit: assembled.base.gas_limit,
                parent_beacon_block_root: Some(assembled.base.parent_beacon_block_root),
                extra_data: assembled.base.extra_data.clone(),
            };

            db.block_hashes
                .insert(latest_block_base.block_number - 1, latest_block_base.parent_hash);

            let evm_env = evm_config
                .next_evm_env(&last_block_header, &block_env_attributes)
                .map_err(|e| ExecutionError::EvmEnv(e.to_string()))?;
            let evm = evm_config.evm_with_env(db, evm_env);

            // Parallel sender recovery - batch all ECDSA operations upfront
            let recovery_start = Instant::now();
            let txs_with_senders: Vec<(BaseTxEnvelope, Address)> = assembled
                .block
                .body
                .transactions
                .par_iter()
                .cloned()
                .map(|tx| -> Result<(BaseTxEnvelope, Address)> {
                    let tx_hash = tx.tx_hash();
                    let sender = match prev_pending_blocks
                        .as_ref()
                        .and_then(|p| p.get_transaction_sender(&tx_hash))
                    {
                        Some(cached) => cached,
                        None => tx.recover_signer()?,
                    };
                    Ok((tx, sender))
                })
                .collect::<Result<_>>()?;
            Metrics::sender_recovery_duration().record(recovery_start.elapsed());

            // Clone header before moving block to avoid cloning the entire block
            let block_header = assembled.block.header.clone();

            let parent_block_hash = assembled.base.parent_hash;
            let parent_beacon_block_root = Some(assembled.base.parent_beacon_block_root);

            let mut pending_state_builder = PendingStateBuilder::new(
                self.client.chain_spec(),
                evm,
                assembled.block,
                prev_pending_blocks.clone(),
                l1_block_info,
                state_overrides,
            );

            pending_state_builder
                .apply_pre_execution_changes(parent_block_hash, parent_beacon_block_root)?;

            for (idx, (transaction, sender)) in txs_with_senders.into_iter().enumerate() {
                let tx_hash = transaction.tx_hash();

                pending_blocks_builder.with_transaction_sender(tx_hash, sender);
                pending_blocks_builder.increment_nonce(sender);

                let recovered_transaction = Recovered::new_unchecked(transaction, sender);

                let executed_transaction =
                    pending_state_builder.execute_transaction(idx, recovered_transaction)?;

                if let Some(time_us) = executed_transaction.execution_time_us {
                    pending_blocks_builder.with_execution_time(tx_hash, time_us);
                }

                for (address, account) in &executed_transaction.state {
                    if account.is_touched() {
                        pending_blocks_builder.with_account_balance(*address, account.info.balance);
                    }
                }

                pending_blocks_builder.with_transaction(executed_transaction.rpc_transaction);
                pending_blocks_builder.with_receipt(tx_hash, executed_transaction.receipt);
                pending_blocks_builder.with_transaction_state(tx_hash, executed_transaction.state);
                pending_blocks_builder
                    .with_transaction_result(tx_hash, executed_transaction.result);
            }

            let latest_flashblock_tx_start = total_transaction_count
                .saturating_add(latest_block_transaction_count)
                .saturating_sub(latest_flashblock_tx_count);
            previous_executed_gas = pending_state_builder.cumulative_gas_used();
            pending_blocks_builder.with_latest_block_context(
                latest_flashblock_tx_start,
                latest_block_base,
                latest_block_l1_block_info,
                latest_block_transaction_count,
                previous_executed_gas,
                pending_state_builder.next_log_index(),
            );
            total_transaction_count += latest_block_transaction_count;

            (db, state_overrides) = pending_state_builder.into_db_and_state_overrides();
            last_block_header = block_header;
        }

        self.publish_pending_blocks(pending_blocks_builder, db, state_overrides)
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::Sealable;
    use alloy_consensus::{Header, Sealed};
    use alloy_primitives::B256;
    use alloy_rpc_types_engine::PayloadId;
    use base_common_consensus::BasePrimitives;
    use base_common_flashblocks::{
        ExecutionPayloadBaseV1, ExecutionPayloadFlashblockDeltaV1, Metadata,
    };
    use base_execution_chainspec::BaseChainSpec;
    use reth_provider::test_utils::MockEthProvider;
    use rstest::rstest;
    use tokio::sync::{broadcast, mpsc};

    use super::*;

    #[rstest]
    #[case::unavailable(0)]
    #[case::wrong_hash(1)]
    #[case::incomplete_prefix(2)]
    fn next_block_authenticates_parent_before_reusing_state(#[case] failure: u8) {
        let client =
            MockEthProvider::<BasePrimitives>::new().with_chain_spec(BaseChainSpec::mainnet());
        let canonical = Header { number: 1, gas_used: 1, ..Default::default() };
        if failure != 0 {
            client.add_header(canonical.hash_slow(), canonical.clone());
        }
        let mut builder = PendingBlocksBuilder::new();
        builder.with_flashblocks([Flashblock {
            payload_id: PayloadId::default(),
            index: 0,
            base: Some(ExecutionPayloadBaseV1 { block_number: 1, ..Default::default() }),
            diff: ExecutionPayloadFlashblockDeltaV1::default(),
            metadata: Metadata::new(1),
        }]);
        builder.with_header(canonical.clone().seal_slow());
        // Executed gas remains zero, so the apparently matching header is insufficient.
        let pending = Arc::new(builder.build().unwrap());
        let (sender, receiver) = mpsc::unbounded_channel();
        drop(sender);
        let processor = StateProcessor::new(
            client,
            Arc::new(ArcSwapOption::empty()),
            3,
            Arc::new(Mutex::new(receiver)),
            broadcast::channel(1).0,
            broadcast::channel(16).0,
        );
        let flashblock = Flashblock {
            payload_id: PayloadId::default(),
            index: 0,
            base: Some(ExecutionPayloadBaseV1 {
                block_number: 2,
                parent_hash: if failure == 1 { B256::ZERO } else { canonical.hash_slow() },
                ..Default::default()
            }),
            diff: ExecutionPayloadFlashblockDeltaV1::default(),
            metadata: Metadata::new(2),
        };
        let result = processor.build_pending_state_for_next_block(&pending, &flashblock);
        match failure {
            0 => assert!(matches!(result, Err(StateProcessorError::ParentUnverified { .. }))),
            1 => assert!(matches!(result, Err(StateProcessorError::ParentHashMismatch { .. }))),
            2 => assert!(matches!(result, Err(StateProcessorError::ParentPrefixMismatch { .. }))),
            _ => unreachable!(),
        }
    }

    #[test]
    fn recovery_rejects_wrong_canonical_parent() {
        let client =
            MockEthProvider::<BasePrimitives>::new().with_chain_spec(BaseChainSpec::mainnet());
        let canonical = Header::default();
        client.add_header(canonical.hash_slow(), canonical);
        let (sender, receiver) = mpsc::unbounded_channel();
        drop(sender);
        let processor = StateProcessor::new(
            client,
            Arc::new(ArcSwapOption::empty()),
            3,
            Arc::new(Mutex::new(receiver)),
            broadcast::channel(1).0,
            broadcast::channel(16).0,
        );
        let flashblock = Flashblock {
            payload_id: PayloadId::default(),
            index: 0,
            base: Some(ExecutionPayloadBaseV1 { block_number: 1, ..Default::default() }),
            diff: ExecutionPayloadFlashblockDeltaV1::default(),
            metadata: Metadata::new(1),
        };
        let result = processor.build_pending_state(None, &[flashblock]);
        assert!(
            matches!(result, Err(StateProcessorError::ParentHashMismatch { .. })),
            "{result:?}"
        );
    }

    #[rstest]
    #[case::caught_up(1)]
    #[case::empty_after_depth_filter(3)]
    #[tokio::test]
    async fn canonical_update_clears_exhausted_pending(#[case] latest_header: u64) {
        let client =
            MockEthProvider::<BasePrimitives>::new().with_chain_spec(BaseChainSpec::mainnet());
        // Keep provider tip at genesis so this queued canonical notification exercises
        // reconciliation rather than the independent stale-snapshot eviction guard.
        client.add_header(B256::ZERO, Header::default());
        let flashblock = Flashblock {
            payload_id: PayloadId::default(),
            index: 0,
            base: Some(ExecutionPayloadBaseV1 { block_number: 1, ..Default::default() }),
            diff: ExecutionPayloadFlashblockDeltaV1::default(),
            metadata: Metadata::new(1),
        };
        let mut builder = PendingBlocksBuilder::new();
        builder.with_flashblocks([flashblock]);
        builder.with_header(Sealed::new_unchecked(
            Header { number: 1, ..Default::default() },
            B256::ZERO,
        ));
        builder.with_header(Sealed::new_unchecked(
            Header { number: latest_header, ..Default::default() },
            B256::ZERO,
        ));
        // The depth case deliberately seeds inconsistent header/flashblock heights.
        // Consistent snapshots with no remaining flashblocks select CatchUp first.
        let pending = Arc::new(ArcSwapOption::from(Some(Arc::new(builder.build().unwrap()))));
        let (tx, rx) = mpsc::unbounded_channel();
        let (sender, _) = broadcast::channel(1);
        let processor = StateProcessor::new(
            client,
            Arc::clone(&pending),
            0,
            Arc::new(Mutex::new(rx)),
            sender,
            broadcast::channel(16).0,
        );
        tx.send(StateUpdate::Canonical(RecoveredBlock::new_unhashed(
            Block {
                header: Header { number: 2, ..Default::default() },
                body: BlockBody::default(),
            },
            Vec::new(),
        )))
        .unwrap();
        drop(tx);
        processor.start().await;
        assert!(pending.load_full().is_none());
    }

    #[tokio::test]
    async fn genesis_update_without_known_canonical_tip_does_not_panic() {
        // With no headers, the provider reports an unknown tip, so the normal
        // superseded-payload filter cannot reject block zero before rebuilding.
        let client =
            MockEthProvider::<BasePrimitives>::new().with_chain_spec(BaseChainSpec::mainnet());
        let pending = Arc::new(ArcSwapOption::empty());
        let (tx, rx) = mpsc::unbounded_channel();
        let (sender, _) = broadcast::channel(1);
        let processor = StateProcessor::new(
            client,
            Arc::clone(&pending),
            3,
            Arc::new(Mutex::new(rx)),
            sender,
            broadcast::channel(16).0,
        );
        tx.send(StateUpdate::Flashblock(Flashblock {
            payload_id: PayloadId::default(),
            index: 0,
            base: Some(ExecutionPayloadBaseV1::default()),
            diff: ExecutionPayloadFlashblockDeltaV1::default(),
            metadata: Metadata::new(0),
        }))
        .unwrap();
        drop(tx);
        processor.start().await;
        assert!(pending.load_full().is_none());
    }
}
