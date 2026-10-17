use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use alloy_primitives::B256;
use async_trait::async_trait;
use base_common_genesis::RollupConfig;
use base_common_rpc_types_engine::BaseExecutionPayloadEnvelope;
use base_consensus_engine::{
    EngineClient, EngineState, EngineSyncStateUpdate, EngineTask, EngineTaskExt, InsertTask,
    SynchronizeTask,
};
use base_protocol::L2BlockInfo;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::follow::error::FollowError;

/// Outcome of [`FollowEngine::replace_unsafe_chain`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChainReplace {
    /// The unsafe head is now the last payload.
    Applied,
    /// The engine has not reached the parent height of the first payload yet; nothing was sent.
    EngineBehind {
        /// Current unsafe head number.
        unsafe_head: u64,
    },
    /// The first payload is at or below the safe head; nothing was sent.
    BelowSafe {
        /// Current safe head number.
        safe_head: u64,
    },
    /// The replacement did not finish within the time limit (engine lock or engine calls).
    TimedOut,
    /// The engine's chain does not hold the expected parent, or the unsafe head is not the last
    /// payload after the inserts.
    Unexpected {
        /// Current unsafe head number.
        number: u64,
        /// Current unsafe head hash.
        hash: B256,
    },
}

#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub(super) trait FollowEngine: Debug + Send + Sync {
    async fn insert_payload(
        &self,
        envelope: BaseExecutionPayloadEnvelope,
    ) -> Result<(), FollowError>;

    /// Replaces the unsafe chain with `payloads` (consecutive blocks, oldest first) in one engine
    /// critical section: the first is inserted authoritatively (forced forkchoice update, may
    /// reorg the current unsafe chain), the rest as ordinary unsafe payloads.
    ///
    /// Nothing is sent unless the engine has already processed the first payload's parent
    /// height (so the execution layer always has that parent and never needs to sync) and the
    /// first payload is above the safe head. Each insert is tried once (no in-place retry) and
    /// the whole replacement is time-limited; any failure is returned for the caller to retry.
    async fn replace_unsafe_chain(
        &self,
        payloads: Vec<BaseExecutionPayloadEnvelope>,
    ) -> Result<ChainReplace, FollowError>;

    async fn update_safe_finalized_blocks(
        &self,
        safe: Option<L2BlockInfo>,
        finalized: Option<L2BlockInfo>,
    ) -> Result<(), FollowError>;
}

/// Upper bound on one [`FollowEngine::replace_unsafe_chain`] call, lock wait included.
const REPLACE_TIMEOUT: Duration = Duration::from_secs(10);

/// Consecutive non-extending payloads the strict insert path skips before it fails. More than
/// the prefetch queue can hold, so only a follower that can no longer extend its head (e.g. the
/// EL keeps answering SYNCING) exits for a restart from the local head.
const MAX_STRICT_SKIPS: usize = crate::follow::prefetcher::PREFETCH_WINDOW + 1;

#[derive(Debug)]
pub(super) struct EngineApiFollowEngine<E: EngineClient> {
    client: Arc<E>,
    rollup_config: Arc<RollupConfig>,
    state: Mutex<EngineState>,
    replace_timeout: Duration,
    /// Only insert payloads that extend the unsafe head (P2P primary).
    strict: bool,
    /// Consecutive payloads skipped by the strict check.
    strict_skips: AtomicUsize,
}

impl<E: EngineClient> EngineApiFollowEngine<E> {
    pub(super) fn new(
        client: Arc<E>,
        rollup_config: Arc<RollupConfig>,
        latest: L2BlockInfo,
        safe: L2BlockInfo,
        finalized: L2BlockInfo,
    ) -> Self {
        let mut state = EngineState::default();
        state.sync_state = state.sync_state.apply_update(EngineSyncStateUpdate {
            unsafe_head: Some(latest),
            local_safe_head: Some(safe),
            safe_head: Some(safe),
            finalized_head: Some(finalized),
        });
        Self {
            client,
            rollup_config,
            state: Mutex::new(state),
            replace_timeout: REPLACE_TIMEOUT,
            strict: false,
            strict_skips: AtomicUsize::new(0),
        }
    }

    /// Inserts only payloads that extend the unsafe head (number = head + 1, parent = head hash)
    /// and skips any other. For P2P primary: after a partial unsafe chain repair, queued gossip
    /// blocks of the dropped fork must never reach the EL with an unknown parent (it would accept
    /// them as SYNCING and sync to them). Without it (off, shadow) a block past a gap is still sent
    /// and the EL syncs past it, the existing recovery.
    pub(super) const fn with_strict_extension(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// Overrides the replacement time limit.
    #[cfg(test)]
    pub(super) const fn with_replace_timeout(mut self, limit: Duration) -> Self {
        self.replace_timeout = limit;
        self
    }

    async fn replace_locked(
        &self,
        payloads: Vec<BaseExecutionPayloadEnvelope>,
    ) -> Result<ChainReplace, FollowError> {
        let Some(first) = payloads.first() else {
            return Ok(ChainReplace::Applied);
        };
        let first_number = first.execution_payload.block_number();
        let first_parent = first.execution_payload.parent_hash();
        let last_hash = payloads[payloads.len() - 1].execution_payload.block_hash();
        // Held across every insert: the follow insert loop cannot interleave a queued block.
        let mut state = self.state.lock().await;
        let safe_head = state.sync_state.safe_head().block_info.number;
        if first_number <= safe_head {
            return Ok(ChainReplace::BelowSafe { safe_head });
        }
        let head = state.sync_state.unsafe_head().block_info;
        if head.number.saturating_add(1) < first_number {
            return Ok(ChainReplace::EngineBehind { unsafe_head: head.number });
        }
        if head.number.saturating_add(1) == first_number && head.hash != first_parent {
            return Ok(ChainReplace::Unexpected { number: head.number, hash: head.hash });
        }
        for (i, envelope) in payloads.into_iter().enumerate() {
            let client = Arc::clone(&self.client);
            let rollup_config = Arc::clone(&self.rollup_config);
            let task = if i == 0 {
                InsertTask::authoritative_payload(client, rollup_config, envelope)
            } else {
                InsertTask::unsafe_payload(client, rollup_config, envelope)
            };
            // One attempt: a SYNCING or failing engine must not spin here holding the lock.
            task.execute(&mut state).await.map_err(FollowError::engine_task)?;
        }
        let head = state.sync_state.unsafe_head().block_info;
        if head.hash != last_hash {
            return Ok(ChainReplace::Unexpected { number: head.number, hash: head.hash });
        }
        Ok(ChainReplace::Applied)
    }

    /// The engine's current unsafe head.
    #[cfg(test)]
    pub(super) async fn unsafe_head(&self) -> L2BlockInfo {
        self.state.lock().await.sync_state.unsafe_head()
    }
}

#[async_trait]
impl<E: EngineClient + Debug + 'static> FollowEngine for EngineApiFollowEngine<E> {
    async fn insert_payload(
        &self,
        envelope: BaseExecutionPayloadEnvelope,
    ) -> Result<(), FollowError> {
        let mut state = self.state.lock().await;
        if self.strict {
            let head = state.sync_state.unsafe_head().block_info;
            let number = envelope.execution_payload.block_number();
            let parent = envelope.execution_payload.parent_hash();
            let next = head.number.saturating_add(1);
            if number > next || (number == next && parent != head.hash) {
                let skips = self.strict_skips.fetch_add(1, Ordering::Relaxed) + 1;
                if skips > MAX_STRICT_SKIPS {
                    return Err(FollowError::UnsafeHeadNotExtended {
                        number,
                        head: head.number,
                        skipped: skips,
                    });
                }
                if skips == 1 || skips.is_multiple_of(10) {
                    warn!(target: "follow", block = number, %parent, head = head.number,
                        head_hash = %head.hash, skipped = skips,
                        "Skipping payload that does not extend the unsafe head");
                } else {
                    debug!(target: "follow", block = number, head = head.number,
                        "Skipping payload that does not extend the unsafe head");
                }
                return Ok(());
            }
            if number == next {
                self.strict_skips.store(0, Ordering::Relaxed);
            }
        }
        let task = InsertTask::unsafe_payload(
            Arc::clone(&self.client),
            Arc::clone(&self.rollup_config),
            envelope,
        );
        EngineTask::Insert(Box::new(task))
            .execute(&mut state)
            .await
            .map_err(FollowError::engine_task)
    }

    async fn replace_unsafe_chain(
        &self,
        payloads: Vec<BaseExecutionPayloadEnvelope>,
    ) -> Result<ChainReplace, FollowError> {
        tokio::time::timeout(self.replace_timeout, self.replace_locked(payloads))
            .await
            .unwrap_or(Ok(ChainReplace::TimedOut))
    }

    async fn update_safe_finalized_blocks(
        &self,
        safe: Option<L2BlockInfo>,
        finalized: Option<L2BlockInfo>,
    ) -> Result<(), FollowError> {
        if safe.is_none() && finalized.is_none() {
            return Ok(());
        }

        let task = SynchronizeTask::new(
            Arc::clone(&self.client),
            Arc::clone(&self.rollup_config),
            EngineSyncStateUpdate {
                local_safe_head: safe,
                safe_head: safe,
                finalized_head: finalized,
                ..Default::default()
            },
        );
        task.execute(&mut *self.state.lock().await).await.map_err(FollowError::engine_task)
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use alloy_eips::eip2718::Encodable2718;
    use alloy_primitives::{Address, B256, Bloom, U256};
    use alloy_rpc_types_engine::{
        ExecutionPayloadV1, ForkchoiceUpdated, PayloadStatus, PayloadStatusEnum,
    };
    use base_common_consensus::{BaseBlock, BaseTxEnvelope, TxDeposit};
    use base_common_genesis::RollupConfig;
    use base_common_rpc_types_engine::{BaseExecutionPayload, BaseExecutionPayloadEnvelope};
    use base_consensus_engine::test_utils::{MockEngineClient, test_engine_client_builder};
    use base_protocol::{BlockInfo, L1BlockInfoBedrock, L2BlockInfo};
    use tokio::time::{self, Instant};

    use super::{ChainReplace, EngineApiFollowEngine, FollowEngine, MAX_STRICT_SKIPS};
    use crate::follow::error::FollowError;

    fn valid_payload_status() -> PayloadStatus {
        PayloadStatus { status: PayloadStatusEnum::Valid, latest_valid_hash: Some(B256::ZERO) }
    }

    fn valid_forkchoice_updated() -> ForkchoiceUpdated {
        ForkchoiceUpdated { payload_status: valid_payload_status(), payload_id: None }
    }

    fn l1_info_deposit_tx() -> Vec<u8> {
        BaseTxEnvelope::from(TxDeposit {
            input: L1BlockInfoBedrock::default().encode_calldata(),
            ..Default::default()
        })
        .encoded_2718()
    }

    fn l2_block_info(number: u64) -> L2BlockInfo {
        L2BlockInfo {
            block_info: BlockInfo {
                hash: B256::with_last_byte(number as u8),
                number,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn payload(number: u64) -> BaseExecutionPayloadEnvelope {
        BaseExecutionPayloadEnvelope {
            parent_beacon_block_root: None,
            execution_payload: BaseExecutionPayload::V1(ExecutionPayloadV1 {
                parent_hash: B256::with_last_byte(number.saturating_sub(1) as u8),
                fee_recipient: Address::ZERO,
                state_root: B256::ZERO,
                receipts_root: B256::ZERO,
                logs_bloom: Bloom::ZERO,
                prev_randao: B256::ZERO,
                block_number: number,
                gas_limit: 30_000_000,
                gas_used: 0,
                timestamp: 1,
                extra_data: Default::default(),
                base_fee_per_gas: U256::ZERO,
                block_hash: B256::with_last_byte(number as u8),
                transactions: vec![l1_info_deposit_tx().into()],
            }),
        }
    }

    #[tokio::test]
    async fn insert_payload_retries_temporary_engine_errors() {
        let rollup_config = Arc::new(RollupConfig::default());
        let client = Arc::new(
            test_engine_client_builder()
                .with_config(Arc::clone(&rollup_config))
                .with_fork_choice_updated_v3_response(valid_forkchoice_updated())
                .build(),
        );
        let genesis = l2_block_info(0);
        let engine = Arc::new(EngineApiFollowEngine::new(
            Arc::clone(&client),
            rollup_config,
            genesis,
            genesis,
            genesis,
        ));

        let insert_engine = Arc::clone(&engine);
        let insert = tokio::spawn(async move { insert_engine.insert_payload(payload(1)).await });

        let deadline = Instant::now() + Duration::from_secs(1);
        while client.last_new_payload_v2().await.is_none() && Instant::now() < deadline {
            time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            client.last_new_payload_v2().await.is_some(),
            "follow insert should attempt engine_newPayload before retrying"
        );

        client.set_new_payload_v2_response(valid_payload_status()).await;

        time::timeout(Duration::from_secs(1), insert)
            .await
            .expect("insert should finish after temporary error clears")
            .expect("insert task should not panic")
            .expect("temporary engine error should be retried");
    }

    #[tokio::test]
    async fn replace_unsafe_chain_sends_nothing_ahead_of_or_off_the_engine_chain() {
        let rollup_config = Arc::new(RollupConfig::default());
        let client = Arc::new(
            test_engine_client_builder()
                .with_config(Arc::clone(&rollup_config))
                .with_new_payload_v2_response(valid_payload_status())
                .with_fork_choice_updated_v3_response(valid_forkchoice_updated())
                .build(),
        );
        let genesis = l2_block_info(0);
        let engine = EngineApiFollowEngine::new(
            Arc::clone(&client),
            rollup_config,
            genesis,
            genesis,
            genesis,
        );

        // Parent height 1 is not in the engine yet.
        assert_eq!(
            engine.replace_unsafe_chain(vec![payload(2)]).await.unwrap(),
            ChainReplace::EngineBehind { unsafe_head: 0 }
        );
        // Parent height is the head, but the head is a different block.
        let mut orphan = payload(1);
        if let BaseExecutionPayload::V1(inner) = &mut orphan.execution_payload {
            inner.parent_hash = B256::repeat_byte(0xee);
        }
        assert_eq!(
            engine.replace_unsafe_chain(vec![orphan]).await.unwrap(),
            ChainReplace::Unexpected { number: 0, hash: genesis.block_info.hash }
        );
        assert!(client.last_new_payload_v2().await.is_none());
        assert_eq!(engine.unsafe_head().await, genesis);
    }

    fn engine_client(
        rollup_config: &Arc<RollupConfig>,
        new_payload: Option<PayloadStatus>,
        forkchoice: ForkchoiceUpdated,
    ) -> Arc<MockEngineClient> {
        let mut builder = test_engine_client_builder()
            .with_config(Arc::clone(rollup_config))
            .with_fork_choice_updated_v3_response(forkchoice);
        if let Some(status) = new_payload {
            builder = builder.with_new_payload_v2_response(status);
        }
        Arc::new(builder.build())
    }

    async fn forkchoice_calls(client: &MockEngineClient) -> usize {
        client.storage().read().await.fork_choice_updated_v3_requests.len()
    }

    #[tokio::test]
    async fn replace_unsafe_chain_never_touches_the_safe_chain() {
        let rollup_config = Arc::new(RollupConfig::default());
        let client =
            engine_client(&rollup_config, Some(valid_payload_status()), valid_forkchoice_updated());
        let engine = EngineApiFollowEngine::new(
            Arc::clone(&client),
            rollup_config,
            l2_block_info(3),
            l2_block_info(2),
            l2_block_info(0),
        );
        assert_eq!(
            engine.replace_unsafe_chain(vec![payload(2)]).await.unwrap(),
            ChainReplace::BelowSafe { safe_head: 2 }
        );
        assert!(client.last_new_payload_v2().await.is_none());
        assert_eq!(forkchoice_calls(&client).await, 0);
    }

    #[tokio::test]
    async fn replace_unsafe_chain_tries_a_syncing_engine_once() {
        let rollup_config = Arc::new(RollupConfig::default());
        let syncing = ForkchoiceUpdated {
            payload_status: PayloadStatus {
                status: PayloadStatusEnum::Syncing,
                latest_valid_hash: None,
            },
            payload_id: None,
        };
        let client = engine_client(&rollup_config, Some(valid_payload_status()), syncing);
        let genesis = l2_block_info(0);
        let engine = EngineApiFollowEngine::new(
            Arc::clone(&client),
            rollup_config,
            genesis,
            genesis,
            genesis,
        );

        let started = Instant::now();
        let result = engine.replace_unsafe_chain(vec![payload(1)]).await;
        assert!(result.is_err(), "a forced insert the EL only syncs is an error: {result:?}");
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(forkchoice_calls(&client).await, 1, "no in-place retry");
        assert_eq!(engine.unsafe_head().await, genesis, "lock released, head unchanged");

        client.set_fork_choice_updated_v3_response(valid_forkchoice_updated()).await;
        // With its true header hash, so the head check after the inserts matches.
        let mut block1 = payload(1);
        if let BaseExecutionPayload::V1(inner) = &mut block1.execution_payload {
            let block: BaseBlock =
                BaseExecutionPayload::V1(inner.clone()).try_into_block().expect("valid fixture");
            inner.block_hash = block.header.hash_slow();
        }
        assert_eq!(
            engine.replace_unsafe_chain(vec![block1]).await.unwrap(),
            ChainReplace::Applied,
            "the next fetch retries"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn replace_unsafe_chain_is_time_limited_while_the_insert_loop_holds_the_engine() {
        let rollup_config = Arc::new(RollupConfig::default());
        // No newPayload response: the insert loop's insert retries (holding the engine lock).
        let client = engine_client(&rollup_config, None, valid_forkchoice_updated());
        let genesis = l2_block_info(0);
        let limit = Duration::from_millis(200);
        let engine = Arc::new(
            EngineApiFollowEngine::new(
                Arc::clone(&client),
                rollup_config,
                genesis,
                genesis,
                genesis,
            )
            .with_replace_timeout(limit),
        );
        let insert_engine = Arc::clone(&engine);
        let insert = tokio::spawn(async move { insert_engine.insert_payload(payload(1)).await });
        while client.last_new_payload_v2().await.is_none() {
            time::sleep(Duration::from_millis(5)).await;
        }

        let started = Instant::now();
        assert_eq!(
            engine.replace_unsafe_chain(vec![payload(1)]).await.unwrap(),
            ChainReplace::TimedOut
        );
        assert!(started.elapsed() < limit * 5);

        // No deadlock: the insert loop finishes once the engine answers.
        client.set_new_payload_v2_response(valid_payload_status()).await;
        time::timeout(Duration::from_secs(2), insert).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn strict_extension_skips_gaps_and_only_strict_mode_does() {
        let rollup_config = Arc::new(RollupConfig::default());
        let genesis = l2_block_info(0);
        // Block 2 on top of the genesis head: a gap (its parent 1 is unknown to the EL).
        for strict in [false, true] {
            let client = engine_client(
                &rollup_config,
                Some(valid_payload_status()),
                valid_forkchoice_updated(),
            );
            let engine = EngineApiFollowEngine::new(
                Arc::clone(&client),
                Arc::clone(&rollup_config),
                genesis,
                genesis,
                genesis,
            )
            .with_strict_extension(strict);
            engine.insert_payload(payload(2)).await.unwrap();
            assert_eq!(
                client.last_new_payload_v2().await.is_some(),
                !strict,
                "off/shadow still send the block past a gap (EL sync recovery); primary skips it"
            );
        }
    }

    #[tokio::test]
    async fn strict_extension_fails_after_more_skips_than_the_queue_holds() {
        let rollup_config = Arc::new(RollupConfig::default());
        let client =
            engine_client(&rollup_config, Some(valid_payload_status()), valid_forkchoice_updated());
        let genesis = l2_block_info(0);
        let engine = EngineApiFollowEngine::new(
            Arc::clone(&client),
            rollup_config,
            genesis,
            genesis,
            genesis,
        )
        .with_strict_extension(true);
        for n in 2..2 + MAX_STRICT_SKIPS as u64 {
            engine.insert_payload(payload(n)).await.unwrap();
        }
        let err = engine.insert_payload(payload(100)).await.unwrap_err();
        assert!(matches!(err, FollowError::UnsafeHeadNotExtended { number: 100, .. }), "{err:?}");
        assert!(client.last_new_payload_v2().await.is_none());
    }
}
