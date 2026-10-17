use std::{fmt::Debug, sync::Arc};

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
    /// height, so the execution layer always has that parent and never needs to sync.
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

#[derive(Debug)]
pub(super) struct EngineApiFollowEngine<E: EngineClient> {
    client: Arc<E>,
    rollup_config: Arc<RollupConfig>,
    state: Mutex<EngineState>,
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
        Self { client, rollup_config, state: Mutex::new(state) }
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
        let task = InsertTask::unsafe_payload(
            Arc::clone(&self.client),
            Arc::clone(&self.rollup_config),
            envelope,
        );
        EngineTask::Insert(Box::new(task))
            .execute(&mut *self.state.lock().await)
            .await
            .map_err(FollowError::engine_task)
    }

    async fn replace_unsafe_chain(
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
            EngineTask::Insert(Box::new(task))
                .execute(&mut state)
                .await
                .map_err(FollowError::engine_task)?;
        }
        let head = state.sync_state.unsafe_head().block_info;
        if head.hash != last_hash {
            return Ok(ChainReplace::Unexpected { number: head.number, hash: head.hash });
        }
        Ok(ChainReplace::Applied)
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
    use base_common_consensus::{BaseTxEnvelope, TxDeposit};
    use base_common_genesis::RollupConfig;
    use base_common_rpc_types_engine::{BaseExecutionPayload, BaseExecutionPayloadEnvelope};
    use base_consensus_engine::test_utils::test_engine_client_builder;
    use base_protocol::{BlockInfo, L1BlockInfoBedrock, L2BlockInfo};
    use tokio::time::{self, Instant};

    use super::{ChainReplace, EngineApiFollowEngine, FollowEngine};

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
}
