//! RPC trait definitions and implementations for flashblocks.

use std::{sync::Arc, time::Duration};

use alloy_eips::{BlockId, BlockNumberOrTag};

/// A [`BlockNumberOrTag`] wrapper that also accepts `"unsafe"` as an alias for `"latest"`.
///
/// Op-conductor v0.9.2 calls `eth_getBlockByNumber("unsafe")` to retrieve the execution-layer
/// unsafe head before starting a sequencer. Our EL exposes this state as `"latest"` (the most
/// recently sealed block), not as `"unsafe"`. This type handles the deserialization so that
/// `"unsafe"` is transparently remapped to `"latest"` before reaching the block lookup logic.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(transparent)]
pub struct BlockNumberOrTagExt(BlockNumberOrTag);

impl BlockNumberOrTagExt {
    const fn is_pending(&self) -> bool {
        self.0.is_pending()
    }
}

impl From<BlockNumberOrTagExt> for BlockId {
    fn from(tag: BlockNumberOrTagExt) -> Self {
        tag.0.into()
    }
}

impl<'de> serde::Deserialize<'de> for BlockNumberOrTagExt {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl serde::de::Visitor<'_> for Visitor {
            type Value = BlockNumberOrTagExt;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a block number or tag")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Self::Value, E> {
                // Remap the Base "unsafe" tag to "latest". Our EL surfaces the unsafe
                // head as "latest" (the most recently sealed block via engine_forkchoiceUpdated).
                if v == "unsafe" {
                    return Ok(BlockNumberOrTagExt(BlockNumberOrTag::Latest));
                }
                v.parse::<BlockNumberOrTag>().map(BlockNumberOrTagExt).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}
use alloy_primitives::{
    Address, TxHash, U256,
    map::foldhash::{HashSet, HashSetExt},
};
use alloy_rpc_types::{
    BlockOverrides,
    simulate::{SimulatePayload, SimulatedBlock},
    state::{EvmOverrides, StateOverride},
};
use alloy_rpc_types_eth::{Filter, Log};
use base_common_network::Base;
use base_common_rpc_types::BaseTransactionRequest;
use jsonrpsee::{
    core::{RpcResult, async_trait},
    proc_macros::rpc,
};
use jsonrpsee_types::{ErrorObjectOwned, error::INVALID_PARAMS_CODE};
use reth_provider::CanonStateSubscriptions;
use reth_rpc::eth::EthFilter;
use reth_rpc_eth_api::{
    EthApiTypes, EthFilterApiServer, RpcBlock, RpcReceipt, RpcTransaction,
    helpers::{EthBlocks, EthCall, EthState, EthTransactions, FullEthApi},
};
use reth_rpc_eth_types::EthApiError;
use tokio::{sync::broadcast::error::RecvError, time};
use tokio_stream::{StreamExt, wrappers::BroadcastStream};
use tracing::{debug, trace, warn};

use crate::PendingBlocks;
use crate::{FlashblocksAPI, PendingBlocksAPI, metrics::Metrics};

/// Max configured timeout for `eth_sendRawTransactionSync` in milliseconds.
const MAX_TIMEOUT_SEND_RAW_TX_SYNC_MS: u64 = 6_000;

/// Eth API override trait for flashblocks integration.
#[cfg_attr(not(test), rpc(server, namespace = "eth"))]
#[cfg_attr(test, rpc(server, client, namespace = "eth"))]
pub trait EthApiOverride {
    /// Returns block by number, with flashblock support for pending blocks.
    #[method(name = "getBlockByNumber")]
    async fn block_by_number(
        &self,
        number: BlockNumberOrTagExt,
        full: bool,
    ) -> RpcResult<Option<RpcBlock<Base>>>;

    /// Returns transaction receipt, checking flashblocks first.
    #[method(name = "getTransactionReceipt")]
    async fn get_transaction_receipt(&self, tx_hash: TxHash)
    -> RpcResult<Option<RpcReceipt<Base>>>;

    /// Returns account balance, with flashblock support for pending state.
    #[method(name = "getBalance")]
    async fn get_balance(&self, address: Address, block_number: Option<BlockId>)
    -> RpcResult<U256>;

    /// Returns transaction count for an address.
    #[method(name = "getTransactionCount")]
    async fn get_transaction_count(
        &self,
        address: Address,
        block_number: Option<BlockId>,
    ) -> RpcResult<U256>;

    /// Returns transaction by hash, checking flashblocks first.
    #[method(name = "getTransactionByHash")]
    async fn transaction_by_hash(&self, tx_hash: TxHash)
    -> RpcResult<Option<RpcTransaction<Base>>>;

    /// Sends a raw transaction and waits for inclusion in a flashblock.
    #[method(name = "sendRawTransactionSync")]
    async fn send_raw_transaction_sync(
        &self,
        transaction: alloy_primitives::Bytes,
        timeout_ms: Option<u64>,
    ) -> RpcResult<RpcReceipt<Base>>;

    /// Executes a call with flashblock state support.
    #[method(name = "call")]
    async fn call(
        &self,
        transaction: BaseTransactionRequest,
        block_number: Option<BlockId>,
        state_overrides: Option<StateOverride>,
        block_overrides: Option<Box<BlockOverrides>>,
    ) -> RpcResult<alloy_primitives::Bytes>;

    /// Estimates gas with flashblock state support.
    #[method(name = "estimateGas")]
    async fn estimate_gas(
        &self,
        transaction: BaseTransactionRequest,
        block_number: Option<BlockId>,
        overrides: Option<StateOverride>,
    ) -> RpcResult<U256>;

    /// Simulates transactions with flashblock state support.
    #[method(name = "simulateV1")]
    async fn simulate_v1(
        &self,
        opts: SimulatePayload<BaseTransactionRequest>,
        block_number: Option<BlockId>,
    ) -> RpcResult<Vec<SimulatedBlock<RpcBlock<Base>>>>;

    /// Returns logs matching the filter, including pending flashblock logs.
    #[method(name = "getLogs")]
    async fn get_logs(&self, filter: Filter) -> RpcResult<Vec<Log>>;

    /// Returns the number of transactions in a block by block number.
    #[method(name = "getBlockTransactionCountByNumber")]
    async fn get_block_transaction_count_by_number(
        &self,
        number: BlockNumberOrTag,
    ) -> RpcResult<Option<U256>>;
}

/// Extended Eth API with flashblocks support.
#[derive(Debug)]
pub struct EthApiExt<Eth: EthApiTypes, FB> {
    eth_api: Eth,
    eth_filter: EthFilter<Eth>,
    flashblocks_state: Arc<FB>,
}

impl<Eth: EthApiTypes, FB> EthApiExt<Eth, FB> {
    /// Creates a new extended Eth API instance with flashblocks support.
    pub const fn new(eth_api: Eth, eth_filter: EthFilter<Eth>, flashblocks_state: Arc<FB>) -> Self {
        Self { eth_api, eth_filter, flashblocks_state }
    }
}

#[async_trait]
impl<Eth, FB> EthApiOverrideServer for EthApiExt<Eth, FB>
where
    Eth: FullEthApi<NetworkTypes = Base> + Send + Sync + 'static,
    FB: FlashblocksAPI + Send + Sync + 'static,
    jsonrpsee_types::error::ErrorObject<'static>: From<Eth::Error>,
{
    async fn block_by_number(
        &self,
        number: BlockNumberOrTagExt,
        full: bool,
    ) -> RpcResult<Option<RpcBlock<Base>>> {
        debug!(
            message = "rpc::block_by_number",
            block_number = ?number
        );

        if number.is_pending() {
            Metrics::rpc_get_block_by_number().increment(1);
            let pending_blocks = self.flashblocks_state.get_pending_blocks();
            if pending_blocks.as_ref().is_some() {
                return Ok(pending_blocks.get_block(full));
            }
            // No pending state available — treat `pending` as `latest`
            EthBlocks::rpc_block(&self.eth_api, BlockNumberOrTag::Latest.into(), full)
                .await
                .map_err(Into::into)
        } else {
            EthBlocks::rpc_block(&self.eth_api, number.into(), full).await.map_err(Into::into)
        }
    }

    async fn get_transaction_receipt(
        &self,
        tx_hash: TxHash,
    ) -> RpcResult<Option<RpcReceipt<Base>>> {
        debug!(
            message = "rpc::get_transaction_receipt",
            tx_hash = %tx_hash
        );

        // Check canonical chain first to avoid race condition where flashblocks
        // state hasn't been cleared yet after canonical block commit
        if let Some(canonical_receipt) =
            EthTransactions::transaction_receipt(&self.eth_api, tx_hash).await?
        {
            return Ok(Some(canonical_receipt));
        }

        // Fall back to flashblocks for pending transactions
        let pending_blocks = self.flashblocks_state.get_pending_blocks();
        if let Some(fb_receipt) = pending_blocks.get_transaction_receipt(tx_hash) {
            Metrics::rpc_get_transaction_receipt().increment(1);
            return Ok(Some(fb_receipt));
        }

        Ok(None)
    }

    async fn get_balance(
        &self,
        address: Address,
        block_number: Option<BlockId>,
    ) -> RpcResult<U256> {
        debug!(
            message = "rpc::get_balance",
            address = %address
        );
        let block_id = block_number.unwrap_or_default();
        if block_id.is_pending() {
            Metrics::rpc_get_balance().increment(1);
            let pending_blocks = self.flashblocks_state.get_pending_blocks();
            if let Some(balance) = pending_blocks.get_balance(address) {
                return Ok(balance);
            }
        }

        EthState::balance(&self.eth_api, address, block_number).await.map_err(Into::into)
    }

    async fn get_transaction_count(
        &self,
        address: Address,
        block_number: Option<BlockId>,
    ) -> RpcResult<U256> {
        debug!(
            message = "rpc::get_transaction_count",
            address = %address,
        );

        let block_id = block_number.unwrap_or_default();
        if block_id.is_pending() {
            Metrics::rpc_get_transaction_count().increment(1);
            let pending_blocks = self.flashblocks_state.get_pending_blocks();
            let canon_block = pending_blocks.get_canonical_block_number();
            let fb_count = pending_blocks.get_transaction_count(address);

            let canon_count =
                EthState::transaction_count(&self.eth_api, address, Some(canon_block.into()))
                    .await
                    .map_err(Into::into)?;

            return Ok(canon_count + fb_count);
        }

        EthState::transaction_count(&self.eth_api, address, block_number).await.map_err(Into::into)
    }

    async fn transaction_by_hash(
        &self,
        tx_hash: TxHash,
    ) -> RpcResult<Option<RpcTransaction<Base>>> {
        debug!(
            message = "rpc::transaction_by_hash",
            tx_hash = %tx_hash
        );

        // Check canonical chain first to avoid race condition where flashblocks
        // state hasn't been cleared yet after canonical block commit
        if let Some(canonical_tx) = EthTransactions::transaction_by_hash(&self.eth_api, tx_hash)
            .await?
            .map(|tx| tx.into_transaction(self.eth_api.converter()))
            .transpose()
            .map_err(Eth::Error::from)?
        {
            return Ok(Some(canonical_tx));
        }

        // Fall back to flashblocks for pending transactions
        let pending_blocks = self.flashblocks_state.get_pending_blocks();
        if let Some(fb_transaction) = pending_blocks.get_transaction_by_hash(tx_hash) {
            Metrics::rpc_get_transaction_by_hash().increment(1);
            return Ok(Some(fb_transaction));
        }

        Ok(None)
    }

    async fn send_raw_transaction_sync(
        &self,
        transaction: alloy_primitives::Bytes,
        timeout_ms: Option<u64>,
    ) -> RpcResult<RpcReceipt<Base>> {
        debug!(message = "rpc::send_raw_transaction_sync");

        let timeout_ms = match timeout_ms {
            Some(ms) if ms > MAX_TIMEOUT_SEND_RAW_TX_SYNC_MS => {
                return Err(ErrorObjectOwned::owned(
                    INVALID_PARAMS_CODE,
                    format!(
                        "time out too long, timeout: {ms} ms, max: {MAX_TIMEOUT_SEND_RAW_TX_SYNC_MS} ms"
                    ),
                    None::<()>,
                ));
            }
            Some(ms) => ms,
            _ => MAX_TIMEOUT_SEND_RAW_TX_SYNC_MS,
        };

        let tx_hash = match EthTransactions::send_raw_transaction(&self.eth_api, transaction).await
        {
            Ok(hash) => hash,
            Err(e) => return Err(e.into()),
        };

        debug!(
            message = "rpc::send_raw_transaction_sync::sent_transaction",
            tx_hash = %tx_hash,
            timeout_ms = timeout_ms,
        );

        let timeout = Duration::from_millis(timeout_ms);
        tokio::select! {
            receipt = self.wait_for_flashblocks_receipt(tx_hash) => {
                receipt.ok_or_else(|| EthApiError::TransactionConfirmationTimeout {
                    hash: tx_hash,
                    duration: timeout,
                }.into())
            }
            receipt = self.wait_for_canonical_receipt(tx_hash) => {
                receipt.ok_or_else(|| EthApiError::TransactionConfirmationTimeout {
                    hash: tx_hash,
                    duration: timeout,
                }.into())
            }
            _ = time::sleep(timeout) => {
                Err(EthApiError::TransactionConfirmationTimeout {
                    hash: tx_hash,
                    duration: timeout,
                }.into())
            }
        }
    }

    async fn call(
        &self,
        transaction: BaseTransactionRequest,
        block_number: Option<BlockId>,
        state_overrides: Option<StateOverride>,
        block_overrides: Option<Box<BlockOverrides>>,
    ) -> RpcResult<alloy_primitives::Bytes> {
        debug!(
            message = "rpc::call",
            transaction = ?transaction,
            block_number = ?block_number,
            state_overrides = ?state_overrides,
            block_overrides = ?block_overrides,
        );

        let block_id = block_number.unwrap_or_default();
        if block_id.is_pending() {
            Metrics::rpc_call().increment(1);
        }
        let (block_id, pending_overrides) = self.pending_execution_overrides(block_id).await?;
        let overrides = merge_overrides(pending_overrides, state_overrides, block_overrides);
        EthCall::call(&self.eth_api, transaction, Some(block_id), overrides)
            .await
            .map_err(Into::into)
    }

    async fn estimate_gas(
        &self,
        transaction: BaseTransactionRequest,
        block_number: Option<BlockId>,
        overrides: Option<StateOverride>,
    ) -> RpcResult<U256> {
        debug!(message = "rpc::estimate_gas", transaction = ?transaction, block_number = ?block_number, overrides = ?overrides);
        let block_id = block_number.unwrap_or_default();
        if block_id.is_pending() {
            Metrics::rpc_estimate_gas().increment(1);
        }
        let (block_id, pending_overrides) = self.pending_execution_overrides(block_id).await?;
        let overrides = merge_overrides(pending_overrides, overrides, None);
        EthCall::estimate_gas_at(&self.eth_api, transaction, block_id, overrides)
            .await
            .map_err(Into::into)
    }

    async fn simulate_v1(
        &self,
        mut opts: SimulatePayload<BaseTransactionRequest>,
        block_number: Option<BlockId>,
    ) -> RpcResult<Vec<SimulatedBlock<RpcBlock<Eth::NetworkTypes>>>> {
        debug!(message = "rpc::simulate_v1", block_number = ?block_number);
        let block_id = block_number.unwrap_or_default();
        if block_id.is_pending() {
            Metrics::rpc_simulate_v1().increment(1);
        }
        let (block_id, pending_overrides) = self.pending_execution_overrides(block_id).await?;
        apply_pending_simulation_overrides(&mut opts, pending_overrides);
        EthCall::simulate_v1(&self.eth_api, opts, Some(block_id)).await.map_err(Into::into)
    }

    async fn get_logs(&self, filter: Filter) -> RpcResult<Vec<Log>> {
        debug!(
            message = "rpc::get_logs",
            address = ?filter.address
        );

        // Check if this is a mixed query (toBlock is pending)
        let (from_block, to_block) = match &filter.block_option {
            alloy_rpc_types_eth::FilterBlockOption::Range { from_block, to_block } => {
                (*from_block, *to_block)
            }
            _ => {
                // Block hash queries or other formats - delegate to eth API
                return self.eth_filter.logs(filter).await;
            }
        };

        // If toBlock is not pending, delegate to eth API
        if !matches!(to_block, Some(BlockNumberOrTag::Pending)) {
            return self.eth_filter.logs(filter).await;
        }

        // Mixed query: toBlock is pending, so we need to combine historical + pending logs
        Metrics::rpc_get_logs().increment(1);
        let mut all_logs = Vec::new();

        let pending_blocks = self.flashblocks_state.get_pending_blocks();

        let mut fetched_logs = HashSet::new();
        // Get historical logs if fromBlock is not pending
        if !matches!(from_block, Some(BlockNumberOrTag::Pending)) {
            // Create a filter for historical data (fromBlock to latest)
            let mut historical_filter = filter.clone();
            historical_filter.block_option = alloy_rpc_types_eth::FilterBlockOption::Range {
                from_block,
                to_block: Some(BlockNumberOrTag::Latest),
            };

            let historical_logs: Vec<Log> = self.eth_filter.logs(historical_filter).await?;
            for log in &historical_logs {
                fetched_logs.insert((log.block_number, log.log_index));
            }
            all_logs.extend(historical_logs);
        }

        // Always get pending logs when toBlock is pending
        let pending_logs = pending_blocks.get_pending_logs(&filter);

        // Dedup any logs from the pending state that may already have been covered in the historical logs
        let deduped_pending_logs: Vec<Log> = pending_logs
            .iter()
            .filter(|log| !fetched_logs.contains(&(log.block_number, log.log_index)))
            .cloned()
            .collect();
        all_logs.extend(deduped_pending_logs);

        Ok(all_logs)
    }

    async fn get_block_transaction_count_by_number(
        &self,
        number: BlockNumberOrTag,
    ) -> RpcResult<Option<U256>> {
        debug!(
            message = "rpc::get_block_transaction_count_by_number",
            block_number = ?number
        );

        if number.is_pending() {
            Metrics::rpc_get_block_transaction_count_by_number().increment(1);
            let pending_blocks = self.flashblocks_state.get_pending_blocks();
            if let Some(block) = pending_blocks.get_block(false) {
                let count = block.transactions.len();
                return Ok(Some(U256::from(count)));
            }
            // No pending state available — treat `pending` as `latest`
            return EthBlocks::block_transaction_count(
                &self.eth_api,
                BlockNumberOrTag::Latest.into(),
            )
            .await
            .map(|opt| opt.map(U256::from))
            .map_err(Into::into);
        }

        EthBlocks::block_transaction_count(&self.eth_api, number.into())
            .await
            .map(|opt| opt.map(U256::from))
            .map_err(Into::into)
    }
}

impl<Eth, FB> EthApiExt<Eth, FB>
where
    Eth: FullEthApi<NetworkTypes = Base> + Send + Sync + 'static,
    FB: FlashblocksAPI + Send + Sync + 'static,
    jsonrpsee_types::error::ErrorObject<'static>: From<Eth::Error>,
{
    async fn pending_execution_overrides(
        &self,
        block_id: BlockId,
    ) -> RpcResult<(BlockId, EvmOverrides)> {
        if !block_id.is_pending() {
            return Ok((block_id, EvmOverrides::default()));
        }
        let permit = self.eth_api.acquire_owned_blocking_io().await.map_err(|error| {
            ErrorObjectOwned::owned(
                -32603,
                format!("failed to acquire pending state preparation permit: {error}"),
                None::<()>,
            )
        })?;
        // Capture one immutable snapshot after admission. Clone its potentially large
        // account/storage map on the bounded blocking pool, never a Tokio worker.
        let pending = self.flashblocks_state.get_pending_blocks().as_ref().map(Arc::clone);
        self.eth_api
            .spawn_blocking_io(move |_| {
                let overrides = pending.as_deref().map_or_else(
                    || (BlockNumberOrTag::Latest.into(), EvmOverrides::default()),
                    pending_snapshot_overrides,
                );
                drop(permit);
                Ok(overrides)
            })
            .await
            .map_err(Into::into)
    }

    async fn wait_for_flashblocks_receipt(&self, tx_hash: TxHash) -> Option<RpcReceipt<Base>> {
        let mut receiver = self.flashblocks_state.subscribe_to_flashblocks();

        loop {
            match receiver.recv().await {
                Ok(pending_state) if pending_state.get_receipt(tx_hash).is_some() => {
                    debug!(message = "found receipt in flashblock", tx_hash = %tx_hash);
                    return pending_state.get_receipt(tx_hash).cloned();
                }
                Ok(_) => {
                    trace!(message = "flashblock does not contain receipt", tx_hash = %tx_hash);
                }
                Err(RecvError::Closed) => {
                    debug!(message = "flashblocks receipt queue closed");
                    return None;
                }
                Err(RecvError::Lagged(_)) => {
                    warn!("Flashblocks receipt queue lagged, maybe missing receipts");
                }
            }
        }
    }

    async fn wait_for_canonical_receipt(&self, tx_hash: TxHash) -> Option<RpcReceipt<Base>> {
        let mut stream =
            BroadcastStream::new(self.eth_api.provider().subscribe_to_canonical_state());

        while let Some(Ok(canon_state)) = stream.next().await {
            for (block_receipt, _) in canon_state.block_receipts() {
                for (canonical_tx_hash, _) in &block_receipt.tx_receipts {
                    if *canonical_tx_hash == tx_hash {
                        debug!(
                            message = "found receipt in canonical state",
                            tx_hash = %tx_hash
                        );
                        return EthTransactions::transaction_receipt(&self.eth_api, tx_hash)
                            .await
                            .ok()
                            .flatten();
                    }
                }
            }
        }
        None
    }
}

fn pending_snapshot_overrides(pending: &PendingBlocks) -> (BlockId, EvmOverrides) {
    let header = pending.latest_header();
    let block = BlockOverrides {
        number: Some(U256::from(header.number)),
        difficulty: Some(header.difficulty),
        time: Some(header.timestamp),
        gas_limit: Some(header.gas_limit),
        coinbase: Some(header.beneficiary),
        random: Some(header.mix_hash),
        base_fee: header.base_fee_per_gas.map(U256::from),
        beacon_root: header.parent_beacon_block_root,
        ..Default::default()
    };
    (
        pending.canonical_block_number().into(),
        EvmOverrides::new(pending.get_state_overrides(), Some(Box::new(block))),
    )
}

fn merge_overrides(
    mut pending: EvmOverrides,
    state: Option<StateOverride>,
    block: Option<Box<BlockOverrides>>,
) -> EvmOverrides {
    if let Some(state) = state {
        let accounts = pending.state.get_or_insert_with(StateOverride::default);
        for (address, user) in state {
            let account = accounts.entry(address).or_default();
            account.balance = user.balance.or(account.balance);
            account.nonce = user.nonce.or(account.nonce);
            account.code = user.code.or_else(|| account.code.take());
            account.move_precompile_to = user.move_precompile_to.or(account.move_precompile_to);
            if user.state.is_some() {
                account.state = user.state;
                // Preserve an invalid user state+stateDiff combination for normal RPC validation.
                account.state_diff = user.state_diff;
            } else if let Some(diff) = user.state_diff {
                if let Some(storage) = account.state.as_mut() {
                    storage.extend(diff);
                } else {
                    account.state_diff.get_or_insert_with(Default::default).extend(diff);
                }
            }
        }
    }
    if let Some(user) = block {
        let block = pending.block.get_or_insert_with(Default::default);
        block.number = user.number.or(block.number);
        block.difficulty = user.difficulty.or(block.difficulty);
        block.time = user.time.or(block.time);
        block.gas_limit = user.gas_limit.or(block.gas_limit);
        block.coinbase = user.coinbase.or(block.coinbase);
        block.random = user.random.or(block.random);
        block.base_fee = user.base_fee.or(block.base_fee);
        block.blob_base_fee = user.blob_base_fee.or(block.blob_base_fee);
        block.beacon_root = user.beacon_root.or(block.beacon_root);
        if let Some(hashes) = user.block_hash {
            block.block_hash.get_or_insert_with(Default::default).extend(hashes);
        }
    }
    pending
}

fn apply_pending_simulation_overrides(
    opts: &mut SimulatePayload<BaseTransactionRequest>,
    pending: EvmOverrides,
) {
    // Pending state is the starting state, not a reset before each simulated block.
    if let Some(first) = opts.block_state_calls.first_mut() {
        let merged = merge_overrides(
            pending,
            first.state_overrides.take(),
            first.block_overrides.take().map(Box::new),
        );
        first.state_overrides = merged.state;
        first.block_overrides = merged.block.map(|block| *block);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PendingBlocksBuilder;
    use alloy_consensus::Header;
    use alloy_consensus::Sealed;
    use alloy_evm::overrides::apply_block_overrides;
    use alloy_primitives::B256;
    use alloy_primitives::Bytes;
    use alloy_primitives::TxKind;
    use alloy_rpc_types::simulate::SimBlock;
    use alloy_rpc_types::state::AccountOverride;
    use base_common_flashblocks::ExecutionPayloadBaseV1;
    use base_common_flashblocks::Flashblock;
    use base_common_flashblocks::Metadata;
    use revm::Context;
    use revm::ExecuteEvm;
    use revm::MainBuilder;
    use revm::MainContext;
    use revm::bytecode::Bytecode;
    use revm::context::BlockEnv;
    use revm::context::TxEnv;
    use revm::primitives::hardfork::SpecId;
    use revm::state::AccountInfo;
    use revm_database::CacheDB;
    use revm_database::EmptyDB;

    #[test]
    fn pending_call_opcodes_use_the_same_snapshot_header_as_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let pending = pending_snapshot(51_044_736, 1_788_878_819)?;
        let (state_block, overrides) = pending_snapshot_overrides(&pending);
        assert_eq!(state_block, BlockId::from(BlockNumberOrTag::Number(51_044_735)));
        let block = overrides.block.ok_or("pending block environment is missing")?;
        let mut database = CacheDB::<EmptyDB>::default();
        let address = Address::repeat_byte(0x55);
        let caller = Address::repeat_byte(0x66);
        let code = Bytecode::new_legacy(Bytes::from_static(&[
            0x43, 0x60, 0x00, 0x52, 0x42, 0x60, 0x20, 0x52, 0x45, 0x60, 0x40, 0x52, 0x41, 0x60,
            0x60, 0x52, 0x48, 0x60, 0x80, 0x52, 0x44, 0x60, 0xa0, 0x52, 0x60, 0xc0, 0x60, 0x00,
            0xf3,
        ]));
        database
            .insert_account_info(address, AccountInfo { code: Some(code), ..Default::default() });
        database.insert_account_info(
            caller,
            AccountInfo {
                balance: U256::from(1_000_000_000_000_000_000_u64),
                ..Default::default()
            },
        );
        let mut environment = BlockEnv {
            number: U256::from(51_044_735),
            timestamp: U256::from(1_788_878_817),
            ..Default::default()
        };
        apply_block_overrides(*block, &mut database, &mut environment);
        let mut evm = Context::mainnet()
            .modify_cfg_chained(|cfg| cfg.set_spec_and_mainnet_gas_params(SpecId::CANCUN))
            .with_block(environment)
            .with_db(database)
            .build_mainnet();
        let result = evm.transact(
            TxEnv::builder()
                .caller(caller)
                .kind(TxKind::Call(address))
                .gas_limit(100_000)
                .gas_price(1000)
                .build()?,
        )?;
        let output = result.result.output().ok_or("pending opcode call did not succeed")?;
        let words: Vec<U256> =
            output.as_chunks::<32>().0.iter().map(|word| U256::from_be_bytes(*word)).collect();
        assert_eq!(
            words,
            vec![
                U256::from(51_044_736),
                U256::from(1_788_878_819),
                U256::from(30_000_000),
                U256::from_be_slice(Address::repeat_byte(0x11).as_slice()),
                U256::from(1000),
                U256::from_be_slice(B256::repeat_byte(0x22).as_slice()),
            ]
        );
        assert_eq!(
            overrides
                .state
                .and_then(|state| state.get(&address).and_then(|account| account.balance)),
            Some(U256::from(51_044_736))
        );
        Ok(())
    }

    #[test]
    fn user_overrides_replace_only_explicit_pending_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let pending = pending_snapshot(100, 200)?;
        let (_, overrides) = pending_snapshot_overrides(&pending);
        let address = Address::repeat_byte(0x55);
        let user_state = StateOverride::from_iter([(
            address,
            AccountOverride {
                nonce: Some(99),
                state_diff: Some([(B256::ZERO, B256::repeat_byte(0x99))].into_iter().collect()),
                ..Default::default()
            },
        )]);
        let merged = merge_overrides(
            overrides,
            Some(user_state),
            Some(Box::new(BlockOverrides {
                number: Some(U256::from(123)),
                time: Some(456),
                ..Default::default()
            })),
        );
        let block = merged.block.ok_or("merged block override missing")?;
        assert_eq!(block.number, Some(U256::from(123)));
        assert_eq!(block.time, Some(456));
        assert_eq!(block.base_fee, Some(U256::from(1000)));
        assert_eq!(block.gas_limit, Some(30_000_000));
        let state = merged.state.ok_or("merged state override missing")?;
        let account = state.get(&address).ok_or("merged account missing")?;
        assert_eq!(account.balance, Some(U256::from(100)));
        assert_eq!(account.nonce, Some(99));
        let storage = account.state_diff.as_ref().ok_or("merged storage missing")?;
        assert_eq!(storage.get(&B256::ZERO), Some(&B256::repeat_byte(0x99)));
        assert_eq!(storage.get(&B256::repeat_byte(1)), Some(&B256::repeat_byte(2)));
        Ok(())
    }

    #[test]
    fn full_user_storage_replaces_pending_storage_before_later_diffs()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_, pending) = pending_snapshot_overrides(&pending_snapshot(100, 200)?);
        let address = Address::repeat_byte(0x55);
        let user = AccountOverride {
            state: Some([(B256::ZERO, B256::repeat_byte(5))].into_iter().collect()),
            ..Default::default()
        };
        let full =
            merge_overrides(pending, Some(StateOverride::from_iter([(address, user)])), None);
        let user = AccountOverride {
            state_diff: Some([(B256::ZERO, B256::repeat_byte(6))].into_iter().collect()),
            ..Default::default()
        };
        let merged = merge_overrides(full, Some(StateOverride::from_iter([(address, user)])), None);
        let accounts = merged.state.ok_or("storage override missing")?;
        let account = accounts.get(&address).ok_or("storage account missing")?;
        let storage = account.state.as_ref().ok_or("full storage override missing")?;
        assert_eq!(storage.len(), 1);
        assert_eq!(storage.get(&B256::ZERO), Some(&B256::repeat_byte(6)));
        assert!(account.state_diff.is_none());
        assert_eq!(account.nonce, Some(3));
        Ok(())
    }

    #[test]
    fn simulation_applies_pending_state_once_and_preserves_later_blocks()
    -> Result<(), Box<dyn std::error::Error>> {
        let (_, pending) = pending_snapshot_overrides(&pending_snapshot(100, 200)?);
        let user_block =
            BlockOverrides { number: Some(U256::from(105)), time: Some(210), ..Default::default() };
        let second = SimBlock::<BaseTransactionRequest> {
            block_overrides: Some(user_block),
            ..Default::default()
        };
        let mut payload = SimulatePayload {
            block_state_calls: vec![SimBlock::default(), second.clone()],
            ..Default::default()
        };
        apply_pending_simulation_overrides(&mut payload, pending);
        let first = payload.block_state_calls.first().ok_or("first simulated block missing")?;
        assert!(first.state_overrides.is_some());
        assert_eq!(
            first.block_overrides.as_ref().and_then(|block| block.number),
            Some(U256::from(100))
        );
        let retained = payload.block_state_calls.get(1).ok_or("second simulated block missing")?;
        assert_eq!(retained.block_overrides, second.block_overrides);
        assert!(retained.state_overrides.is_none());
        Ok(())
    }

    #[test]
    fn captured_pending_state_and_header_remain_coherent_after_publication()
    -> Result<(), Box<dyn std::error::Error>> {
        let current = arc_swap::ArcSwapOption::from(Some(Arc::new(pending_snapshot(100, 200)?)));
        let captured = current.load_full().ok_or("captured pending snapshot missing")?;
        current.store(Some(Arc::new(pending_snapshot(101, 202)?)));
        let (state_block, overrides) = pending_snapshot_overrides(&captured);
        assert_eq!(state_block, BlockId::from(BlockNumberOrTag::Number(99)));
        assert_eq!(overrides.block.as_ref().and_then(|block| block.number), Some(U256::from(100)));
        assert_eq!(overrides.block.as_ref().and_then(|block| block.time), Some(200));
        assert_eq!(
            overrides.state.and_then(|state| state
                .get(&Address::repeat_byte(0x55))
                .and_then(|account| account.balance)),
            Some(U256::from(100))
        );
        Ok(())
    }

    fn pending_snapshot(
        number: u64,
        timestamp: u64,
    ) -> Result<PendingBlocks, crate::StateProcessorError> {
        let header = Header {
            number,
            timestamp,
            gas_limit: 30_000_000,
            beneficiary: Address::repeat_byte(0x11),
            mix_hash: B256::repeat_byte(0x22),
            base_fee_per_gas: Some(1000),
            ..Default::default()
        };
        let mut builder = PendingBlocksBuilder::default();
        builder.with_header(Sealed::new(header));
        builder.with_flashblocks([Flashblock {
            payload_id: Default::default(),
            index: 0,
            base: Some(ExecutionPayloadBaseV1 {
                block_number: number,
                timestamp,
                ..Default::default()
            }),
            diff: Default::default(),
            metadata: Metadata::new(number),
        }]);
        builder.with_state_overrides(StateOverride::from_iter([(
            Address::repeat_byte(0x55),
            AccountOverride {
                balance: Some(U256::from(number)),
                nonce: Some(3),
                state_diff: Some(
                    [
                        (B256::ZERO, B256::repeat_byte(3)),
                        (B256::repeat_byte(1), B256::repeat_byte(2)),
                    ]
                    .into_iter()
                    .collect(),
                ),
                ..Default::default()
            },
        )]));
        builder.build()
    }
}
