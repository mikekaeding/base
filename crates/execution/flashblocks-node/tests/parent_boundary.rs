//! Full-node acceptance tests for authenticated pending-parent reuse and safe fallback.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use alloy_eips::Decodable2718;
use alloy_eips::Encodable2718;
use alloy_network::ReceiptResponse;
use alloy_primitives::B256;
use alloy_primitives::U256;
use base_common_consensus::BaseBlock;
use base_common_consensus::BaseTransactionSigned;
use base_common_flashblocks::Flashblock;
use base_flashblocks::FlashblocksAPI;
use base_flashblocks::FlashblocksReceiver;
use base_flashblocks::FlashblocksState;
use base_flashblocks::PendingBlocks;
use base_flashblocks_node::test_harness::FlashblockBuilder;
use base_flashblocks_node::test_harness::FlashblocksBuilderTestHarness;
use base_test_utils::Account;
use eyre::Context;
use eyre::Result;
use eyre::eyre;
use reth_primitives_traits::RecoveredBlock;
use tokio::time::timeout;

/// One actual Engine-API-built parent, deliberately not yet delivered to the pending processor.
#[derive(Debug)]
pub struct ParentBoundary {
    /// Isolated node, account state and manually controlled pending processor.
    pub harness: FlashblocksBuilderTestHarness,
    /// Independently executed canonical parent, visible to the provider.
    pub canonical: RecoveredBlock<BaseBlock>,
    /// Full parent payload carrying a deliberately different provisional block hash.
    pub parent: Flashblock,
    /// First child payload, anchored to the actual canonical hash.
    pub child: Flashblock,
}

impl ParentBoundary {
    /// Builds equivalent canonical/pending inputs with the requested number of synthetic transfers.
    /// Canonical delivery to the pending processor remains under the test's control.
    pub async fn new(transfers: u64) -> Result<Self> {
        let mut harness = FlashblocksBuilderTestHarness::new().await;
        let mut parent = FlashblockBuilder::new_base(&harness).build();
        let deposit =
            parent.diff.transactions.first().ok_or_else(|| eyre!("parent deposit missing"))?;
        let deposit = BaseTransactionSigned::decode_2718(&mut deposit.as_ref())
            .wrap_err("decode parent L1 attributes deposit")?;
        let mut transactions = vec![deposit];
        for nonce in 0..transfers {
            transactions.push(harness.build_transaction_to_send_eth_with_nonce(
                Account::Alice,
                Account::Bob,
                100_000,
                nonce,
            ));
        }
        let canonical = harness.new_canonical_block_without_processing(transactions).await;
        let header = canonical.header();
        let base = parent.base.as_mut().ok_or_else(|| eyre!("parent base payload missing"))?;
        base.parent_hash = header.parent_hash;
        base.fee_recipient = header.beneficiary;
        base.prev_randao = header.mix_hash;
        base.block_number = header.number;
        base.gas_limit = header.gas_limit;
        base.timestamp = header.timestamp;
        base.extra_data = header.extra_data.clone();
        base.base_fee_per_gas = U256::from(
            header.base_fee_per_gas.ok_or_else(|| eyre!("canonical parent base fee missing"))?,
        );
        base.parent_beacon_block_root = header
            .parent_beacon_block_root
            .ok_or_else(|| eyre!("canonical parent beacon root missing"))?;
        parent.diff.transactions = canonical
            .body()
            .transactions()
            .map(|transaction| transaction.encoded_2718().into())
            .collect();
        parent.diff.receipts_root = header.receipts_root;
        parent.diff.logs_bloom = header.logs_bloom;
        parent.diff.gas_used = header.gas_used;
        parent.diff.withdrawals_root = header
            .withdrawals_root
            .ok_or_else(|| eyre!("canonical parent withdrawals root missing"))?;
        parent.diff.blob_gas_used = header.blob_gas_used;
        parent.diff.block_hash = B256::with_last_byte(42);
        parent.diff.state_root = B256::ZERO;
        let child = FlashblockBuilder::new_base(&harness).build();
        Ok(Self { harness, canonical, parent, child })
    }

    /// Measures actual receive-to-publication latency, excluding harness sleeps and node startup.
    pub async fn publish(
        state: &FlashblocksState,
        payload: Flashblock,
        label: &str,
    ) -> Result<Arc<PendingBlocks>> {
        let mut published = state.subscribe_to_flashblocks();
        let started = Instant::now();
        state.on_flashblock_received(payload);
        let pending = timeout(Duration::from_secs(2), published.recv())
            .await
            .wrap_err("pending publication deadline expired")?
            .wrap_err("pending publication channel closed")?;
        eprintln!("parent_boundary label={label} publication_us={}", started.elapsed().as_micros());
        Ok(pending)
    }
}

#[tokio::test]
async fn complete_parent_reuses_state_despite_different_provisional_hash() -> Result<()> {
    let scenario = ParentBoundary::new(1).await?;
    let state = &scenario.harness.flashblocks;
    let mut resets = state.subscribe_to_resets();
    assert_ne!(scenario.parent.diff.block_hash, scenario.canonical.hash());
    let parent = ParentBoundary::publish(state, scenario.parent, "parent").await?;
    assert!(parent.matches_canonical_parent(scenario.canonical.header()));

    let child = ParentBoundary::publish(state, scenario.child, "authenticated_child").await?;
    assert_eq!(child.latest_block_number(), 2);
    assert_eq!(
        child.earliest_block_number(),
        1,
        "must reuse pending state before canonical notice"
    );
    assert_eq!(child.latest_block_transaction_count(), 1);
    let transaction = scenario.harness.build_transaction_to_send_eth_with_nonce(
        Account::Alice,
        Account::Bob,
        200_000,
        1,
    );
    let transaction_hash = *transaction.hash();
    let transfer =
        FlashblockBuilder::new(&scenario.harness, 1).with_transactions(vec![transaction]).build();
    let appended = ParentBoundary::publish(state, transfer, "same_block_append").await?;
    assert_eq!(appended.latest_block_transaction_count(), 2);
    assert_eq!(
        appended.get_balance(Account::Bob.address()),
        Some(scenario.harness.canonical_balance(Account::Bob) + U256::from(200_000))
    );
    let receipt = appended
        .get_receipt(transaction_hash)
        .ok_or_else(|| eyre!("child transfer receipt missing"))?;
    assert!(receipt.status());
    assert_eq!(receipt.gas_used(), 21_000);
    let overrides = appended.get_state_overrides().ok_or_else(|| eyre!("child state missing"))?;
    assert_eq!(overrides.get(&Account::Alice.address()).and_then(|account| account.nonce), Some(2));
    assert!(resets.try_recv().is_err(), "a complete parent must not reset consumers");
    Ok(())
}

#[tokio::test]
async fn larger_complete_parents_preserve_child_state_and_receipts() -> Result<()> {
    // Transaction-count scaling only: these transfers are not representative DEX execution load.
    for transfers in [64, 256, 512] {
        let scenario = ParentBoundary::new(transfers).await?;
        let state = &scenario.harness.flashblocks;
        let mut resets = state.subscribe_to_resets();
        let parent = ParentBoundary::publish(state, scenario.parent, "load_parent").await?;
        assert!(parent.matches_canonical_parent(scenario.canonical.header()));
        let child = ParentBoundary::publish(state, scenario.child, "load_child").await?;
        assert_eq!(child.earliest_block_number(), 1);
        let transaction = scenario.harness.build_transaction_to_send_eth_with_nonce(
            Account::Alice,
            Account::Bob,
            200_000,
            transfers,
        );
        let transaction_hash = *transaction.hash();
        let payload = FlashblockBuilder::new(&scenario.harness, 1)
            .with_transactions(vec![transaction])
            .build();
        let appended = ParentBoundary::publish(state, payload, "load_append").await?;
        assert_eq!(
            appended.get_balance(Account::Bob.address()),
            Some(scenario.harness.canonical_balance(Account::Bob) + U256::from(200_000))
        );
        let receipt = appended
            .get_receipt(transaction_hash)
            .ok_or_else(|| eyre!("load child transfer receipt missing"))?;
        assert!(receipt.status());
        assert_eq!(receipt.gas_used(), 21_000);
        assert!(resets.try_recv().is_err());
        eprintln!("parent_boundary completed_transfer_count={transfers}");
    }
    Ok(())
}

#[tokio::test]
async fn conflicting_parent_environment_waits_then_recovers_child() -> Result<()> {
    let mut scenario = ParentBoundary::new(1).await?;
    scenario.parent.base.as_mut().ok_or_else(|| eyre!("parent base missing"))?.prev_randao =
        B256::with_last_byte(99);
    let state = &scenario.harness.flashblocks;
    let parent = ParentBoundary::publish(state, scenario.parent, "conflicting_parent").await?;
    assert!(!parent.matches_canonical_parent(scenario.canonical.header()));
    let mut published = state.subscribe_to_flashblocks();
    let mut resets = state.subscribe_to_resets();
    state.on_flashblock_received(scenario.child);
    assert!(timeout(Duration::from_millis(25), published.recv()).await.is_err());
    assert!(state.get_pending_blocks().is_none(), "unverified lineage must not stay tradable");
    assert!(resets.try_recv().is_err(), "finalization waits are not discontinuities");

    state.on_canonical_block_received(scenario.canonical);
    let recovered = timeout(Duration::from_secs(2), published.recv())
        .await
        .wrap_err("canonical recovery deadline expired")?
        .wrap_err("canonical recovery channel closed")?;
    assert_eq!(recovered.latest_block_number(), 2);
    assert_eq!(
        recovered.earliest_block_number(),
        2,
        "recovery must discard the conflicting parent"
    );
    Ok(())
}

#[tokio::test]
async fn wrong_parent_hash_stays_unpublished_after_canonical_recovery() -> Result<()> {
    let mut scenario = ParentBoundary::new(1).await?;
    scenario.child.base.as_mut().ok_or_else(|| eyre!("child base missing"))?.parent_hash =
        B256::with_last_byte(77);
    let state = &scenario.harness.flashblocks;
    ParentBoundary::publish(state, scenario.parent, "parent_before_wrong_child").await?;
    let mut published = state.subscribe_to_flashblocks();
    state.on_flashblock_received(scenario.child);
    assert!(timeout(Duration::from_millis(25), published.recv()).await.is_err());
    state.on_canonical_block_received(scenario.canonical);
    assert!(
        timeout(Duration::from_millis(100), published.recv()).await.is_err(),
        "canonical recovery must not authenticate a child naming a different parent"
    );
    assert!(state.get_pending_blocks().is_none());
    Ok(())
}
