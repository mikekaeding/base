//! Opt-in, read-only replay of captured mainnet Flashblocks against canonical historical state.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use alloy_consensus::TxReceipt;
use alloy_eips::Encodable2718;
use alloy_network::TransactionResponse;
use alloy_primitives::B256;
use alloy_primitives::KECCAK256_EMPTY;
use alloy_primitives::U256;
use alloy_primitives::keccak256;
use base_common_flashblocks::Flashblock;
use base_execution_chainspec::BaseChainSpec;
use base_flashblocks::FlashblocksAPI;
use base_flashblocks::FlashblocksReceiver;
use base_flashblocks::FlashblocksState;
use base_node_runner::BaseNode;
use eyre::Context;
use eyre::Result;
use eyre::ensure;
use eyre::eyre;
use reth_provider::BlockHashReader;
use reth_provider::BlockNumReader;
use reth_provider::BlockReader;
use reth_provider::ReceiptProvider;
use reth_provider::StaticFileProviderFactory;
use reth_provider::TransactionVariant;
use reth_provider::TryIntoHistoricalStateProvider;
use reth_provider::providers::BlockchainProvider;
use reth_provider::providers::ReadOnlyConfig;
use reth_tasks::Runtime;
use reth_trie_common::HashedPostState;
use reth_trie_common::KeccakKeyHasher;
use serde_json::Value;
use serde_json::json;
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires BASE_REPLAY_DATA_DIR read-only mainnet view and BASE_REPLAY_CAPTURE"]
async fn historical_parent_replay_matches_canonical_state_roots() -> Result<()> {
    let directory = std::env::var("BASE_REPLAY_DATA_DIR").wrap_err("read mainnet view path")?;
    let capture = std::env::var("BASE_REPLAY_CAPTURE").wrap_err("read captured payload path")?;
    let canonical_notices =
        std::env::var("BASE_REPLAY_CANONICAL_NOTICES").is_ok_and(|value| value == "1");
    let source = File::open(capture).wrap_err("open captured Flashblocks")?;
    let mut blocks = BTreeMap::<u64, Vec<Flashblock>>::new();
    for line in BufReader::new(source).lines() {
        let line = line.wrap_err("read captured Flashblock line")?;
        let mut row: Value = serde_json::from_str(&line).wrap_err("decode capture record")?;
        let payload: Flashblock = serde_json::from_value(row["payload"].take())
            .wrap_err("decode captured Flashblock payload")?;
        blocks.entry(payload.metadata.block_number).or_default().push(payload);
    }
    let last = *blocks.last_key_value().ok_or_else(|| eyre!("empty capture"))?.0;
    blocks.retain(|number, payloads| {
        *number != last && payloads.first().is_some_and(|payload| payload.index == 0)
    });
    let factory = BaseNode::provider_factory_builder()
        .open_read_only(
            Arc::new(BaseChainSpec::mainnet()),
            // Sync on opening each read transaction, not via an index-mutating background watcher.
            ReadOnlyConfig::from_datadir(directory).no_watch(),
            Runtime::test(),
        )
        .wrap_err("open explicitly read-only mainnet provider")?;
    let provider =
        BlockchainProvider::new(factory.clone()).wrap_err("open canonical blockchain view")?;
    let blocks: Vec<_> = blocks.into_iter().collect();
    ensure!(blocks.len() >= 16, "at least sixteen complete blocks are required");
    let swap_topics = [
        keccak256("Swap(address,uint256,uint256,uint256,uint256,address)"),
        keccak256("Swap(address,address,int256,int256,uint160,uint128,int24)"),
        keccak256("Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)"),
    ];
    let mut verified_blocks = 0;
    let mut root_checks = Vec::new();
    let mut swap_pools = BTreeSet::new();
    let mut swaps = 0;
    // Pair-local processors bound retained state and compare exactly the same sixteen blocks.
    for pair in blocks.as_chunks::<2>().0.iter().take(8) {
        ensure!(pair[1].0 == pair[0].0 + 1, "capture contains a block gap");
        let state = FlashblocksState::new(5);
        state.start(provider.clone());
        let mut publications = state.subscribe_to_flashblocks();
        let mut resets = state.subscribe_to_resets();
        for (number, payloads) in pair {
            let canonical = provider
                .sealed_block_with_senders((*number).into(), TransactionVariant::WithHash)
                .wrap_err_with(|| format!("read canonical block {number}"))?
                .ok_or_else(|| {
                    eyre!("canonical block {number} not yet persisted or unavailable")
                })?;
            let captured_transactions: Vec<_> =
                payloads.iter().flat_map(|payload| payload.diff.transactions.iter()).collect();
            let canonical_transactions: Vec<_> = canonical
                .body()
                .transactions()
                .map(|transaction| transaction.encoded_2718())
                .collect();
            ensure!(
                captured_transactions.len() == canonical_transactions.len(),
                "incomplete captured block {number}"
            );
            ensure!(
                captured_transactions
                    .iter()
                    .zip(&canonical_transactions)
                    .all(|(captured, canonical)| captured.as_ref() == canonical.as_slice()),
                "captured transaction order differs from canonical block {number}"
            );
            for (expected_index, payload) in payloads.iter().enumerate() {
                ensure!(
                    payload.index
                        == u64::try_from(expected_index).wrap_err("convert payload index")?,
                    "nonsequential captured index"
                );
                let started = Instant::now();
                state.on_flashblock_received(payload.clone());
                let pending = timeout(Duration::from_secs(5), publications.recv())
                    .await
                    .wrap_err_with(|| {
                        format!("publication timed out for {number}/{}", payload.index)
                    })?
                    .wrap_err("pending publication stream closed")?;
                let publication_us = started.elapsed().as_micros();
                ensure!(
                    pending.latest_block_number() == *number
                        && pending.latest_flashblock_index() == payload.index,
                    "unexpected pending publication"
                );
                if *number == pair[1].0 && payload.index == 0 && !canonical_notices {
                    ensure!(
                        pending.earliest_block_number() == pair[0].0,
                        "complete parent was not reused"
                    );
                }
                ensure!(resets.try_recv().is_err(), "captured valid lineage reset consumers");
                println!(
                    "{}",
                    json!({"kind":"publication", "canonical_notices":canonical_notices,
                    "block":number,"index":payload.index,"transactions":payload.diff.transactions.len(),
                    "retained_transactions":pending.pending_transaction_count(),"publication_us":publication_us})
                );
                if expected_index + 1 == payloads.len() {
                    ensure!(
                        pending.matches_canonical_parent(canonical.header()),
                        "pending header differs from canonical block {number}"
                    );
                    root_checks.push((canonical.header().clone(), Arc::clone(&pending)));
                    let canonical_receipts = provider
                        .receipts_by_block((*number).into())
                        .wrap_err("read canonical block receipts")?
                        .ok_or_else(|| eyre!("canonical receipts unavailable for {number}"))?;
                    let canonical_state = factory
                        .history_by_block_hash(canonical.hash())
                        .wrap_err("open canonical post-state for all replayed writes")?;
                    let bundle = pending.get_bundle_state();
                    let mut checked_slots = 0;
                    for (address, account) in &bundle.state {
                        let actual = canonical_state
                            .basic_account(address)
                            .wrap_err("read canonical account after replay")?;
                        match (&account.info, actual) {
                            (None, None) => {}
                            (Some(expected), Some(actual)) => {
                                ensure!(
                                    expected.balance == actual.balance
                                        && expected.nonce == actual.nonce
                                        && expected.code_hash
                                            == actual.bytecode_hash.unwrap_or(KECCAK256_EMPTY),
                                    "replayed account differs from canonical post-state"
                                );
                            }
                            _ => {
                                return Err(eyre!(
                                    "replayed account existence differs from canonical post-state"
                                ));
                            }
                        }
                        for (slot, expected) in &account.storage {
                            let actual = canonical_state
                                .storage(*address, B256::from(slot.to_be_bytes::<32>()))
                                .wrap_err("read canonical storage after replay")?
                                .unwrap_or(U256::ZERO);
                            ensure!(
                                actual == expected.present_value,
                                "replayed storage differs from canonical post-state"
                            );
                            checked_slots += 1;
                        }
                    }
                    let pending_transactions: Vec<_> =
                        pending.get_transactions_for_block(*number).collect();
                    ensure!(
                        pending_transactions.len() == canonical_receipts.len(),
                        "receipt count mismatch"
                    );
                    let mut block_swaps = 0;
                    for (transaction, canonical_receipt) in
                        pending_transactions.iter().zip(&canonical_receipts)
                    {
                        let receipt = pending
                            .get_receipt(transaction.tx_hash())
                            .ok_or_else(|| eyre!("missing replay receipt"))?;
                        let executed_receipt =
                            receipt.inner.inner.receipt.clone().map_logs(|log| log.inner);
                        ensure!(
                            &executed_receipt == canonical_receipt,
                            "executed receipt differs from canonical receipt"
                        );
                        for log in receipt.inner.inner.receipt.logs() {
                            if log.topics().first().is_some_and(|topic| swap_topics.contains(topic))
                            {
                                swap_pools.insert(log.address());
                                block_swaps += 1;
                            }
                        }
                    }
                    swaps += block_swaps;
                    verified_blocks += 1;
                    println!(
                        "{}",
                        json!({"kind":"receipts_verified_block","canonical_notices":canonical_notices,
                        "block":number,"hash":canonical.hash(),"expected_state_root":canonical.header().state_root,"gas_used":canonical.header().gas_used,
                        "transactions":canonical_transactions.len(),"swap_logs":block_swaps,
                        "accounts_checked":bundle.state.len(),"storage_slots_checked":checked_slots})
                    );
                }
            }
            if canonical_notices {
                state.on_canonical_block_received(canonical);
            }
        }
    }
    ensure!(swaps > 0, "captured blocks contain no recognized DEX swaps");
    println!(
        "{}",
        json!({"kind":"execution_complete", "blocks":verified_blocks, "root_checks_pending":root_checks.len()})
    );
    for (canonical, pending) in root_checks {
        let number = canonical.number;
        // Pin the database transaction before refreshing the read-only file index:
        // archive commits can publish their MDBX and static-file metadata separately.
        let database = factory.provider().wrap_err("pin historical verification snapshot")?;
        database
            .static_file_provider()
            .initialize_index()
            .wrap_err("refresh static-file visibility for pinned snapshot")?;
        let tip = database.best_block_number().wrap_err("read pinned verification tip")?;
        let static_tip = database.last_block_number().wrap_err("read static-file tip")?;
        println!(
            "{}",
            json!({"kind":"verification_snapshot", "database_tip":tip, "static_tip":static_tip})
        );
        for _attempt in 0..50 {
            if database.block_hash(tip).wrap_err("check pinned tip header visibility")?.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
            database
                .static_file_provider()
                .initialize_index()
                .wrap_err("refresh pinned snapshot's pending static-file commit")?;
        }
        ensure!(
            database.block_hash(tip).wrap_err("verify pinned tip header visibility")?.is_some(),
            "static files do not expose pinned database tip {tip} after bounded synchronization"
        );
        let parent_number = database
            .block_number(pending.parent_hash())
            .wrap_err("resolve immutable replay parent")?
            .ok_or_else(|| eyre!("replay parent unavailable"))?;
        let base = database
            .try_into_history_at_block(parent_number)
            .wrap_err("open canonical replay parent state")?;
        let bundle = pending.get_bundle_state();
        let hashed = HashedPostState::from_bundle_state::<KeccakKeyHasher>(&bundle.state);
        let root = base
            .state_root(hashed)
            .wrap_err("compute independently verified resulting state root")?;
        ensure!(
            root == canonical.state_root,
            "computed state root differs from canonical block {number}"
        );
        println!("{}", json!({"kind":"root_verified", "block":number, "state_root":root}));
    }
    println!(
        "{}",
        json!({"kind":"summary","verified_blocks":verified_blocks,
        "swap_logs":swaps,"unique_swap_pools":swap_pools.len(),"canonical_notices":canonical_notices})
    );
    Ok(())
}
