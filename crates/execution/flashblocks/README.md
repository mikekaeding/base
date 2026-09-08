# `base-flashblocks`

<a href="https://github.com/base/base/actions/workflows/ci.yml"><img src="https://github.com/base/base/actions/workflows/ci.yml/badge.svg?label=ci" alt="CI"></a>
<a href="https://github.com/base/base/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-MIT-d1d1f6.svg?label=license&labelColor=2a2f35" alt="MIT License"></a>

Flashblocks state management for Base nodes. Subscribes to flashblocks and combines the state with the canonical block stream to provide a consistent view of pending transactions, blocks, and receipts before they are finalized on-chain.

## Overview

- **`FlashblocksState`**: Core state container that tracks pending blocks and transactions.
- **`FlashblocksSubscriber`**: WebSocket subscriber for receiving flashblock updates from the builder.
- **`StateProcessor`**: Processes incoming flashblocks and produces state updates.
- **`PendingBlocks`**: Manages the collection of pending blocks with builder pattern via `PendingBlocksBuilder`.
- **`PendingStateBuilder`**: Builds pending state from executed transactions.
- **`CanonicalBlockReconciler`**: Reconciles flashblock state with canonical chain updates.
- **`ReorgDetector`**: Detects chain reorganizations affecting pending state.

Canonical reconciliation rebuilds any still-pending future flashblocks from a fresh canonical state
provider. Pending state must not carry a database read transaction across canonical blocks because
long-lived snapshots can expire and interrupt flashblock processing.

The normal Flashblock path never waits for canonical state. At block boundaries it validates the
next block's parent hash and base fee against the speculative parent in constant time. A
block-boundary mismatch means the producer sealed a hidden tail after the last public prefix, so the
next index-zero payload is held briefly and rebuilt from canonical state without resetting consumers.
Cached payloads retain their original receive time and recovery republishes them in index order so
downstream state machines see the missing index zero. The trader adapter uses that timestamp to mark
payloads older than the 200 ms freshness budget as synchronization-only rather than trade signals.
An in-line sequence or execution divergence still quarantines that lineage. Canonical
updates have queue priority. Directly queued snapshots delayed by at least one 200 ms Flashblock
interval are still published in sequence for downstream state continuity, while the trader adapter
marks them synchronization-only and suppresses decisions.

## RPC Extensions

This crate provides pending-state-aware Ethereum RPC implementations used by
`base-flashblocks-node`:

- **`eth_getBlockByNumber("pending", ...)`**: returns the latest pending block built from flashblocks.
- **`eth_getTransactionReceipt`** and **`eth_getTransactionByHash`**: check canonical data first, then flashblocks pending state.
- **`eth_getBalance`**, **`eth_getTransactionCount`**, **`eth_call`**, **`eth_estimateGas`**, and **`eth_simulateV1`**: use flashblocks pending state when requested with the `pending` tag.
- **`eth_getLogs`**: combines historical logs with pending flashblock logs when the range ends at `pending`.
- **`eth_getBlockTransactionCountByNumber("pending")`**: returns the transaction count from the latest pending flashblock state.
- **`eth_sendRawTransactionSync`**: sends a raw transaction and waits for inclusion in flashblocks or the canonical chain.
- **`eth_subscribe("newFlashblocks")`**: streams pending block updates from flashblocks.
- **`eth_subscribe("pendingLogs", filter)`**: streams logs from the latest flashblock.
- **`eth_subscribe("newFlashblockTransactions", ...)`**: streams transaction hashes or full transactions from the latest flashblock.

## Usage

Add the dependency to your `Cargo.toml`:

```toml
[dependencies]
base-flashblocks = { git = "https://github.com/base/base" }
```

Subscribe to flashblocks and process state updates:

```rust,ignore
use std::{sync::Arc, time::Duration};

use base_flashblocks::{
    FlashblocksAPI, FlashblocksState, FlashblocksSubscriber, PendingBlocksAPI,
};
use url::Url;

let flashblocks_url = Url::parse("ws://127.0.0.1:1111")?;
let state = Arc::new(FlashblocksState::new(3));

// Start the state processor after a node provider is available.
state.start(provider.clone());

// Connect to the builder's flashblocks WebSocket and forward decoded payloads into state.
let mut subscriber =
    FlashblocksSubscriber::new(Arc::clone(&state), flashblocks_url, Duration::from_secs(30));
subscriber.start();

// Read the current pending snapshot.
let pending_blocks = state.get_pending_blocks();
let pending_block = pending_blocks.get_block(true);

// Subscribe to future pending snapshot updates.
let mut updates = state.subscribe_to_flashblocks();
while let Ok(pending) = updates.recv().await {
    let block = pending.get_latest_block(true);
    println!("pending block: {}", block.header.number);
}
```

## License

Licensed under the [MIT License](https://github.com/base/base/blob/main/LICENSE).

## Pending RPC execution

`eth_call` and `eth_estimateGas` with `pending` combine the accumulated pending account/storage
state with the latest pending header from one immutable snapshot. The underlying database remains
anchored to the canonical block preceding that snapshot. NUMBER, TIMESTAMP, GASLIMIT, COINBASE,
PREVRANDAO, and BASEFEE therefore describe the same pending block as the storage being executed.
An unavailable pending snapshot falls back to the latest canonical block.

Explicit user block overrides take precedence field by field. Account overrides also preserve
unmentioned pending balance, nonce, code, and storage. A user `state` replaces the whole storage
map; `stateDiff` replaces only supplied slots. Invalid simultaneous user `state` and `stateDiff`
remain invalid for the underlying RPC validator.

The Flashblocks `eth_simulateV1` extension executes its first group in the latest pending block,
with explicit user group overrides taking precedence. It applies the pending storage snapshot
once, before that group; subsequent groups retain simulated mutations and normal Reth block/time
progression. These compact overrides do not provide a historical archive or sequencer inclusion
guarantee, and do not change exact state or block-hash guards.

Pending snapshot acquisition uses an Arc reference. Preparing a call clones the full account and
storage override map, so its cost scales with pending accounts and slots. Preparation acquires the
existing RPC blocking-IO permit and performs that clone on Reth's blocking pool. It must never
clone the complete map on a Tokio scheduler thread or once per simulated group. The preparation
permit is released before the underlying RPC acquires its execution permit, avoiding nested
permit acquisition. Request cancellation retains the permit until active preparation completes.

Ordinary transaction overrides include bytecode only when execution changes the pre-commit code hash.
Existing canonical code stays in the database, avoiding repeated hashing and jump-table analysis
for every RPC call. A later transaction retains code overrides created by earlier pending
Flashblocks, even if that transaction does not load the code. Real code changes use the original
unpadded bytes; creation and destruction preserve their storage-reset semantics. Fresh and cached
executions share this accumulation path. Block system commits also enter the pending map: current
EIP-2935/EIP-4788 storage and historical Canyon deployer installation remain visible to pending RPC.
These few system accounts retain original code explicitly because they are captured after commit.
See [performance notes](docs/performance.md).

The RPC regressions execute EVM environment-reading bytecode against an older canonical starting
environment, verify explicit overrides, preserve coherent snapshots across publication, and
check that multiple simulation groups do not reset pending storage.
