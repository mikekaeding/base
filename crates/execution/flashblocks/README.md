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

Same-block processing does not wait for canonical state. Across a block boundary, the incoming
parent hash must authenticate the locally available canonical header. Its complete header must match
the executed public prefix, including the ordered transaction root and cumulative executed gas;
only a zero provisional state root can be replaced by the authenticated canonical root. A public
`diff.block_hash` is not that canonical identity, and a hash difference alone does not prove a hidden
tail. An unavailable header, different canonical hash, or differing prefix is reported separately;
the next index-zero payload is retained and rebuilt from canonical state without resetting consumers.
Cached payloads retain their original receive time and recovery republishes them in index order so
downstream state machines see the missing index zero. The trader adapter uses that timestamp to mark
payloads older than the 200 ms freshness budget as synchronization-only rather than trade signals.
An in-line sequence or execution divergence still quarantines that lineage. Canonical
updates have queue priority. Directly queued snapshots delayed by at least one 200 ms Flashblock
interval are still published in sequence for downstream state continuity, while the trader adapter
marks them synchronization-only and suppresses decisions.

Boundary verification performs one local header lookup, header hash and fixed-size field comparison,
not a network request or state-trie rebuild. It does not remove waits for unavailable canonical data;
any latency reduction requires measurement on a running node.

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
