# Flashblocks boundary costs

Same-block append is unchanged by parent authentication. Next-block reuse performs a local header
lookup, header hash and fixed-size comparison; no network request or state-root computation.

Full rebuilds reuse their already-read canonical header for the first boundary. Further boundaries
require one local canonical-header lookup and comparison per block. Validation precedes expensive
execution and rejects unavailable or inconsistent lineage.

`base-flashblocks-node/tests/parent_boundary.rs` measures receive-to-publication with real Engine API
fixtures, excluding harness sleeps. Its small blocks and unoptimized test build are correctness/stress
checks, not production throughput or tail-latency estimates. Deployment-build measurements with real
load and canonical-header availability are still required before claiming a live latency improvement.
