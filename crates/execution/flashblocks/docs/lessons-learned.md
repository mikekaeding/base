# Pending override invariants

- revm `Bytecode::bytes()` contains interpreter padding for legacy code. Use `original_bytes()`
  for RPC overrides, otherwise both `EXTCODEHASH` and `EXTCODESIZE` can change.
- A loaded account's code is not necessarily a code change. Repeating unchanged code overrides
  can move most RPC CPU time into hashing and jump-table analysis.
- Compare code hashes before commit and preserve earlier pending code overrides. Dropping an
  unloaded code field can lose a pending creation or delegation even when current storage is right.
- Profile against a binary with proven matching executable text before trusting recovered symbol
  addresses; a rebuilt binary with the same source name is insufficient evidence.
- A correct transaction overlay is insufficient: block system calls may commit directly to the
  execution database. Header beacon-root metadata does not install the corresponding contract
  storage in RPC state. Regressions must call real system code starting from canonical state, not
  merely inspect the mutated execution cache.
- Block-level fee parameters can contain a transaction-scoped calculation cache. When reused for
  several RPC receipts, invalidate that cache at every transaction boundary. Similar L2 gas values
  do not validate L1 data fees; compare short and long encoded transactions, including a stale
  cache inherited by a new builder, against independent per-transaction calculations.
