# Flashblocks validation lessons

- A fallback or recovery path must preserve the same authentication as a fast path. Otherwise a
  rejected child can become accepted on the next canonical callback despite naming the wrong parent.
- Header and state lookups must share an immutable block hash. Two independently correct height
  lookups can still identify different forks when a reorg occurs between them.
- Full-node fixtures must use actual parent hashes. A zero placeholder can let tests accidentally
  depend on missing lineage validation. Canonical callbacks should follow insertion into that node's
  provider, not substitute for it.
- Verify resulting balances, nonce, receipt status and gas, not only publication. Separate test-node
  startup and fixed harness sleeps from measured processing, and do not extrapolate toy-block timings.
