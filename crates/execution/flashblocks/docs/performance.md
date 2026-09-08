# Pending RPC preparation

Every explicit code override makes alloy hash its bytes and build a revm jump table for each RPC
request. This is unnecessary for code whose pre-commit hash did not change. Pending accumulation
now loads the pre-commit account (normally already cached by execution) and compares hashes once
per transaction. Only actual transaction code changes carry bytecode into repeated RPC preparation. Balance,
nonce and storage maps still clone and apply per request; this does not remove all preparation cost.

A bounded September 8, 2026 production profile attributed 62.92% of sampled user cycles to Keccak
and 25.57% to legacy bytecode analysis. An independently sampled call graph placed 62.38% under
`eth_call` execution and 33.57% under gas estimation. The rebuilt symbol binary's `.text` exactly
matched the running binary. These are pre-change workload measurements, not post-deployment gains.

The ignored regression benchmark calls the actual alloy `apply_state_overrides` implementation
on identical canonical fixtures and pending balance/nonce/storage updates. On an unoptimized
macOS test build, 100 requests with 128 unchanged 16 KiB contracts produced:

| Preparation | Explicit code per request | Total time for 100 requests |
| --- | ---: | ---: |
| Previous padded code overrides | 2,097,280 bytes | 21,577,633 µs |
| Unchanged code omitted | 0 bytes | 29,022 µs |

These synthetic debug timings isolate repeated override preparation and must not be extrapolated
to production p95, throughput or profitability. The semantic regressions independently execute
storage reads, `EXTCODESIZE` and `EXTCODEHASH` for both canonical and pending-created contracts.
After deployment, compare matching workloads and context/freshness rejection rates before claiming
an end-to-end improvement. No freshness deadline or execution permit changed.

Run the bounded benchmark separately:

```sh
cargo nextest run -p base-flashblocks --run-ignored only \
  -E 'test(benchmark_repeated_pending_rpc_override_preparation)' --success-output immediate
```

Pre-execution uses a temporary commit hook once per block, cloning only the small system-call
states (normally the two history contracts; additionally the deployer at Canyon activation). These
accounts retain full original code because capture occurs after their internal commit. Their cost
is bounded by protocol system calls, independently of the number and size of contracts touched by
ordinary transactions. The hook is removed before transaction execution; it does not retain or
clone every transaction state. No production latency claim is made without a deployment profile.
