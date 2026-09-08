# Pending RPC state

The pending execution database and accumulated RPC overrides advance together. Before each fresh
or cached transaction is committed, compare its resulting account code hash with the pre-commit
database. Balance, nonce and storage updates remain independent of code changes. Untouched accounts
do not change database state and therefore do not create overrides.

The accumulated override map must travel with the pending database across Flashblocks. An unchanged
code hash means retain any earlier pending code override, not remove it: the RPC starts from
canonical state and still needs contracts created or delegated during the pending prefix. A changed
hash with unloaded code must resolve to matching database bytecode or fail closed. Creation and
selfdestruction replace storage; later changes extend that replacement instead of mixing `state`
with `stateDiff`.

RPC preparation applies this map to the canonical database underlying the captured pending
snapshot. Only original bytecode is a valid RPC code override; interpreter padding is never part
of a contract's on-chain identity. Header selection and explicit caller override precedence are
unchanged by code omission.
