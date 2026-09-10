# Flashblocks parent authentication

Pending reuse, cold initialization and canonical recovery must all bind execution to the parent
named by the incoming payload. A canonical notification does not waive that invariant. Authenticate
against the local canonical header before applying system calls or populating `BLOCKHASH`.
Open the backing state provider by that verified header's hash, not a second height lookup that can
resolve to a different block if a reorg intervenes.

When a rebuild spans several blocks, each previously executed header and its actual cumulative gas
must match its authenticated canonical header. Only an explicitly zero provisional state root may
be substituted. A complete ordered transaction prefix or matching height alone is not sufficient.

Unavailable or mismatching parents remain cached, nontradable, with their original receive time.
Correct canonical evidence permits recovery; it must never make a conflicting child tradable.
