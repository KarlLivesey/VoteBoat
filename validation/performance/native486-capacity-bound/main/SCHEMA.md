# 486: prove spare Counter capacity before building a distinct-ID tree

Before editing:485 restores checked480 after all96 fixed484 cases reject483's
temporary ingress API. Preserve all Mac loaded gains and Linux/cold-tail losses.
The current480 profile482 still locates Counter validation under57.003% of Linux
native cycle-period weight (overlapping submission55.257%); this is the measured
stage, not a CPU attribution inferred from elapsed storage timings.

Data/API shape: change only Counter's existing validate_proposal algorithm. Its
candidate syntax check remains first. Let R be remaining_operations(), immutable
for this call. When R>0, use direct next()/push to buffer at most R pending
OperationIds. Stop on the first None, even for a non-fused iterator. If fewer than
R IDs exist, distinct not-yet-applied IDs plus the candidate are at most R, so
return the exact existing CounterReceipt byte bound without tree construction or
dedup lookups. At exactly R, replay the buffered IDs followed by the untouched
iterator into the original full BTreeSet/dedup algorithm. R=0 uses that original
algorithm immediately. Do not trust size_hint, collect/count/nth overrides or a
claimed distinct count for correctness, and do not preallocate R slots.

Ownership/transitions: the scratch Vec owns only copied IDs, no command payloads,
application references or accepted requests. Logical length is at most R, itself
at most Counter's unchanged1M configured lifetime limit. Incremental Vec growth
can reserve extra capacity; do not claim physical allocation equals length or a
measured memory plateau. Near capacity the fallback can temporarily hold the
buffer and growing exact tree; the buffer is consumed/dropped within this call.
There is no persistent state, revision/cache, API/seam/default change, admission
scope, batching delay or receipt/ticket/outcome removal. Ready preparation and all
other applications retain their existing implementation and contracts.

Failure/cleanup: the original can first exceed R distinct novel pending IDs only
on pending item R+1. Buffering the first R therefore cannot miss an earlier
capacity error or consume past its original first failing item. Candidate syntax
still precedes iterator access. Duplicate/applied/conflicting IDs are safe under
the same raw-count bound; R=0 and pending lengths reaching R use exact fallback.
Iterator panic unwinds the
temporary buffer without application mutation; no callback or I/O is added.
Restart/checkpoint/retry/durable effect authority is unchanged because no state
escapes. Keep the original validator as an independent test oracle.

Acceptance: exhaustive small applied-ID/pending-ID/candidate combinations against
the old algorithm, plus larger capacity boundaries; exact results/receipt bytes
and application checkpoint unchanged. Direct hostile-hint/overridden-method,
non-fused first-None, panic, R0/exact-R and first-capacity-error iteration controls
must preserve observable consumption/error order. Tests must not mirror only the
fastpath. Run affected application/client/provider all/core checks sequentially,
actual native three-service retry/recovery controls, metadata and full enabled
formatter/four strict root/standalone checks. Freeze hashes/modes/commands/exits,
commit/push the meaningful checked slice without waiting for CI. Then fresh
asserted native root/executable artifacts and scoped Mac qualification, followed
by unchanged paired480/486 Linux/Mac workload including all cold-tail checks.
No speed claim until those actual measurements; preserve failures/regressions.

The full P9 objective stays active: all sustained framework cells/margins on both
hosts, native headroom/fairness/skew, bounded memory/debt, independent placement/
shared barriers and P8 composition. Real ext4/XFS pending; no P10/Windows/Pi work.
