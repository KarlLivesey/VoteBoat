# Bounded checkpoint interstitial attribution

Schema reviewed and authorized after native15132 completed all36 cases and
source/artifact guards. Isolated implementation is qualified and the six-path overlay is promoted. Main
qualification passes 32 controls, three actual CLI histories and the complete
strict hook. Fresh Linux release measurement passes all nine cases; matched Mac timing also passes all nine cases. State_to_pin accounts for about 93% of Linux whole compaction, so the
separately reviewed 426 predicate is the next bounded optimization. The acceptance criteria below remain required.

## Evidence and actual call path

The first Linux 424 ON leader has 204 checkpoints and 408.832ms inclusive compaction,
maximum 2.486ms (mean 2.004ms). Scoped provider clone construction is 11.678ms,
append 4.868ms; publication is 1.090ms. These are elapsed wall intervals, not CPU.
They do not assign the remaining work or prove a cause.

compact_replica invokes verify_log, binding/application/prefix checks, pin and
load_pinned, exact reference/bootstrap/checkpoint-membership verification, cloned
Noop restoration, Raft.begin_compact, persist_effect and final reconcile_log.
verify_log's full GroupLog equality and the cloned temporary's destruction occur
AFTER MemoryStore.state's construction interval and BEFORE pin_for_log starts.
The construction fraction therefore does not bound the saving of eliminating that
materialization/destruction while retaining full content equality.

begin_compact checks fencing/dependencies, exact store/index/term and cached
checkpoint membership; persist_update uses OwnedNextLog::plan_copy. The ordinary
Noop conditions imply plain/clone-stable matching compaction: no configuration
records/membership checkpoint, empty normalized payloads, unchanged HardState,
matching committed reference. Stale summary metadata rejects admission rather than
silently replaying. Full/configured fallback remains available and mandatory;
actual branch timing is not proven just by these source conditions.

OwnedNextLog's Compact plan performs exact scalar/predecessor/snapshot checks plus
removed-prefix retained-byte accounting. Its prepared install drains the removed
prefix and installs metadata. MemoryStore already measures its own same planner/
drain inside append; Core planning/install sit outside that provider interval.
Core complete correlates exact durable ticket, installs its pending plan, retains
membership/retirement rules and invokes After::Compact. That invalidates ordinary,
admitted, empty-alias and repair collectors, then broadcasts as leader. append_for
clones at most 1024 retained originals per peer or requests a verified snapshot;
context is only a checked sequence increment, not a history hash. No explicit
full-history hash exists in that path. Broadcast/request work still needs elapsed
attribution before changing collector retirement or scheduling.

## Static allocation evidence and hypothesis

The actual empty-envelope report derives size_of<LogEntry>=64 bytes. StoreState
reports 4,334,273 cloned retained entries across 204 checkpoints plus one empty setup
call: mean 21,246 entries, 1,359,772 element bytes per temporary. Peak retained entries
are 21,944 (1,404,416 element bytes). Thus verification revisits roughly 4.13 times
measured originals; arrays are about 1.3MiB, not just the 5000 checkpoint threshold.
Other bootstrap/image allocations are excluded from these element-byte estimates.

Host getconf reports glibc 2.44; no custom global_allocator declaration was found in
root/matched/pinned minimal library sources. Large-array allocator deallocation,
mmap/munmap and TLB work is a hypothesis, not an observed cause. Actual allocation
capacity/backend thresholds/syscalls were not measured; glibc adaptive thresholds
mean size alone does not imply mmap. The reported Mac whole compaction ~31ms versus
construction~6.4ms is another reason to distinguish construction from Eq/destruction
without attributing a platform cause.

## Minimum data/API shape

Extend the EXISTING benchmark-only const-generic Recorder with six fixed optional
method-boundary stamps, each carrying the method call ordinal and Instant:
state.end, pin.start, pin.end, append.start, barrier.end and reconcile.start.
Reuse each actual method's existing start/end clock observations; no per-entry
trace, allocation, shared mutex, reset/measurement epoch or additional Core/public
helper seam. The state/pin/append/barrier/reconcile methods are each invoked exactly
once per successful compact_replica call. Actor's existing before/after scoped
provider counts certify those ordinals; collected stamps belong to this call only.

After the unchanged helper returns, collect three bounded gap accumulators:

| Gap | Exact interval | Work included |
| --- | --- | --- |
| state_to_pin | state.end→pin.start | Full content Eq, cloned GroupLog destruction, subsequent binding/application/prefix guards and observer/return overhead |
| pin_to_append | pin.end→append.start | Pinned image load, reference/bootstrap/membership checks, app clone/restore, begin_compact/Core planning and persist preflight; includes the load provider interval |
| barrier_to_reconcile | barrier.end→reconcile.start | Core complete/install/drain, collector retirement and broadcast, completion/ticket cleanup and minor return/observer overhead |

These are boundary names, not pure Eq/Core labels. Gaps themselves occur in order;
provider load intervals nested within them are not additive. Everything outside
these gaps/method intervals remains explicitly unattributed. Maxima are maxima of
whole successful gap observations, never differences of provider maxima. Records
cover actor lifetime including warmup/drain, not an invented timed-only window.

## Ownership and failures

Existing exclusive actor owns all recorders. TRACE=false reads no clocks, stamps
or counter cells. No callback influences provider/Core outcomes; ordinary tickets,
bindings, exact equality, snapshot pins, restore, completion and reply proofs stay
unchanged. Before→after counts must advance exactly 1 for each selected method;
each stamp ordinal must equal the after count. Check monotonic ordering without
panicking. Missing/stale/overflowed observation on an otherwise successful helper
invalidates diagnostics only at successful owned shutdown, as existing Recorder
policy does. Failed/partial helpers keep their original errors and produce no
fabricated complete gap; no provider cleanup or fencing change is introduced.

## Completion checks and next decision

Synthetic boundary/ordinal controls cover exact gaps, stale/missing stamps,
backward order, checked overflow and maxima. Disabled-mode poison confirms no
reads. Actual off/on maintenance, every original/receipt/frontier and partial
helper-error controls remain; no copied helper algorithm. Fmt/strict zero and
fresh guarded release/off-on observer-impact evidence precede any rate claim.

If state_to_pin dominates, review borrowed exact matches_state with default full
comparison and exact native/memory overrides: remove materialization/destruction
but keep full byte/OperationId/header equality, binding and durable visibility.
If barrier_to_reconcile dominates, isolate Core install versus broadcast before a
collector/replication change. If pin_to_append dominates, verify plain-versus-full
planning/metadata validation before changing caches. This is an evidence-driven
choice; no optimization is authorized by this schema alone.
