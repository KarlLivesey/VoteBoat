# Finer checkpoint helper attribution (422)

Purpose: locate the measured ~400ms/204-checkpoint maintenance cost before another
optimization. 420 stays rejected/unapplied. Frozen418 already caches plain
checkpoint membership and plans plain matching compaction without full Core or
provider materialization. The surviving exact provider-state clone/Eq, removed
prefix accounting/drains, and completion broadcast remain separate candidates.

Keep existing checkpoint_application, compact_replica and Core algorithms exactly
as invoked. Add four actor-inclusive intervals: checkpoint_reserve,
checkpoint_publish, checkpoint_compact, checkpoint_effects. Existing maintenance
continues to include no-op checks and all four. Provider intervals surround actual
MemoryStore.state (visibility check + clone), append/barrier and MemorySnapshots
begin/write/seal/publish/abort/load/latest-reference/pin/load-pinned/reconcile.
Identity/binding/limit getters stay uninstrumented. Snapshot buffers remain 8-byte
Noop payloads with original bootstrap/metadata checks and exact two-anchor pins.

Extend the fixed Recorder with Cell-backed interval storage so &self state reads
can be measured under the existing exclusive actor owner. MemorySnapshots becomes
const-generic like MemoryStore. No trait, dependency or provider algorithm copies.
Each helper samples fixed provider counter arrays before/after its existing call;
checked deltas accumulate per-scope calls/failures/elapsed/items with no reset or
measurement epoch. Provider maxima are whole lifetime; maxima cannot be differenced.
TRACE=false reads no attribution clocks, cells or snapshots. All data/phase counts
are fixed-size and bounded, without per-entry traces or heap growth during recording.

Provider observation never changes an underlying result. Checked counter overflow
sets a diagnostic-invalid flag; helpers complete or fail normally, retaining their
own fencing/pin/pending cleanup. Successful actor shutdown rejects an invalid
measurement; original helper errors remain the reported cause. There is no added
abort, rollback, authority or client receipt. Original IDs, application results,
barriers, snapshots and all follower frontiers remain independently checked.

Intervals are elapsed wall time, not CPU. Publication/compaction/effects and
maintenance are inclusive. Snapshot reconcile includes load-pinned; neither should
be counted twice. Scoped provider sums mark nesting explicitly. Any compact time
outside known provider intervals is an UNATTRIBUTED helper residual containing
Eq, validation, application clone/restore, Core staging/completion and marshalling;
it is not pure Core. Do not sum concurrent replicas or infer a critical path.

Controls: disabled mode performs no cell/clock reads; fixed snapshots/deltas are
checked with no invented maximum delta; nested reconcile/load attribution and
failure results are preserved; invalid observation cannot alter provider outcomes.
Existing original-result/maintenance/lagging snapshot controls run off/on. Actual
separate-runtime off/on CLI histories include 12000 entries, exact requests/frontiers
and scoped publication/compaction method counts. Fmt/strict zero. Freeze sources
with root before expanded hook/commit; no diagnostic timing during 421 builds.
