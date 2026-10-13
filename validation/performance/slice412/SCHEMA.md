# Slice412 opaque owned log validation

Purpose: expose safe reuse of the private validated delta planner to host stores,
removing modeled MemoryStore history-sized copies without changing its volatility,
exact-ticket barrier or any native durability boundary.

API: public log::OwnedLogState::new(LogLimits), limits(), get(GroupIdentity),
apply_batch(Vec<LogMutation>), into_states(). The state map and summaries remain
private; no mutable lookup, unchecked recovery import, derived Clone, durability
token or new LogStore provider. Constructor validates bounds and starts empty;
bootstrap admission uses the existing create validator.

State: one canonical owned GroupLog and exact versioned LogSummary per group.
Every full batch checks limits, duplicate group units, reused IDs/counts and plans
all updates through NextLog (plain delta or exact legacy full fallback). All
install correlations are checked before any mutation. Private PreparedLog commits
nonfallibly under unique groups and exclusive owner access; callers cannot supply
or tamper with proof/cache objects. Commit rebuilds/replaces each version summary.

Failure: invalid later unit, stale revision or exhausted counter leaves EVERY
state unchanged. No asynchronous effect or I/O occurs in this helper. Snapshots,
configuration and replacement retain full validation and generation semantics.
Snapshot pin/storage identity enforcement remain the selected driver's/store's
existing responsibility. Model store still rejects checkpoints, one outstanding
volatile batch, wrong/replayed barrier tickets and reads before completion.

Acceptance: differential complete states and exact errors against apply_batch;
later-invalid cross-group atomicity; duplicate/create/group limits; config/joint,
snapshot/suffix generation and ballot origins; old payload pointers survive pure
append/metadata; readonly exports and consuming export preserve original data;
helper limits are immutable; modeled tickets/state/ranges remain guarded; existing
shared/core/native/provider conformance tests stay clean. Add no performance claim
until identical frozen reference/candidate histories/harness and exact completions
are measured. Native production was already optimized in409; model improvements
are not a new native/durable service result or a matched OpenRaft comparison.
