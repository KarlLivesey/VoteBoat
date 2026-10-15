#470 before-edit schema: bounded same-group append persistence

Status: implementation plan, not a performance result. Baseline is published
469/`2d53b4c4`, whose production code equals retained455. The complete P9 goal
remains active. The current native barrier context is83–88% of inclusive elapsed
wall time; nested publication and sync are not additive CPU measurements.
This slice must deliver a selected usable native service path, not an unused
Core or worker API. No improvement is assumed before measurement.

## Selection, public composition and limits

Keep all existing serial APIs and defaults. Add startup-only
`Raft::configure_append_window(Option<AppendWindowLimits>)` plus an exact
`append_window_limits()` getter. Limits are `max_units`, `max_originals` and
`max_retained_bytes`; validate units2..=64, originals at least units and no
greater than the selected BatchLimits proposal envelope, bytes positive and
no greater than the selected LogLimits in-memory batch ceiling. This is a
bounded ordinary-command capability, not a wire/WAL format or quorum change.
The existing first-event configuration lock also applies to the first staging
attempt, including a refused attempt. No later live reconfiguration.

`ProposalAdmission::supports_append_window()` defaults false, separate from
`supports_ready_batch()`. An implementation explicitly promises validation
against unchanged durable Core/application context while prior staged originals
remain in the pending iterator, including duplicates/conflicting128-bit IDs and
canceled callers. It must tolerate the window's pending-dependency observation;
the capability grants no durability or authentication authority. Counter opts in
with a differential admission oracle. BucketCounter/composites/custom providers
remain serial unless they explicitly implement the stronger promise. Preserve
the order of context/proposal/receipt-capacity checks.

`PersistenceWorker::max_outstanding_units_per_group()` defaults one. Existing
native spawn selects one; an explicit native construction selects a finite
validated depth. Keep one atomic unit/group per submit request and per provider
append_batch; allow FIFO successive same-group requests only under that selected
capability. No new LogStore assertion or durability shortcut is introduced.

Add `NativeStartup.append_window: Option<AppendWindowLimits>`; existing callers
select None. Native startup validates the application capability and finite
worker/store/shard/owner/driver room before creating resources, configures each
recovered Core before its first event, then constructs the selected worker.
Explicit incompatible selection is an error, not an implicit downgrade.
Generic hosts configure their Core before registration; ReplicaDriver::new
validates selected Core limits against the actual application and worker,
identity/authorization and available protected budgets before intake. Runtime
owner reads the selected per-group Core capability. Logical/recursive owner
authorization does not select this path; existing features continue serially.
ReplicaDriver's physical and internal logical constructors deliberately require
ProposalAdmission in addition to their existing read/state bounds, so the
capability check is available at construction rather than deferred past intake.

No automatic multiplication of per-unit budgets. Window originals are bounded
across visits, separately from each visit's item/time/byte limit. Whole window
Core ownership includes independent plans, bounded complete accepted tail,
exported updates and conversion/tail-replacement peaks, charging full capacities
and immutable command bytes conservatively. Separate owner input/output,
worker metadata/payload, application receipts and native accepted state remain
charged by their existing limits. Preserve control/recovery reserves globally
and per group. If the narrow window ceiling cannot fit a legacy-valid input,
seal/drain and use the unchanged ordinary path; do not add its ceiling to legacy
proposal validation. Resource-pressure fallback cannot discard or reorder input.
Before the first unit, reserve a whole-window bulk escrow for eventual output:
checked U times (the opening Core effect_reservation(shard.max_event_bytes)
plus max_retained_bytes), plus all U frame/parked/unit-ID metadata. This is an
intentionally conservative output bound: the opened peer/configuration set is
stable and final retained-log growth is bounded by max_retained_bytes. It fits
inside the existing selected owner bulk ceiling; it does not enlarge that
ceiling. Construction validates finite minima and dynamic admission verifies
actual room before consumption. Retain it through the anchor continuation and
early leased sends. No accepted Durable may depend on finding future output
credits. Each successor also checks temporary/Core/worker input capacity first;
if room is unavailable, seal/drain rather than wait on credits held by itself.

## Core ownership and state transitions

Opaque AppendUnitId scopes full store binding, group incarnation, checked
nonreused window sequence and ordinal. StagedAppend carries that ID plus a
public mutable LogUpdate independent of the protected owned plan. Rejection
returns the original Proposal Vec. Aside from the first-attempt startup lock,
every staging error leaves Core/window/sequence counters/input ownership intact.

Only a plain stable local leader, stable committed membership, unchanged term,
hard state, ballot origin, generation and commit may open/stage the window.
No transfer/read/learner/configuration/snapshot/legacy persistence dependency.
Every unit is a nonempty Command-only exact suffix extension with the sole
continuation After::LeaderAppend and no reply or membership mutation.

Maintain separate accepted metadata/summary/tail from canonical durable state.
The accepted LogView is complete: canonical history plus the bounded window
tail, never merely the latest unit. Plan each unit against its exact predecessor
before publishing it; no clone of complete retained canonical history, new
EntryHistory or digest/revision equality substitute. Existing public step and
ordinary completion remain Busy while a window exists. state()/membership()
remain the unchanged durable/stable view during the window.

Validate exported effect equality against the selected independent unit plan
before transfer. Exact Written admission checks ID, binding, group, revision,
generation, last index/term and strict batch order. It grants no local durable,
quorum, application, maintenance or retirement authority. Early replication
resolves only the selected Written unit's bytes, preserves the existing one
ordinary request/peer, aliases, ranges and configured1024 ordinary envelope.
Never replace A's context with B/C or expose an unwritten descendant.

First valid exact Durable seals intake and records only that unit's evidence.
Wrong/stale/duplicate evidence cannot seal or mutate an unrelated window.
Out-of-order evidence is explicit, never a maximum prefix. Even if the physical
publication covered other bytes, only the requested exact tickets count.
Canonical state stays unchanged until **every unit in the sealed window has
exact Durable**. Preflight the immutable predecessor chain and required output
capacity, install plans in revision order, then update local durable progress
once, broadcast once and invoke maybe_commit once against the final revision.
Do not replay After::LeaderAppend per unit. This avoids a commit-only revision
at A colliding with already accepted B/C. Any impossible installation/callback
failure fences before effects escape. Normal commit persistence and ordered
application still gate every original success.

AppendCompletion is Waiting or Retired{units,effects}. Retired identifies the
whole ordered window; it is not an application receipt or a credit-release
instruction. Ordinary commit effects continue through the existing exact
Written/Durable path. Existing early requests retain their original context
until ordinary ACK/invalidation; later ACK drives the remaining durable suffix.

## Runtime, worker correlation and scheduling

Use `PersistUnitId{effect:EffectTicket, append:Option<AppendUnitId>}` throughout
PersistUnit, worker events and deliveries. EffectTicket includes exact visit
and effect sequence even for the ordinary path. WorkerTicket adds the selected
store/worker generation and request sequence. Neither group-only matching nor
the latest visit may authenticate a sibling completion.

There remains one runnable mutable visit/group. Park each append continuation
under its exact unit/visit; retain its used input credits, reservation, source
identity, leases and early effects. Only the scoped window path obtains a fresh
data-only visit through the ordinary fair scheduler. Public live/with_core/finish
must not acquire authority over arbitrary parked visits. Charge every parked
frame against existing global/per-group active-visit and byte ceilings.

Before a successor consumes input, drive available worker/control/timer intake
in the existing order and within finite poll budgets. Exhausted work budget
ends this staging opportunity. Seal on first Durable, any queued control/timer/
background boundary, visit deadline, aggregate exhaustion, close, quiesce or
failure. No private busy loop, compulsory fill wait, checkpoint-frequency
change, new deadline, host lookahead or default ready-quantum change.

When Core retires the window, validate its complete ordered IDs. Promote one
designated anchor continuation to the runnable visit and attach the one final
effect sequence; hold all other frames' credits until that continuation,
including a possible ordinary commit Persist and existing leased sends, drains.
Then retire each frame and release each original input usage exactly once.
Application/router pending commands persist through their normal takeover.

Native retention tracks exact units/counts per group, not a boolean group set.
Polling one terminal releases only that request's worker credits. Written does
not. Worker ready_window keeps its existing no-wait FIFO gather; sequentially
append each gathered request to accepted storage, then barrier the exact ticket
union and project exact per-unit terminal completions. Never remove WAL sync,
manifest write/file sync, rename or directory sync. A syncing A barrier has a
closed dependency set; B/C queued during it can share the **next** barrier.

Replica submission splits the data FIFO at its first duplicate group, then
boundedly submits successive requests under existing poll/effect work ceilings.
Preserve control-first scheduling. Stop on overload and restore the refused
prefix to the **front**, byte/order/lease exact: appending it behind B/C would
reorder dependent revisions. No retry can silently consume another original.

## Failures, close, recovery and cleanup

Pre-submission refusal retains original input or the exact rejected lease for
retry. A provider Failed fences the affected group/window for recovery in this
first slice, preserving which units were never submitted, accepted, Written,
known rejected or uncertain. A later B failure cannot erase accepted/durable A.
Drain already accepted worker requests to terminal observation; do not roll back
ancestors or announce NotProposed merely because a wait was canceled.

Close/quiesce seals and drives owned completions/effects before closing workers.
Abort reports unresolved outcomes and requires explicit recovery; shared host
resources belonging to other instances are untouched. Restart reconstructs the
coherent existing WAL/manifest/log/application prefix and new session domains,
never volatile window IDs or authority. Reject all late old completions.
Checkpoints, snapshots, reclaim, term/vote/configuration/read/transfer/control
events wait for the finite window and anchor continuation to drain.

## Checks and measurement decision

Core: serial differential histories;2/3-unit staged chains; exact bytes/IDs/
positions; unchanged rejection input/state; aggregate/peak arithmetic; first
Durable/control seal; no partial canonical/quorum advance; one final commit
plan; foreign/stale/duplicate/out-of-order tickets; preserved early request/
alias and ACK before local Durable. Runtime/provider: parked credits and exact
continuation lookup; front-restored overload FIFO; other-group/control progress;
default-one and opted-in downstream workers; default-false/context-sensitive
application refusal/fallback; canceled waits and close/drain/abort accounting.
Native: actual A syncing while B/C queue, next-barrier B+C, every existing
sync/publication failure boundary, recovery/retry/dedup and real TCP/QUIC
three-service exact-original histories. Process-crash tests are not power-loss
proof. Update component/contract inventory and downstream conformance ledgers.

Root format/four strict profiles, affected exact tests, separate matched checks
and full enabled pre-push hook must be zero. Commit/push the complete checked
slice without waiting for CI. Fresh asserted native artifacts then compare
within-host matched configuration: explicit pipeline off/on, same ready quantum,
application, offered load, originals, maintenance, workers, storage and completion
boundaries. Record actual completed originals/s, latency distributions/refusals,
accepted units/originals per physical barrier, storage/owner debt and memory,
drain/recovery, every raw failure/tail and source/tool/artifact hashes. Sample
CPU separately from clean timing; use compact text/JSON/folded stacks only.
No browser or kernel-policy changes. The required full curve, both-host healthy
margins, sustainable memory/debt/headroom/fairness/skew/placement and P8
composition remain open; many-group aggregate cannot replace one-group floors.

## File ownership during implementation

Core agent: raft/window/completion/proposals/early replication and minimal
crate-private transition view accessors, with focused Core tests.
Runtime agent: worker/native worker, shard/timed/owner/ready/router plumbing,
ReplicaDriver submission and correlations, with focused runtime/worker tests.
Root: application capability, ReplicaDriver construction validation, native
startup/call sites, benchmark selection/readback, contracts/conformance and
coherent integration/qualification. Coordinate shared replica.rs edits explicitly.
Benchmark-only transparent ObservedLog will correlate successful append tickets
with original-command counts in a bounded table capped by selected pending-unit
limits. On a successful explicit barrier, record its units, known originals and
largest same-group unit count in fixed0..64 plus65-or-more histogram bins;
record dropped/missing correlations separately and refuse a complete-amortization
claim if either is nonzero. No changed provider result, retained user payload,
unbounded event log or extra durability authority. These counters supplement
the existing sync/publication timing; startup's file-only reader cannot by itself
establish same-group logical dependency sharing. New CLI selection/readback must
validate actual Core/worker settings and join constructed workers on failure.
Successful LogStore barrier calls are logical dependency events. The benchmark
also records their exact nested sync/publication call deltas, separates1/1
synchronized,0/0 no-I/O and ambiguous calls, and counts same-group multi-unit
calls only when correlated to one sync and one publication with complete ticket
metadata. Histogram65 means65-or-more. Require count agreement for any physical
per-barrier interpretation; startup's file-only reader reports that it lacks
these dependency observations. Recovered owner escrow is checked before any
worker/connector starts; an initial-bootstrap minimum arithmetic check rejects
impossible fresh selections before I/O. Recovery may still advance files/sessions
before its actual state-size check, under the ordinary explicit cleanup contract.
The source fragments CORE_PLAN.md/RUNTIME_PLAN.md are supplementary rationale;
this unified schema controls any conflicting proposed spellings or semantics.

Focused integration correction: repeated data submissions within one poll are
enabled only for an explicitly multi-unit worker. A default-capacity-one worker
retains one data-batch attempt per poll, including existing independent-group
work. The batch unit/byte bounds, exact accepted identities, refused-prefix FIFO,
control priority and close/failure ownership are unchanged. This scopes the new
submission cadence to its declared capability; it does not raise a limit or
alter persistence authority. Before changing it, the candidate's first native
hundred-group snapshot progress timeout reproduced twice on Btrfs with the
original15s deadline; exact published469 passed the same condition. That is a
regression signal, not proof of this cadence's individual causal share. Check
the same failed control first after the correction, then remaining failed
histories and actual selected-pipeline TCP/QUIC controls; keep every old failure.

Measured admission correction, before editing: profile16's whole debug test
launch places71.79%/70.22% of selected baseline/candidate CPU sample weights
under Shard::reserved_connections. Both profiled cases exceed the unchanged
progress limit; these are overlapping inclusive CPU samples, not elapsed times
or a pipeline speed claim. The default submission-cadence correction did not
resolve the unprofiled failure. Rebuilding the full owned connection union for
an incoming event that adds no new required pair is the next measured target.

Data/API shape: factor the existing event-payload peer walk into one
crate-private borrowed visitor shared with the unchanged public
Raft::event_connection_replicas reference. Add a boolean-only eligibility check
at Shard::check_connection_event, with no cached result, revision, digest,
generation or new authority. Every advertised pair must exactly match retained
history (or the exact local identity), and every nonlocal pair must also match
the current provisioned set when present. Inactive history alone is insufficient:
route replacement can legitimately remove a historical peer. Scan existing
unfenced groups' scalar local-node/binding/Core-fence guards. A Core may be
fenced through with_core while its Group remains live; that guard cannot be
omitted. Check incoming group/target as well. Any failed guard, unknown pair,
conflict or provisioning mismatch takes the original complete union path,
preserving its error order. Eligibility itself never emits a new rejection.

Transitions/ownership: queue admission already validates the owned union;
queued events are immutable and private. Register, budget/route replacement and
with_core retain their full validation. Accepted Core peer history is monotonic;
failed steps can expose only peers from the already-reserved event. Stopping a
group removes its queued ownership while history remains. Append windows contain
commands only and introduce no new peer identity. Thus a fully covered incoming
event cannot enlarge that validated union. Apart from the volatile
incomplete-callback guard described below, no new cached comparison state
survives a call, so restart and cleanup use the existing reconstruction and
ownership rules.
No membership, transport, durability, fencing or authorization check is removed.

Required callback correction found during the invariant audit: parked
with_persist_unit(Some) must run the same post-callback full connection validation
and history retention as with_core. A public completion callback can replace its
Core, so command-only staging does not by itself establish this invariant.
Factor and reuse that existing guard, including the Core-fenced early result and
stop-on-validation-error behavior, before enabling the covered-input shortcut.
Test an early parked completion callback that introduces an unknown/conflicting
peer; it must retain the ordinary guarded outcome and fence the invalid group.

Acceptance: compare fast eligibility and fallback against the existing full
reference through known repeated snapshots/configurations, full capacity,
unknown/competing queued peers, store/incarnation/local/target conflicts,
history-retained but unprovisioned peers, another live group's fenced Core,
register/stop and budget/route replacement. Preserve the public event walk's
intermediate configurations and all snapshot/repair/authority forms. Rerun the
actual first failing Btrfs history with the original15s limit, then the other
failed histories and affected checks. Actual selected-pipeline TCP/QUIC histories,
all strict checks and publication still precede any performance qualification.

Caught callback unwind correction, before editing: each private runtime Group
gets a volatile `connection_validation_pending` bit, initially false. Exact
ordinary or parked mutable Core access sets it immediately before invoking the
host callback. Only successful full post-callback union validation and history
retention clears it. A caught panic leaves it set; the existing Core-fenced
early-result path also leaves it set. No panic interception, return-value change,
revision/equality shortcut, provider authority, or persisted state is added.
Covered-input eligibility requires every live group (including the target) to
have no incomplete callback validation; otherwise the original full reference
path remains mandatory. A later successfully checked callback may clear the bit;
stop continues to fence/remove original queued ownership. Tests use an ordinary
and a parked callback that replace Core then panic, catch the unwind in the
fixture, and verify exact resource refusal/original ownership. Ordinary repair
then full validation must restore eligibility, including a callback returning
an ordinary Result error. This is bounded mutable-access exception safety, not a
new performance claim or a new panic/reuse contract imposed on embeddings.

The independent Core visitor acceptance controls additionally assert exact
expected pair order and early-error prefixes for Configure Joint, LearnerRepair
Joint, and granted/refused AuthorityReply. They supplement the shared runtime
differential oracle; all inspect resources only and grant no protocol authority.
