# 480: reuse exact admission reservations inside one ready prefix

Before production edits.479's independent raw Mac CPU-delta traversal finds
Counter::validate_proposal in41.807% of observed interval weight:26.410% from
ready execution and15.397% from submission. Reserved-set insertion and applied
dedup lookup dominate resolved admission stacks. The captured31:1 skew retains
2024 hot/six cold pending at the horizon, unlike Linux's offer-capped65-pending
capture. Unknown ancestry27.914%, interval-to-current-stack bias (including wait
frames), whole-launch scope and debug0 remain explicit. These are sampled
contexts, not additive CPU timers or an expected wall-time gain. Target the
largest supported removable ready-stage work; submission remains separate.

## Data/API and ownership

Extend public ProposalAdmission with optional `prepare_ready_admission` returning
`Option<ReadyAdmission<'_>>`, default None. `ReadyAdmission` is a named boxed
FnMut(OperationId, &[u8]) -> Result<usize, ApplicationError> capability, borrowed
from immutable application state. Its constructor consumes the exact pending
iterator; the returned context must own any retained input data and cannot borrow
temporary iterator items. It validates one original at a time, returns the same
receipt bound/error as the existing validator, and incorporates successful
predecessors. It mutates only temporary validation state, performs no I/O and
grants no application, admission, authentication or durability authority.

The router owns one private lazy context within one synchronous advance call.
Every empty selected prefix resets it. The first candidate uses the unchanged
per-command validator; only a nonempty prefix attempts preparation, once. This
keeps singleton execution unchanged and avoids constructing a context for a
candidate that already failed before any batch exists. Default None selects the
original path for the rest of that prefix. A context is never reused across an
empty prefix or group/Core transition, and is dropped before application mutation,
return or error. Closure-local allocation may remain after collection until the
next empty-prefix callback, but that callback resets it before any new validation.
Explicitly drop it after owner advancement before processing returned steps.
Borrowed application state remains serialized; no revision cache, global owner
or restart watermark is introduced.

At preparation, include the same unapplied log, all retained router pending and
otherwise-unrepresented selected predecessors, in the same order as today. Do
not reserve only the proposed subset: queued/in-flight originals still consume
capacity. Identity, leader, configuration callback, per-original Core-context
check, applied-boundary guard, original receipt reservation and whole receipt
bound checks remain in their existing order for every candidate. No outcome,
position, original byte comparison or callback is removed.

Counter's context builds the exact reserved OperationId BTreeSet once, using the
unchanged full applied dedup lookup and configured remaining capacity. It retains
an initial pending-capacity error rather than emitting it during construction:
each candidate still checks its8B syntax first, then that pending error, then its
own exact operation reservation/capacity. Do not consume beyond the first pending
capacity failure. Candidate byte conflicts remain original application outcomes,
not a new admission policy. Keep existing validate_proposal unchanged as oracle.
The executable Counter wrapper forwards the capability and maps the successful
bound to its actual wrapped receipt size; maintenance/context-dependent apps
retain default fallback. No provider is required to opt in.

## Transitions, partial progress and cleanup

Unprepared -> unchanged first validation -> optional Prepared/Fallback for the
second candidate -> sequential successful-prefix checks -> Drop. Any failed
candidate ends that selected prefix and its temporary context is discarded,
including failure in subsequent per-command context/receipt guards;
unchosen originals retain existing queue/admission ownership. First-candidate
errors still use the original step/error path. Context checks, receipt failures,
new term/configuration, protected control, byte/item/visit budgets, persistence
errors, owner fencing, cancelled waits and retries retain existing behavior.
The next empty-prefix callback drops prior state before another validation.
Return/abort/error drops everything by ownership. Restart constructs nothing
from this temporary state; normal verified log/checkpoint/application recovery
remains authoritative.

Memory is bounded by one Counter box and the same configured pending-reservation
set the old single validation already built. The executable wrapper uses a second
constant-size box to map the successful receipt bound, sharing that one set;
there are no per-original contexts or retained history copies.
The router must not accidentally hold a context through application
execution. Public optional semantics and host implementation/conformance scope
must be recorded in the contract inventory and review ledger, without claiming
full certification of all downstream applications.

## Acceptance

Differential Counter controls compare sequential context checks to the unchanged
validator over the exact growing pending prefix: duplicates, differing bytes,
already-applied IDs, capacity edges, malformed candidates, pending overflow,
successful predecessors and drop/reprepare after replay/checkpoint restore.
An independent downstream host context must exercise the same public API. Runtime
controls count zero preparation for singleton, once per real prefix, fallback
behavior, reset across groups/steps, mid-prefix refusal and unchanged individual
positions/outcomes/receipt budgets. Preserve protected traffic, cancellation,
term/configuration and actual service retry/recovery controls.

Run affected application/client/provider suites, actual TCP/TLS+QUIC services and
standalone matched checks as scoped by changed inputs, then formatting/four strict
profiles/standalone checks through the enabled hook. Never weaken a threshold.
Commit and push the complete checked slice, including every new source module.
Fresh asserted native artifacts from that published commit and paired Linux/Mac
measurements follow; do not label this implementation a gain before measurements.
No wider batch, persistence redesign, gathering timer, default change or rejected
fusion/pipeline is bundled into it. Preserve full P9 and pending ext4/XFS native
acceptance; current native rates are separate from framework floors.
