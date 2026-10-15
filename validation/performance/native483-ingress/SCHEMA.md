# 483: exact temporary admission reuse across a synchronous ingress scope

Before production edits.482's current480 loaded Linux recording attributes
57.003% of native cycle-period weight to exact Counter validation; the submission
monomorph alone is55.257%, versus ready execution5.083% and reconciliation19.528%
in overlapping contexts.94.874% of reserved-set insertion leaf-period and93.655%
of applied-map lookup leaf-period have that submission validator as direct caller.
The candidate reached4096 pending.480 removed ready-prefix rebuilding, but every
ingress submission still builds the same exact reservations again. Target that
remaining measured algorithm, rather than the smaller few-live reconciliation
shortcut. No allocator or end-to-end gain is inferred from these samples.

Mac's different moderate QUIC profile has reconciliation19.846% and admission
0.040% of observed CPU deltas; its793ms p99 does not reproduce481's extreme tails.
Those interval/current-stack samples include wait-frame bias and do not identify
the precise cause of individual delays. Preserve the tail regressions and the
separate reconciliation audit. This slice is not presented as their proven cure.

## Smallest usable API and composition path

Add optional `ProposalAdmission::prepare_proposal_admission`, default None,
returning the named `PreparedAdmission<'app>` exact temporary validator. Keep
`ReadyAdmission` compatible as an alias to the same boxed FnMut signature and
keep `prepare_ready_admission`: its default delegates to the new general method.
An existing host overriding only the ready method remains ready-only; ingress
must not silently opt that host in. Counter and the executable Counter wrapper
implement the general capability using480's existing exact set/error algorithm.
The unchanged full Counter `validate_proposal` remains the reference. Maintenance
and other providers retain their existing fallback unless explicitly opted in.

Expose a scoped `ProposalIngress` capability with two methods: `ready_leader(group)`
returns the live leader-role/current-term-commit predicate for diagnostic routing, and `submit`
returns the existing individual ClientTicket/ClientRejected with original request.
Readiness is not authorization, a lease, commitment, or linearizable-read evidence.
`ClientRouter::with_submission_batch` lends that capability to a synchronous
callback over the supplied owner and application lookup. `Node` provides the
same scope, preserving ordinary draining refusal before other request checks.
Normal `submit`, `propose` and privileged `propose_maintenance` remain available;
no new maintenance privilege or network request envelope is added.

The capability borrows the router/owner exclusively and application state
immutably for the call. It exposes no owner mutation, poll, advance, delivery,
configuration, application mutation or returned long-lived cache. Only the
existing per-original tracked admission may mutate the owner. No worker result
can be delivered, Core transitioned or application applied through this scope.
An admitted provider failure can still fence the owner; every subsequent
original must recheck the current binding/failure/role normally. Read-only readiness queries observe the current Core instead of a cached
leader selection. A fenced owner may retain Leader role; submit independently
rejects fencing. Source review found the old benchmark also requires a committed
entry in the current term, so the capability preserves that exact predicate. Provider-supplied prepared capabilities retain their coherent
immutable-state obligations; default None remains the safe unchanged fallback.

The public methods use a stack-scoped capability/callback, not an allocated
request/output batch. The caller keeps each result immediately and remains
responsible for observing every original. The existing client/owner limits still
bound all accepted requests, bytes and receipt credits. There is no gathering
timer, speculative persistence, new worker, batching default, format or durability
token. Record the new operations/capability and partial conformance scope.

## Exact cache scope, state transitions and error order

Keep at most one context for the current contiguous ordinary group/application
run. The first original uses the unchanged full validator; only after successful
tracked admission may a second original prepare, once. Cache default None for
that run. Reset on group/application identity or applied-index change, protected
control, missing application and every failed check/admission. Application
identity comparison only distinguishes borrowed objects; it is not a state-
equality shortcut. The exclusive owner/immutable application scope, repeated
guards and absence of transitions are what make retained validation coherent.

Preparation receives exactly the old submission iterator: unapplied durable Core
commands followed by all retained group router commands, in the same order.
Earlier successful submissions are already registered there. Do not replace
this with just the current proposal subset or a count/digest. A prepared callback
must give exactly the full validator's receipt bound/error while incorporating
successful predecessors. It cannot retain borrowed iterator bytes; pending and
immutable application lifetimes stay independent. Counter retains exact owned
OperationIds and full applied-state membership, preserving syntax-before-capacity
errors and original-operation content/outcome behavior.

Per original keep the old order: closed/binding/fenced/group/leader, command size,
control classification and request-capacity preflight, applied boundary,
application Core-context check, exact proposal admission, receipt minima/maximum,
owned byte/metadata charge, final capacity and sequence checks, tracked owner
admission, then router indexes/credits. Node still checks draining and application
presence in its original order. Never skip authentication/context or receipt
checks merely because the application validator is prepared.

The context may reserve a candidate before a later charge/admission fails. Drop
the whole context on any such failure; the next original restarts from the full
validator/current retained state. Rejected IDs must not consume later capacity.
The scope takes its entire temporary validator into a local before each lookup
and submission, leaving its retained slot empty. Only a complete successful
admission restores it. A provider/lookup panic caught inside the same callback
therefore drops every partially changed reservation before a later original.
Successful predecessors remain admitted/charged with their original individual
tickets; there is no whole-batch rollback. A control original gets normal legacy
checks and ends the ordinary prepared run before and after its admission.

Unprepared -> successful first admission -> Prepared/Fallback -> sequential
individual admissions -> reset/drop. Normal return, early callback return or
panic drops temporary state without removing accepted ownership. Original
cancellation, failure/fencing, lost observations, retries and shutdown keep their
existing paths. Restart reconstructs nothing from this cache; verified durable
log/application recovery and original tokens remain authoritative.

## Actual embedding/benchmark use, without changing arrivals

Use the new Node capability in the native offered benchmark's existing
at-most64 due-offer loop. Borrow all three nodes in nested scopes and preserve
their index order. For every original, read the clock at its existing decision
point, check the same offered/global/per-group window, select the current leader
via each scope's read-only readiness and immediately submit/record the individual
result. Do not precollect requests, precharge future offers, cache leader choice,
reorder groups, delay for more work, widen the64-decision quantum or move
maintenance/output polling/timer boundaries. End all scopes before the existing
maintenance step or next node poll. This makes the public Rust embedding path
usable without a new transport protocol or harness-only shortcut.

Existing network service single-request entry points remain unchanged; actual
TCP/TLS+QUIC retry/replacement/recovery controls still run. New scoped embedding
controls must exercise real three-voter durability and per-original outcomes,
including mixed groups and a downstream application implementing the same public
prepared contract. No snapshot/WAL/replication capacity or cadence is changed.

## Acceptance and publication

Compare scoped submissions against repeated legacy submit on equivalent valid
owners/applications: duplicates/conflicting bytes, malformed/oversized inputs,
capacity/receipt/sequence boundaries, fallback and ready-only host providers,
group/app changes, protected controls, runtime refusal/fencing, partial success,
retry after failure, empty/singleton scope and callback unwind. Assert pending
inputs and every individual result/order/charge; ensure constructors are zero
for empty/singleton and once per eligible run; fallback returns no allocated
context but its factory is attempted once to discover None. Exercise dropping before
application/owner mutation and ordinary Node draining. Keep the original Counter
oracle unchanged and480 differential controls.

Run affected application/client/provider/Node suites in all/core configurations,
actual three-service TCP/QUIC and new scoped native histories, matched workspace,
contract metadata and negative controls, formatter/four strict profiles/standalone
through the enabled hook. Fix causes with focused reruns and exact scope reuse;
never weaken lint/checker thresholds. Commit/push every new module and checked
slice without waiting for CI. Fresh asserted published artifacts, scoped Mac
qualification and paired480/483 measurements follow before any gain claim.

Full P9 remains active and incomplete. The native third filesystem is pending;
all seven sustained framework cells/both hosts, healthy margin, bounded memory/
debt, durable headroom/fairness/skew, placement/shared barriers and P8 composition
remain. FileLogIo/ready16 defaults stay;8,422,500 originals/s is a framework minimum,
not a ceiling. No P10, Windows, browser, kernel-policy or Pi work.
