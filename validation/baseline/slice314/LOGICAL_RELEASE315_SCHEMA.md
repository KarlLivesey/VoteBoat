# Proposed315 mandatory logical release boundary

Read-only proposal; no production implementation or qualification is claimed.
This supplements `target/slice314/PLANNED_NATIVE_SCHEMA.md`. The scope is the
authenticated, non-Byzantine host/provider composition already selected there.
It does not sandbox a malicious application or a host independently running raw
Raft and publishing invented business values.

## Existing contracts and the exact gap

- `runtime/effects.rs:582`: public `take_effect` returns public-field
  `EffectLease`, including the raw Send. Rejecting its eventual release after an
  outbound queue accepted it is too late to gate that response.
- `runtime/effects.rs:687`: public `complete_effect` and
  `complete_effect_with` run caller callbacks on `&mut Raft` and return their
  generic results. Logical mode must refuse before callback execution, not
  merely before clearing the lease. `validate_lease` currently checks original
  ticket and effect kind, not full same-kind payload equality.
- `runtime/replica.rs:608`: `retry_send` transfers the message before calling
  `release_transferred_send`. `retry_committed` submits/polls application results
  and immediately calls `ClientRouter::deliver`; `retry_read` executes and
  publishes a read in the same visit.
- `runtime/applications.rs:430` and `runtime/reads.rs:338`: public poll/complete
  expose original opaque receipts/results and release their charges. Their
  result getters already expose values after polling. Guarding only the higher
  client/read convenience router leaves this lower path open.
- `runtime/read_requests.rs:511`: `execute` takes the original query, executes
  it once through `ReadRouter`, consumes that result and immediately publishes
  `ReadOutcome::Read`. A gate after Node polling cannot undo publication.
- `runtime.rs:94`: `RuntimeOwner` is a public identity tuple. All four router
  binding structures are publicly constructible identity tuples as well.
  Equality of those IDs is necessary correlation, not sealed logical authority.
- `runtime/snapshots.rs:394` legitimately completes an original snapshot job
  using `complete_effect_with`. Making public generic callbacks physical-only
  therefore also requires a private, named snapshot completion path.

## Construction and authority binding

Introduce one small opaque owner authorization binding, minted by the owner
assembly, not by a public identity-only constructor. Its inspected identity is
the existing `RuntimeOwner`; its private variant is Physical or Logical. The
logical variant contains the verified mapping/incarnation, logical parent node,
backing child identity, representative epoch/session and fresh release-owner
generation. It is produced only by the validated logical startup/claim path.
Restoring or replacing the representative never reuses that generation.

The runtime/owner retains this mode below `ReplicaDriver`. A plain physical
`EffectOwner::new` cannot downgrade a logical-marked runtime/core assembly;
unsupported or inconsistent construction refuses before intake. The logical
assembly consumes the verified construction authority and binds its actual
child bridge and selected persistence/snapshot workers. A boolean mode argument
or matching public store/node IDs is insufficient.

Mint application/client/read-execution/read-invocation bindings from this owner
authorization. Keep readable identities/generations, but prevent arbitrary
struct-literal construction of a logical binding. Router constructors and every
cross-router preflight compare both identity and authority variant/generation.
An ordinary physical binding with the same public IDs must fail logical owner
checks; a plain router constructor must reject a logical binding unless the
logical assembly installed its coordinator. This requires a small constructor
API migration, not a global identity registry or an adapter-controlled gate.

The child protocol alone creates an opaque, non-cloneable `ReleasePermit` after
consuming the original child Applied output or resolving that exact original
outcome through a fresh quorum read. It contains its original child
owner/ticket/operation/outcome position and the exact immutable capsule bytes.
Private construction does not itself prove durability: permit issuance must
validate actual retained child protocol output and original command content.
An admitted command, local sync, write receipt or caller-supplied certificate
never mints a permit.

The capsule binds release owner; mapping/parent group incarnation; logical node;
representative fence/session; original visit/effect; parent term and configuration;
original request/response kind; exact parent payload/prefix context; and applicable
router/result/client/read tickets, original operation/command positions or full
read barrier and query/result encoding. A batch capsule must bind every covered
receipt and its canonical original position. Historical committed capsules
remain exact-request scoped: a later fence does not manufacture authority for a
new request, owner generation or payload.

## Private capture followed by authorized publication

Use one bounded coordinator with three private owned pending variants: Send,
Applied batch and Read. Each retains its original lease, captured typed values,
encoded capsule and exact original child invocation. Public inspection may expose
phase/tickets/usage, never a pre-permit successful payload. No raw replay of a
business callback is needed to regenerate an observation.

**Send:** a private owner take/capture path retains the exact original message
and effect ticket. Build its bounded capsule from that message and the retained
parent context. No ordinary queue sees the message until its original capsule
has committed. The coordinator then transfers the authorized message/frame to
the selected bounded outbound queue and clears the original lease only after
its matching accepted queue ticket. Queue refusal retains the original message,
permit and charges for retry; it cannot turn a failed submit into sent evidence.
Same-kind changed content must refuse. An explicit private drop of an unsent
expired background snapshot is cancellation, not successful release.

**Applied batch:** keep existing application preflight, contiguous application
and receipt/position validation. Reserve typed-result and encoding credits before
applying. Apply once and retain the original `ApplicationResults<R>` privately
while its original Committed lease remains held. Do not enqueue ordinary ready
results or call the public owner release yet. After the batch's exact capsule
permit, a private checked completion clears the original Committed lease and
the existing client delivery validation consumes the original result. It uses
the original receipts, operations, positions and retained command correlation;
the codec never constructs replacement receipts. Already cancelled client waits
may consume original internal results without publishing Applied. Otherwise all
covered local successes require the exact permit before pending requests become
terminal or receive successful outputs.

**Read:** preserve original read invocation/query ownership. Encode the query
while it is still owned, reserve both original-result and encoding allowances,
then privately validate/consume the actual `ReadBarrier` and execute `read_at`
once on the serialized owner. Store the original `ReadResults<R>` plus full
barrier (group/configuration/term/request/index) and applied application context.
The owner records a captured-read state while retaining the visit/effect lease;
permit completion clears that state without calling `finish_read` or `read_at`
again. A private read publication consumes the exact typed result only after its
capsule permit. Leadership-changed, cancellation and pre-execution admission
refusal keep their existing non-success behavior. Retaining a captured
`Result<R, ApplicationError>` preserves the actual application error; do not
silently replace it with success, a fabricated durable refusal or another query.

## Direct public API rules in logical mode

The ordinary public `take_effect`, `complete_effect`, `complete_effect_with`,
`release` and `release_transferred_send` refuse before payload transfer or callback
execution. Returning the original rejected lease remains the ownership contract
where the API takes one. The private coordinator uses named capture, authorized
completion and no-success discard paths; it does not call a public bypass with a
temporarily disabled mode. Physical mode keeps its current behavior.

Public `ApplicationRouter::submit`, `ReadRouter::submit`, `ClientRouter::deliver`
and `ReadRequests::execute` cannot publish logical success through their ordinary
paths. Check mode/binding before taking a query, applying a batch, consuming a
result or invoking a callback; return the original owned inputs on rejection.
Only named private capture/publication paths enter the logical transitions.
Public application/read poll queues contain only authorized outputs, so their
public complete methods cannot extract pre-permit originals. Completion must
still validate mode-aware original result bindings and outstanding ownership.
Client/read successful completions enter their public queues only after permit;
their existing poll/complete then retain original one-use credit semantics.

`cancel_read`, failed-owner discards and cancellation observation may remain
public where they produce no successful payload and validate exact original
ownership. A cleanup flag cannot authorize positive release. Named snapshot and
persistence delivery paths continue checking their actual worker/output binding;
they can update the parent core only from the selected child-backed providers.
Any resulting Send passes the same private capture gate. No public generic
callback is needed for that internal completion.

## Small bounded response codec

Existing allocation traits do not serialize values. Add a capability required
only for applications used in logical mode, alongside their existing traits:

- immutable format identifier/version selected at construction;
- an encoded receipt-batch upper bound from the original entries, available
  before apply;
- a query encoding bound and read-result encoding upper bound from the original
  query, available before it is consumed;
- receipt/query/result encoders borrowing the actual values and writing into a
  caller-provided bounded byte slice, returning checked initialized length.

The engine encodes the fixed context/ticket/position/barrier envelope and the
application-error tag/content canonically. The capability encodes application
values; it is not a generic serialization framework or a requirement for physical
applications. No new Clone bound is needed. The coordinator stores the original
typed values and the once-encoded exact bytes for retries. Checked arithmetic
includes both live typed/router charges and encoded/header/container charges.
Entry/query preflight reserves capacity before apply/read or child admission;
oversized output or encoding failure after callback consumption fences and
requires explicit cleanup/recovery instead of executing that callback again.

The trusted application codec contract requires deterministic, faithful encoding
of its semantic values. Generic runtime metadata cannot independently prove
injectivity of arbitrary downstream Rust values. Conformance fixtures validate
the selected counter and a downstream application. Protocol-level conflicting
original action reuse compares the entire encoded capsule and refuses; it does
not rely on an unverified content hash or a different newly encoded receipt.

## Failure, boundedness and acceptance

Reserve a pending slot, encoded byte allowance and terminal output allowance
before child admission. Keep separate control/recovery capacity so saturation
cannot prevent consuming already accepted original work. Overload leaves
uncaptured original work intact; no unbounded side queue or blocking consensus
call is introduced. Close stops intake; accepted mutations/capsules/results keep
their credits until actual terminal consumption. Cancelled observation produces
Unknown/unavailable, not timeout Abort or forgotten child work. Provider errors
fence new positive production; cleanup polls and consumes actual owned child
outputs and joins selected workers. Stale/cross-owner permits never free another
pending action's credits. A partial publication failure retains the original
result/permit until explicit completion or failed-owner recovery; publication
ordering cannot consume a permit and then lose the only retained successful
value before the bounded public completion is installed.

Required controls before claiming315: direct physical API calls on logical
owners/routers refuse before a queue/query/app callback counter changes; forged
public identity equality cannot construct/downgrade the sealed authority; raw
logical Send/results do not appear in public pre-permit polls; a defective queue
or driver substitution cannot bypass capture; stale/wrong-content/wrong-kind/
cross-owner original permits refuse without freeing credits; admitted/local-sync
child work cannot publish; actual child quorum loss holds responses/results;
actual Applied and original fresh-quorum resolution release each result once;
original wait cancellation drains without successful publication; oversize and
conflicting codec/action controls preserve ownership; counter and downstream
codec paths behave identically; all physical histories remain valid. Exercise
persistence-free ReadAck/PreVote alongside ordinary writes and local reads.

This proposal does not qualify the child application, authenticated envelope,
whole-child recovery or native three-by-three composition itself. Those remain
the explicit315 dependencies and acceptance histories in the parent schema.
