# Proposed315: child-backed parent assembly

**Planned, not implemented or qualified.** This advances P8 logical voters after
the314 independent protocol gate. The current deliverable is one usable fixed
parent cohort;316 still owes supported recursion and layer membership histories.
No measured performance/default-promotion or security work begins here.

## Data and authority

The deterministic backing-child application retains one exact logical parent
store mapping, its `GroupLog`, representative epoch/session, complete parent
snapshot bytes and pins, immutable pending action/outcome identities and bounded
retry floors. Apply the existing public `log::apply_batch` validator to parent
mutations; do not introduce another parent log-transition implementation. Child
`LogEntry` indices only locate original child commands/outcomes. Parent revision,
generation, accepted membership and application/checkpoint indices remain separate.

Define typed Claim, parent mutation, snapshot publication/retention and response
capsule commands. The child application validates full mapping/epoch/content and
ordered prerequisites atomically. Proposal-context admission also checks the
actual child core's latest accepted representative eligibility. Replay of a
historically committed command follows its original ordered child context; a
later configuration must not retroactively erase a promise. Conflicting action
reuse refuses, and lost observations query the exact retained original outcome.

Use existing StateMachine, ProposalAdmission, bounded receipt/read and checkpoint
contracts. A replacement image is obtained through an original consumed child
quorum read, not a local application getter or caller-supplied ready flag.
Construction validates the complete mapping/DAG, incarnations, codecs, parent
command/snapshot envelope and reserved recovery capacity before starting work.

## Runtime and ownership

The backing-child Nodes retain their selected ordinary WAL/snapshot workers and
real authenticated peers. The parent persistence/snapshot providers use the same
public worker contracts and one explicitly host-owned bounded child bridge.
Their synchronous submission transfers original work into owned queues; an
explicit caller-driven pump submits/polls the actual child Nodes. No trait call
silently blocks on network consensus, creates another runtime, or opens a
fallback parent WAL.

Retain full original Node/ticket/operation/command/query and parent visit/effect
scope until terminal consumption. Only actual child Applied or an exact original
outcome resolved by a fresh quorum read authorizes completion. Emit parent Written
before Durable; both may conservatively wait for Applied. Child admission, local
sync and transport delivery alone produce no parent evidence. Snapshot Published
also waits for retained full child-owned bytes/pin, and older pins survive until
the exact parent anchor is durable.

All parent Sends and successful local client/read outputs pass protocol-owned
capsule release. In particular, `runtime/replica.rs::retry_send` currently transfers
Sends directly to OutboundQueue and `retry_read` executes ReadReady directly;
wrapping only PersistenceWorker would leave the known cached ReadAck defect.
Retain the original effect/result lease through the asynchronous capsule outcome.
Reserve outputs before child admission, and preserve physical-mode behavior.
Mechanism substitution cannot disable logical mapping/context validation.

Bind logical authority at construction below the convenience driver. Existing
public `EffectOwner::release_transferred_send`, `ClientRouter::deliver` and
`ReadRequests::execute` otherwise permit direct publication by an embedding.
In logical mode, successful release requires the matching protocol-owned permit;
the plain physical path cannot construct a logical owner or accept its binding.
Refusal/cancellation may consume original ownership without publishing success.
Test direct public calls as well as the named logical driver: replacing a queue
or bypassing a convenience wrapper must not turn off this invariant.
In particular, refusal after `take_effect` has already exposed a raw Send, or
after `complete_effect_with` has called an arbitrary core callback, is too late.
Logical mode must refuse these physical entry points before exposure/callback.
Its private capture path retains the real original leases and router outputs;
public application/read result polling does not export logical success early.
Sealed construction authority binds this mode; matching public identity structs
is insufficient. This protects the supported assembly contract under the existing
trusted non-Byzantine host/provider model, not a sandbox against a hostile host.

Source review also confirms that `ApplicationResults` and `ReadResults` are opaque
owned values, whereas `BoundedReadableStateMachine` supplies only allocation
bounds. For logical mode, an explicit bounded response codec must encode the
original query/result/receipt into the capsule's exact application context.
Retain the original values and their existing router charges while that capsule
is pending; encoding does not construct a replacement result or receipt.
This additional capability is necessary to bind generic application content,
not a general serialization framework. Physical applications need not implement
it. The selected native counter and a downstream application must exercise the
same contract, including oversized and conflicting content refusal. Read capture
and public publication must become separate transitions; placing a gate after
`Node::poll_read` would already have published success.

## Authenticated ingress and startup

Existing WireScope, PeerAssignments and ordinary ingress bind physical
Message.from/sender to the authenticated session. A logical sender can differ
from that physical representative. Add an explicit negotiated envelope carrying
both identities and the exact capsule/request context. Preserve the outer actual
TLS/QUIC peer scope; validate the mapped inner parent scope before core ingress.
Do not rewrite an ordinary Message under a physical scope check. Unsupported
assemblies/versions refuse before opening work. Historical exact packets cannot
authorize another request/session/term or count the physical sender as another
logical leaf.

A named verified recovered-image view exposes the child-owned parent log and
snapshot to existing compacted/member recovery. Synchronous LogStore mutation and
barrier methods on that startup view refuse; they cannot manufacture durability.
Recover the backing child's own checkpoint and synchronized suffix first, verify
the parent log/checkpoint/application pair, and only then enable its representative.
Missing required bytes or inner quorum means unavailable, never empty bootstrap.

## Failure and acceptance

Close stops intake while accepted mutations/capsules/outputs drain. Cancellation
abandons observation, not a committed promise. Epoch/session replacement cannot
release a newer action's credits. Provider error fences phase production and
returns original ownership for explicit recovery/drain; the host joins selected
workers without dropping another user's resources. Actual whole-child restart
must reconstruct commitment through ordinary child recovery;314's surviving
child-runtime model assumption is not sufficient native evidence.

Required native histories on Linux/macOS: three logical parent voters each backed
by three real child voters; parent election/write/read/original retry; child
quorum loss and repair without fabricated response; representative failure with
retained parent vote/prefix; persistence-free response fencing; old parent and
child checkpoint plus surviving suffix; whole-child restart; exact stale/cross-
identity completion and payload refusal; bounded cleanup and joined workers.
Exercise TCP/TLS and QUIC, and public host-provider composition over the same
contracts. Run formatting/four strict all-target Clippy profiles at zero. Fixed
one-level qualification does not complete nested activation or all P8.

Source review: `src/log.rs` GroupLog/LogUpdate/LogStore/apply_batch;
`src/worker.rs` original PersistUnit/WorkerEvent;
`src/snapshot_worker.rs` SnapshotJob/publication dependency;
`src/application.rs` deterministic context/admission/receipt/checkpoint contracts;
`src/runtime/replica.rs` effect release;
`src/runtime/node.rs` original child output consumption;
`src/wire.rs` WireScope;
`src/transport/membership.rs` physical PeerAssignments;
`src/runtime/ingress.rs` physical session ingress;
pack chapters03/04/07/11/16/17 and `docs/LOGICAL_VOTERS.md`.
