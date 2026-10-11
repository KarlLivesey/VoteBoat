# Proposed native qualification harness315

Read-only preparation while314 is frozen. Nothing below is implemented or
qualified. This plan instantiates target/slice314/PLANNED_NATIVE_SCHEMA.md and
docs/LOGICAL_VOTERS.md; it introduces no alternate consensus engine, capsule
authority, or transport authentication. Native315 completes one fixed level;
nesting, layer activation and serialized lifecycle histories remain316.

## Minimal actual topology and identities

One parent group has three stable logical leaves L0/L1/L2. Each leaf maps to a
different child group C0/C1/C2 with three actual ordinary child Raft voters. Use
nine independently owned child Nodes, one group per Node, rather than three
multi-group Nodes: the latter is valid shared-WAL composition but couples all
three child groups when withholding a Node's poll and cannot isolate an inner
quorum outage. The nine voter contexts are co-located on three fixture physical
identities, not a claim of nine machines or independent failure domains.

Each mesh uses physical NodeId1/2/3, with distinct StoreIdentity per(child group,
physical node), own root/endpoints/WAL/snapshot worker and runtime owner. Store
IDs and group/incarnations disambiguate these independent physical meshes. Do
not count those physical identities as additional parent leaves. Parent logical
NodeIds and logical StoreIdentities are separate from physical representatives;
each parent's store identity persists across representative claims while its
live store session, runtime owner and epoch have their exact scoped lifetimes.

Reuse the three existing TLS credentials for their three physical fixture hosts,
with explicit per-mesh peer NodeId/StoreIdentity/certificate/server-name pins.
tests/support/tls.rs::configuration configures credentials independently of store
identity; NativeStartup supplies exact pins and NativeTlsSession validates the
actual hello against the expected PeerIdentity. No dynamic certificate service
is required. Reserve TCP and UDP candidates outside the ephemeral dial pool using
the existing fixture pattern, then open real production listeners. Every child
and parent transport retains its own actual authenticated session binding.

Parent networking needs the proposed explicit logical envelope: authenticated
outer physical from/sender/to plus inner parent logical leaf/store, mapping,
request/capsule context and format capability. Existing WireScope::matches
(src/wire.rs:26) requires Message.from/sender/to equal the physical scope;
PeerAssignments::build (src/transport/membership.rs:58) requires core local node
and store equal physical local identity. Rewriting Message.from under those
checks is invalid. Parent construction/ingress must select the new checked
logical mode; ordinary child Nodes keep the existing physical format. Unsupported
format/mapping must refuse before opening any worker or protocol work.

## Host-owned composition and pump

The host owns child Nodes, three parent Nodes and one bounded bridge for each
logical leaf. Workers hold queue handles, never mutable references into their
own parent Node or the child cohort. A synchronous worker submit transfers the
original PersistUnit/SnapshotWork only into reserved owned queue capacity.
The explicit host pump then submits original commands/queries to actual child
Nodes, polls them with supplied monotonic time and bounded NodePollBudget,
consumes opaque completions, and makes exact parent provider events available.
The next parent poll consumes them. No trait call waits for network consensus,
recursively polls another runtime, or opens a parent fallback WAL.

Use generic Node<S,T,E,A,W,O,H,C,F> provider seams for the parent bridge worker,
snapshot provider and authenticated envelope transport. NativeNode is only the
ordinary child fixture alias. Existing Node bounds are ReadyScheduler,
TimerService, ElectionEntropy, ProposalAdmission + BoundedReadableStateMachine +
CheckpointStateMachine (receipt ApplicationReceipt), PersistenceWorker,
OutboundQueue, SnapshotWorker, PeerConnector with Clone endpoint, and its
PeerTransportFactory. A separate native-convenience-only driver would prevent
the required public host-provider composition. The public logical constructor
must bind logical authority below the pump; substituting a queue cannot opt out.

Retain each parent visit/effect/provider request and its original child node
slot, RuntimeOwner, ClientTicket/ReadInvocationTicket, operation, exact encoded
command/query, mapping and epoch. Node::poll_client/poll_read output is opaque;
compare original ticket and full scope, then consume with complete_client/read
on that same Node. ClientOutcome::Applied exact content or a consumed fresh
quorum ReadOutcome::Read resolving the original retained action may authorize
the dependency. A diagnostic local application getter, child admission,
local Written/sync, transport delivery and arbitrary index cannot do so.

Written and Durable parent WorkerEvents may conservatively follow the same
original child Applied outcome, in that order, with parent LogTicket fields
from the retained validated parent mutation, not the child index. Preserve the
original visits and worker binding. Child indices identify child provenance;
parent log revision/generation/commit/application indices remain independent.
The backing application uses public log::apply_batch, not a new log transition.

Every successful parent Send/client/read needs the exact child response capsule,
including responses without persistence. Existing ReplicaDriver::retry_send
(src/runtime/replica.rs:608) releases directly after OutboundQueue admission;
retry_read (:728) directly executes ReadReady. Public EffectOwner release,
ClientRouter::deliver and ReadRequests::execute must enforce logical authority
as well. Retain original opaque results/leases and their credits before capsule
admission; capture and publication are distinct. Test direct public calls and
host-provider substitution, not only the convenience pump. Refusal/cancellation
can consume ownership without publishing success.

## Lifetime capacities and startup

Propose a deliberately small fixed fixture envelope: parent commands at most
1KiB, parent application snapshots at most4KiB, bounded retained parent entries,
actions/capsules/pins and concurrent inputs. Exact limits must be derived and
validated by encoded-size/retained-capacity checks before construction; these
numbers are proposed fixture bounds, not yet a verified production configuration.
All parent update/batch/capsule commands plus metadata must fit the selected child
client/log/wire ceilings; the entire backing application checkpoint and fresh
quorum image must fit child snapshot/read/transport envelopes with reserved
recovery/control capacity. Bound allocations by Vec capacity as existing contracts
require. Include an exactly accepted boundary and first over-limit refusal.

Concrete current obstacle: default child command is64KiB in LogLimits,
ClientRouterLimits and WireLimits; default parent command is already64KiB and
wire snapshot512KiB. Full default parent payload plus child envelope therefore
does not fit. NativeStartup currently selects default log/wire limits internally.
For the first selected harness, constrain parent limits explicitly and reject
unsupported combinations. Supporting larger production envelopes requires
checked caller-selected native limits or a separately specified payload staging
protocol; no silent truncation or ad-hoc chunking in the fixture.

Start ordinary child Nodes from NativeStartup::open_with_protocol_and_timers
or prepared selected parts. Recover each child's actual own checkpoint and WAL
suffix before election. Obtain original consumed fresh child-quorum parent image,
claim the checked representative under latest accepted child eligibility, and
verify parent GroupLog/checkpoint/application pair before constructing its core.
Only the proposed verified recovered-image LogStore view can feed parent recovery;
append/barrier on that synchronous view refuse. No local child application getter
or in-memory expected state may become the recovered parent store.

## Histories and causal controls

Run each history with TCP/TLS and QUIC, real file workers and monotonic caller
polling. The following are acceptance assertions, not completed evidence.

1. Campaign the parent through the actual logical envelope, write X then Y,
   consume original applied client results and fresh quorum reads. Retry identical
   original operation/content and observe original outcome; conflicting bytes
   and mapping/epoch/request identity refuse without a new success. Assert parent
   leaves count once, child positions differ from parent indices, and every
   released response has its own original consumed child capsule dependency.
2. Retain a real produced child output without consuming it. Parent durability
   and success remain unreleased and credits remain charged. Consume its exact
   original output to progress. Lose observation/cancel wait, then resolve only
   the immutable original action via a new actual quorum read. Reject actual
   cross-node/cross-session/stale output while keeping newer ownership intact.
   Direct public release calls without the matching permit must refuse.
3. Isolate two original owners in one child mesh by withholding their polls,
   retaining their real sockets/WAL/ownership. Poll the survivor and the other
   meshes. A new parent storage action and persistence-free read response at that
   leaf cannot obtain child commitment; no durable/success event appears. Restore
   polling, consume original fresh quorum outcomes and finish the exact request.
   This is actual unavailable quorum, not merely healthy reply withholding.
4. Replace a representative after an actual parent vote/prefix and lost result.
   Keep the old owner/output scope, claim a new eligible representative, obtain
   fresh child-quorum image and reconstruct its own retained promises. Original
   old-session responses cannot authorize fresh requests or free newer credits.
5. Publish a parent checkpoint with full child-owned bytes/pin; then mutate the
   exact parent anchor. Attempt early anchor and reclaim first and require
   refusal. Checkpoint the child's complete application using NodeControl::
   Checkpoint; add a committed child suffix with later parent promise/write/
   capsule while preserving the old child's checkpoint boundary. Reclaim ordinary
   child WAL only through its original owned maintenance ticket if needed.
6. Stop and join all three owners of one child, discard their runtime caches and
   completion slots, and reopen only their own files in NativeOpenMode::Recover.
   Reestablish actual child consensus and a fresh quorum image before enabling
   the parent; verify original parent term/vote, exact X/Y bytes, committed prefix,
   snapshot pins, suffix generation and retry/outcome identity. Native315 must
   distinguish unsynchronized-tail loss through an actual subprocess power cut;
   graceful close/reopen is labelled cold whole-child restart, not crash proof.
   Missing bytes/image/quorum returns unavailable, never empty bootstrap.
7. Exercise a downstream bounded application/response codec and public host
   worker/queue composition through the same trusted Node/release contracts.
   Oversized or conflicting response encoding returns original values and
   ownership, with no capsule or parent success. A host codec supplies bytes,
   never commitment or a fabricated permit.

Declare which restart path is cold versus abrupt in evidence. If the bridge
borrows actual local child Node handles, place the entire host assembly in one
subprocess rather than splitting a child behind an invented remote bridge. The
controlling test retains only original caller operation IDs and actually observed
acknowledgements; it obtains no authority to supply an image, capsule or ready
flag. Terminate the host at a captured actual phase, reopen all original files
in a new host, and query/retry using original IDs through the real assembly.
This loses all child runtimes plus parent caches and qualifies whole-child
reconstruction; report that it is an assembly-wide cut rather than selective
one-child failure. Ordinary fixture control stdin/stdout may request phase pause
or report observed progress, but never inject committed state. Exact abrupt
orchestration remains an assembly choice; graceful recovery is labelled cold.

## Failure, cleanup and usable path

Close parent intake first; continue pumping accepted mutations/capsules and
consume retained client/read/provider outputs. Stop waiting abandons observation,
not committed work. Provider failure fences parent phase production; preserve
original accepted requests for explicit recovery rather than returning rollback.
Keep child providers alive until the parent bridge relinquishes every dependency.
Then use actual parent Node drain/into_parts or into_recovery, close children,
poll actual retained protocol deadlines, reclaim WAL/snapshot workers, finish
connector dialers and join every owned worker. Rebind TCP/UDP endpoints to prove
release; remove files only on an explicitly successful disposable test finish.
Never join another embedding's shared resources or drop accepted ownership to
make teardown pass. Error paths preserve the first error and actual files.

Proposed test location after314 publication: tests/logical_voter_native.rs with
child modules assembly/drive/history/recovery/ownership, required tls and quic
guards. Reuse small fixture patterns instead of including the entire100-group
support target or copying its consensus behavior. A usable Rust embedding should
expose the same explicit bounded pump/construct/close path; no second executable
service is necessary to prove315's fixed cohort. Root registers metadata/docs and
owns integrated Linux/macOS four-profile qualification and publication.

## Reuse source map and limitations

- tests/transaction_driver/cluster.rs: distinct real group leaders, NativeStartup
  configuration, original ticket/barrier comparison, bounded wait/checkpoint,
  native connector/WAL/snapshot joins and endpoint rebind.
- tests/transaction_driver/recovery.rs: actual old checkpoint plus later committed
  suffix, retained original phase identity, whole-cohort cold reopen.
- tests/transaction_driver/unavailable.rs: actual unpolled owners, no new quorum
  authority, retained durable state, repair and original resume.
- tests/support/native_node.rs and native_checkpoints.rs: generic facade provider
  composition, original maintenance tickets, checkpoint/replay100-group evidence.
- tests/support/peer_driver.rs uses synthetic session/connector controls; reuse
  ownership assertions only, never its fake sessions as native315 evidence.
- tests/routed/native.rs: real separate child mesh startup, TCP/UDP reservation,
  typed shutdown diagnostics, proposal/read exact outcomes and joined workers.
- src/runtime/node.rs: propose/read/poll/complete/cancel/control/from_parts,
  normal drain and recovery ownership; src/worker.rs and snapshot_worker.rs:
  original accepted work, event ordering and fixed lifetime reservation contracts.

Concrete missing production capabilities are the child-backed application/bridge,
constructor-bound release authority and response codec, verified recovery view,
and negotiated logical/physical envelope. Existing providers implement actual
child consensus/network/durability; they do not already enable logical parents.
This plan does not claim native315 evidence or implement316 nesting/membership.
