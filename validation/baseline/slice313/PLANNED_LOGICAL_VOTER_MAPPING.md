# Proposed314: replicated logical-voter refinement

**Proposed and unimplemented.** Read-only design review; no source edits, builds,
tests or git operations. Functional protocol/model work belongs to P8. Measured
comparison with hierarchical aggregation, normal/default promotion, striping and
relay tuning remain P9; security review remains P10. This proposal does not claim
that ordinary Raft tests prove the extension.

## Smallest complete composition

Use one parent ordered Raft group with three logical voter identities. Each
logical voter is backed by a real three-physical-replica child group whose
application stores that logical parent's replica state. One active physical
representative drives the parent's unchanged term/log/quorum rules from this
replicated state. The child's native WAL/snapshots are the durable base; there
is **no physical parent WAL, fallback store, or second authoritative parent log**.
The parent application's checkpoints are data in this child application.
Parent runtime caches are reconstructed copies, not independent durability.

The construction-selected parent PersistenceWorker and SnapshotWorker use the
existing public contracts. They submit owned child commands and correlate full
original child ClientTicket/ReadInvocationTicket, content, operation and consumed
Applied/quorum-read result. Native child Nodes are one specialization of generic
public provider composition. Child workers remain ordinary selected providers.
Recursive nesting is an inductive extension over an acyclic, construction-checked
bounded depth; the first native assembly can demonstrate one level, but cannot
claim arbitrary-depth native qualification from that alone.

**Necessary scope discovery:** worker substitution alone is insufficient.
`raft/receive.rs:406–434` can emit ReadAck without a Persist when the parent term
is unchanged. A former representative could therefore answer a newly invoked
read from cached parent state after the child has fenced it and voted in a newer
parent term. All parent protocol Sends require a child-committed exact response
capsule before release, including otherwise persistence-free responses (and any
future PreVote/check-quorum implementation). Logical service success must also
respect the live representative gate; local Node liveness or cached leadership
cannot authorize a new read or client result. This is protocol-owned release
logic, not permission for an arbitrary outbound provider to assert success.

## Checked data and identities

Keep distinct types for logical voter, parent group/incarnation, backing child
group/incarnation, stable logical store identity, physical representative
NodeId/StoreIdentity, physical secure session and committed representative epoch.
Existing parent policy leaves/Core NodeId may be an explicitly validated internal
projection of logical IDs; physical transport identities are never silently
substituted for them. A durable mapping binds each parent logical leaf to exactly
one child authority and logical store incarnation.

The child application retains:

- exact parent Bootstrap/GroupLog, including hard term/vote, BallotOrigin,
  accepted contiguous suffix, revision/generation, commit boundary, effective
  configuration/history and snapshot membership;
- complete required parent entry payloads and parent application checkpoint
  metadata/bytes, durable logical references and retained pins;
- current representative fence: monotone epoch, physical identity/session scope,
  authorized backing-child configuration and original claim operation;
- immutable action identity/content, expected representative epoch and parent
  revision, canonical retained outcomes and exact response/request context;
- bounded recovery/dedup/rejection floors needed to prevent reused actions or
  superseded requests from manufacturing evidence after checkpoint/compaction.

A response capsule binds parent/logical-store/child identities, representative
fence, parent term and configuration, full requester origin/session/sequence,
request and response kinds/content, and relevant previous/matching index+term,
payload or checkpoint identity. The child validates it against its canonical
parent state and current fence. A claimed high index alone is insufficient.
The physical child index is provenance, never a parent commit/log index.

## Ordered transitions and effect ownership

1. Establish the child group through existing authorized creation. Commit its
   logical-parent bootstrap/mapping before any parent campaign or response.
   Missing durable child state is an error, never an empty replacement voter.
2. A successor commits ClaimRepresentative(expected epoch, next epoch, exact
   eligible physical identity). Restore canonical parent state through actual
   child quorum observation before enabling its parent runtime. Old-epoch new
   actions are refused by the child application, including pure responses.
3. For parent Persist, submit the original LogUpdate with epoch, expected revision
   and original action identity. Reuse shared `log::apply_batch` transition
   validation. Child admission/Written/commit intention produces no parent
   durability. Only the actual consumed child Applied permits an exact logical
   LogTicket and DurableLog. Emit WorkerEvent::Written before Durable; both may
   conservatively wait for Applied. Do not label child volatile admission Written.
4. Parent `Raft::admit_effect`/`complete` consume their original scoped tickets
   and produce normal effects. Then commit exact response capsules for any Sends
   before releasing them to authenticated transport. A simple first assembly can
   serialize one parent's event → persistence → capsule → release; two serial
   child decisions are acceptable. Predicting a completion on a clone must never
   substitute for an actual child result. Pure Send paths go through the same gate.
5. A representative transition rejects old attempts against the new fence. Lost
   observations resolve through actual fresh child status and exact original
   content. No timeout supplies a vote/ack, deletes a promise, or chooses a new
   store. A stale child/worker/parent session cannot release the new runtime's
   original effects. Previously emitted packets remain historical messages and
   must still match the exact live parent request/term/configuration context.

The authenticated outer envelope must preserve the real physical TLS/QUIC peer
binding while naming the separately validated logical sender and child capsule.
Existing `WireScope` and PeerAssignments assume physical from/sender identity;
an explicit logical ingress/egress protocol is required rather than rewriting
Message.from underneath that check. Core ingress occurs only after validating
the mapping, representative/capsule scope, destination and request context.
Non-Byzantine authenticated providers remain the trust model; checksums or a
caller-supplied capsule are not a new cryptographic certificate. Parent terms,
configuration, candidate freshness and matching-prefix checks remain mandatory.

## Checkpoint, payload and restart mapping

Publish a parent checkpoint only after the child commits the full bytes plus
metadata/pin. SnapshotOutput::Published uses a stable **logical** SnapshotRef,
not an unrelated physical child's snapshot identity. The parent then commits
its original snapshot anchor LogUpdate through the same child-backed worker.
Only that anchor authorizes later reconciliation/reclamation. Preserve an older
pinned parent checkpoint until the replacement anchor is committed. Transfer or
recovery cannot count a digest whose parent payload/checkpoint bytes are absent.

A physical child's own checkpoint must contain the logical application state,
fence, parent payload/snapshots, pins and action floors. Its real checkpoint plus
committed suffix reconstructs that state before the new representative starts.
Restore the parent's selected checkpoint, replay its own surviving committed
suffix and recover membership at the correct accepted-log boundary. Never feed
the model oracle, another parent's image or a newest diagnostic index into
recovery. Process/power cuts distinguish accepted/written child bytes from the
actual native synchronized base. If required data/quorum is unavailable, pause
or explicitly refuse; do not recreate an empty voter or fall back to local WAL.

`LogStore` synchronous barrier/state methods cannot simply hide asynchronous
child consensus. Live persistence belongs to the async worker contracts. Startup
needs an explicitly verified recovered-image view (or a named recovery API) of
the child-owned log/snapshot pair; any synchronous read-only adapter must reject
unsupported mutations and cannot pretend a volatile mirror is durable. Existing
compacted recovery verifies log+snapshot+application before election, and must
be retained rather than bypassed with the uncompacted `Raft::recover` API.

## Parent and child membership

Keep the two transitions distinct. An existing backing child uses ordinary P4
learner readiness and joint/final activation while preserving its logical voter
and all parent promises. Representative eligibility/fence must follow the
child's accepted/committed configuration correctly; an old removed physical
representative cannot continue responding from a cached parent core.

The parent changes logical leaves through its own validated joint/final protocol.
A new child-backed leaf must have the complete required parent payload/checkpoint
and identity before it becomes necessary for the parent quorum. Parent final
commit cannot discard data needed by retained joint history or outstanding
responses. Reconfiguration of both layers is serialized by explicit replicated
intents/phase dependencies in the first implementation, not by editing local
maps. Qualification must cover child joint transition + representative failure,
parent joint transition + snapshot/suffix rollback, and the selected coordinated
overlap rules. Blocking overlap is acceptable only if declared and enforced
before admission; it is not evidence that arbitrary overlapping transitions work.

## Formal refinement and independent finite model

Abstract state is one crash-recoverable ordinary parent voter per logical ID,
with durable term/vote/accepted contiguous log/configuration/checkpoint and
volatile parent role/request state. Refinement projects **committed and applied
child application state with available bytes** to that voter's durable state.
Child admission, local write, uncommitted commands, physical traffic and rejected
capsules are stuttering steps. A committed parent mutation is one abstract
storage transition; a committed exact response capsule authorizes the matching
abstract response. ClaimRepresentative is a crash/restart of volatile parent
ownership with identical durable promises. Physical representative replacement
never creates another abstract vote. Quorum trees count logical IDs once; child
votes count only in their own group. Inductively apply the relation at each
selected depth with an ordinary native durable base.

Use an independent bounded model, separate from production transition helpers.
Record the actual state/action/search bounds and any frontier truncation. Minimum
matrix: three parent logical voters; three physical child voters per cell (a
fourth learner for one child membership transition); two parent terms/candidates,
two conflicting parent entries, two representative epochs, small request/action
queues and explicit old/joint/new configuration states. Add a new logical cell
for parent membership cases rather than relabeling an existing cell's promises.
Explore separated finite profiles if combined exploration is too large; state
which combinations remain unqualified.

Actions cover propose/write/sync/child commit+apply, lost/duplicate/reordered
completion, request/response capsule release, representative fence/failover,
physical process/power restart, partitions/inner quorum loss, suffix replacement,
parent and child joint/final activation, parent snapshot publication/anchor,
child checkpoint+suffix restore, payload loss and bounded close/cancel.

Independent oracles check single logical vote per term, no conflicting parent
commit, contiguous matching durability, original release dependency, exact request
and identity freshness, current joint predicates, recovered acknowledged payload,
parent application dedup, no checkpoint boundary without state, no duplicate
logical counting, and bounded credits under inner quorum loss. History comparison
uses original acknowledged operations, never oracle restoration.

Required weakened controls must produce actual counterexamples: worker-only cached
ReadAck after fencing/new parent promise; local-written child ack before sync/
commit; representative counted as another vote; missing payload with matching
digest; old epoch/suffix completion releasing new effects; loss of parent vote on
representative replacement; new-only activation before joint commitment; and
checkpoint anchor advancing before recoverable bytes. The cached-read trace
starts the read after a newer parent write completes, so it cannot be excused as
an earlier overlapping read. Finite model evidence is not a general proof.

## Finite implementation order and remaining dependencies

314 establishes checked mapping/action/capsule schemas, refinement arguments and
the independent model plus weakened controls. 315 implements the child application,
child-backed workers, logical authenticated release/ingress, recovery and actual
native fixed-cohort history through public providers. Then qualify failover,
nested checkpoint/suffix and declared child/parent reconfiguration cases against
actual synchronized native storage on Linux/macOS. A fixed one-level demo is
independently usable but does not complete the unqualified remaining P8 promises.

Bound all payload/checkpoint/action/capsule counts and capacities at construction;
reserve control/recovery credit. A first profile may require each bounded parent
image to fit one child command, rejecting incompatible assemblies before intake.
Do not silently omit snapshots. Response retention must use specified monotone
request/session floors and exact current retry outcomes, or a declared bounded
lifetime refusal; unbounded heartbeat/response history is not acceptable. These
are functional data/ownership obligations, not a performance-tuning workstream.

Source basis: pack chapters03/04/07/11/16/17; `log.rs:61–220,284–318`;
`raft.rs:123–133,348–391,1066–1148`; `raft/receive.rs:123–159,406–450`;
`worker.rs:20–187`; `snapshot_worker.rs:25–167`; `snapshot.rs:31–154,325–444`;
`wire.rs:21–29`; `transport/peers.rs:917–953` and
`transport/membership.rs:34–99`; `runtime/replica.rs:25–98`;
actual Node client/read completion contracts reviewed for313.
