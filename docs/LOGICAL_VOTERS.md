# Logical voters: protocol gate314

**Model-qualified proposed protocol; no native logical-voter implementation is claimed.**
The refinement and independent finite models are qualified in
[slice314](../validation/baseline/slice314/README.md). Native
assembly follows in315, then supported nesting and configuration qualification.
Performance comparison and default promotion remain P9. Ordinary physical-voter
Raft stays supported. Recursive quorum policies already implemented in VoteBoat
count physical voters; they do not implement this extension.

## Identity and durable authority

One logical parent voter is backed by a real child consensus group. Its stable
logical voter/store identity differs from its physical representative, physical
authenticated session, child identity and representative epoch. The parent keeps
one ordered log and its ordinary term, freshness and quorum rules.

The child application owns the parent's replica image: bootstrap, hard term/vote,
ordered entries and payloads, accepted configuration history, suffix generation,
commit boundary, parent application checkpoint, snapshot references/pins and
original response contexts. Its own child WAL/checkpoints are authoritative.
The parent's runtime cache is a reconstructed copy, never another durable store
or an empty fallback when the child is unavailable.

Construction checks the complete parent/logical-store/child mapping, distinct
logical leaves, exact incarnations, formats, resource envelopes and acyclic
nesting/depth. Unsupported combinations refuse before workers or protocol work
start. A first fixed native assembly has three parent logical voters, each backed
by three physical child voters; that alone will not qualify all nested profiles.

## Refinement relation

Let `C` be concrete child state and `V` one abstract crash-recoverable parent
voter. Define `R(C, V)` by projecting the **applied committed child image with
recoverable referenced bytes** into the parent voter below. Child indices remain
provenance; they are never substituted for parent log/commit/application indices.

| Concrete state | Abstract parent state or transition |
| --- | --- |
| Exact child-owned parent/logical-store incarnation | One distinct voter and durable store, counted once |
| Retained parent hard term, vote and ballot origin | The same durable promise under ordinary parent rules |
| Ordered parent entries, terms and complete bytes | The same accepted matching prefix and suffix generation |
| Surviving parent configuration records and checkpoint membership | The effective latest accepted old/joint/new policy |
| Published parent checkpoint bytes, metadata and retained pins | The same recoverable parent checkpoint and surviving suffix |
| Applied child mutation with its original action identity | One abstract storage transition |
| Applied exact response capsule with original context/dependencies | Authorization of that same abstract response |
| Committed representative replacement plus verified reconstruction | Abstract volatile crash/restart preserving durable promises |
| Admission, local write, synchronization without child commitment, observation loss, stale refusal | Stuttering: no new parent durability or response authority |

This relation assumes the qualified ordinary parent Raft rules and ordinary
child consensus contract. It does not prove either engine afresh. Required
preservation obligations are: every admitted mutation keeps the relation; every
released parent effect has the matching applied child dependency; replacement
reconstructs the same promises; and child recovery preserves acknowledged bytes.
Finite models exercise specified instances, not an unbounded proof.

For supported recursion, apply the same relation at each checked DAG depth until
reaching a physical durable base. Induction requires that every lower level
already satisfies the same durability/release/recovery contracts. The constructor
must reject cycles and insufficient envelopes; a one-level test cannot substitute
for this argument or selected nested native histories.

## Action and response ownership

An action carries immutable ID/content, parent/logical-store/child incarnations,
representative epoch and physical session, expected parent revision/suffix
generation and exact original effect/request identity. Duplicate content retries
the original outcome; conflicting reuse refuses. Accepted inputs, observations,
packets and snapshot pins remain bounded and owned until their original terminal
consumption or explicit recovery handoff.

For persistence, submit the original parent mutation to the child. Admission or
local Written is insufficient. Only its consumed original child Applied result
can produce the exact parent worker completion. A conservative implementation
may emit Written and then Durable after Applied. It must retain the original
scope and dependency bytes rather than manufacture a new ticket from a high
child index.

Persistence-only gating is insufficient: ReadAck and other responses can have no
parent storage effect. Every parent Send and locally successful service response
therefore needs a child-committed **response capsule**. The capsule binds full
parent/logical-store/child identity, representative fence, parent term/effective
configuration, requester origin/session/sequence, request/response kind and exact
matching prefix/payload/checkpoint context. Child application validation compares
it with the canonical image and fence before publication.

Historical packets may arrive after a representative transition, but remain
usable only for their exact existing parent request and applicable term/policy.
A capsule committed for an earlier read cannot authorize a fresh read invoked
after a newer completed write. No receiver is assumed to learn a remote fence
instantaneously. Fresh local service success needs the same request authority,
including the leader's own logical evidence.

The wire envelope preserves the authenticated physical sender while separately
naming the mapped logical sender and capsule context. Rewriting `Message.from`
under a physical-only scope check is invalid. Enabling this extension requires
an explicit negotiated capability/format and logical ingress/release checks;
implementing an ordinary transport trait alone does not enable the protocol.
The fault model remains authenticated non-Byzantine providers, not new homemade
cryptographic certificates.

## Representative and membership transitions

A successor first commits an exact claim against the prior representative epoch,
then reconstructs the canonical parent image through a fresh child quorum result.
Only then may it enable the parent runtime. New claims/actions require eligibility
under the child's latest accepted policy. Accepted suffix rollback recomputes
eligibility. A capsule's ordered committed application also validates the child
transition preceding it; local applied configuration does not replace core
accepted-policy timing. Old exact historical packets remain independently scoped.

The child changes physical membership with its ordinary learner/joint/final
protocol. The parent changes logical leaves through its own joint protocol; a
new backing child must retain the complete required parent image and original
identity before becoming necessary for the parent quorum. Neither transition
turns a physical representative into another logical vote.

The first supported overlap policy serializes layer membership changes through
one **control-only parent lifecycle intent**. Its immutable operation, destination,
expected configurations and phase are parent replicated state. Competing intents
serialize at that ordered log; separate flags or a local mutex are insufficient.
Admit the child transition only after the original parent intent is authoritative.
Loss/restart resolves that intent and the exact child outcome through quorum reads.
Release only after the selected final outcome is durable. Ordinary child business
writes require no parent intent or ancestor commit. Arbitrary overlapping layer
transitions are unsupported until separately specified and qualified.

## Checkpoint, recovery and failure

Parent snapshot publication requires full child-owned bytes/metadata/pin before
the parent anchor mutation becomes durable. Retain the old pin until its valid
replacement anchor is committed. A digest or reference without recoverable bytes
cannot authorize publication, acknowledgement, import or reclamation.

The child's own checkpoint includes the entire logical application state,
representative fence, parent payload/checkpoints and retained action/request
floors. Restore that checkpoint plus its own surviving committed suffix first;
then verify and restore the parent's log/checkpoint/application pair. Never
restore from an oracle, another voter's image, a newest diagnostic index or an
unverified volatile mirror.

Live child consensus is asynchronous. A synchronous LogStore barrier must not
hide network work or invent durability. Startup needs an explicitly child-verified
recovered-image view; runtime persistence/snapshots use their actual owned worker
contracts. Missing image, bytes or inner quorum means unavailable/refused progress.
Cancellation stops waiting, not a committed promise. Provider errors fence phase
production; recovery/drain returns original ownership and joins selected workers.

## Required evidence

Five independent finite profiles cover promises/response release, payload and
nested checkpoint recovery, child activation, parent activation and serialized
overlap. They separate each physical replica's written/synchronized records from
child commit/application and volatile observations. Independent oracles import
no production transition/policy helpers and never supply recovery images.

Report exact state/edge/depth counts, frontier/cap flags, selected graph assumptions
and positive terminal witnesses. Each weakened guard must produce a real replayable
counterexample: worker-only stale ReadAck; local-written acknowledgement;
representatives counted as extra votes; missing payload; stale completion reuse;
vote erased on replacement; new-only activation before joint commitment; and
checkpoint anchor before recoverable bytes. Safe counterparts must refuse the
missing dependency and progress after it is supplied. Canned oracle-failure flags
and fabricated committed images are not evidence.

The separated models use bounded command workloads and explicit ordinary-child
consensus assumptions. A synchronized physical prefix supplies bytes, not by itself
knowledge of commitment. A selected representative restart must first obtain the
committed boundary from a fresh child quorum and then decode its **own** synchronized
checkpoint and suffix. Profiles that retain surviving child consensus authority
across one physical crash do not qualify simultaneous loss of every child runtime.
Catch-up-before-next-request and serial membership schedules must be reported as
graph bounds; exhaustion of those graphs does not exhaust their omitted combinations.
Native acceptance still owes actual whole-child restart and the supported nesting
and lifecycle histories.

Model qualification precedes native315. Actual native acceptance must additionally
cover original child results, parent writes/reads/retries, representative failure,
inner quorum loss, old checkpoint/suffix recovery, context refusal, public host
composition and real drain/joins on Linux/macOS. Supported nested and parent/child
activation histories remain316; remaining transaction scope also keeps P8 open.
