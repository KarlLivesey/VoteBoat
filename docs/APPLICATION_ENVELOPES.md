# Application deployment envelopes

StateMachine::deployment_requirements optionally reports the selected provider's
whole configured lifetime schema, maximum command bytes and maximum checkpoint
bytes. Bounds include retained retry outcomes and lifecycle metadata. They must
remain valid across admission, application, checkpoint and restore; using the
current occupancy or current checkpoint length is insufficient. Providers perform
no I/O or state changes while computing or validating these bounds.

The default returns None. Existing downstream applications can continue ordinary
static operation; configuration execution and positive learner readiness require
an explicit capability. The shared validate_deployment_requirements check rejects
missing/zero actual bounds, a different application identity (below) and
declarations smaller than the reported envelope. Larger compatible byte
declarations are accepted, subject to the selected storage/wire/snapshot
providers' independent capacity checks.

## Compared identity

`ReadinessRequirements` names every layer of the application stack: the outer
`application_schema` and `identity`, then `inner_profile`, one layer per
wrapped application, outermost first (at most seven), and the stack's
deployment plan. A layer identity is the protocol family
(`raft::ApplicationFamily`, whose constants are the registry of in-tree
families) and four configuration words. The deployment plan
(`raft::DeploymentPlan`, at most 192 words, carried in the profile) holds,
per layer that declares one, the exact words of bounded construction inputs
that decide committed entries but are not limits: the groups, voters, stores
and operations an application was built for, and a transaction participant's
opening image. `InnerProfile::wrapping` carries
an inner layer's plan one layer deeper, so every wrapper that uses it
propagates the plan. Readiness compares schema, family, configuration and plan
of every layer exactly (`UnsupportedSchema`), so two families that reuse
schema numbers, or two replicas configured differently, refuse each other
before any entry. Only `command_bytes` and `snapshot_bytes` are storage
reservations, accepted when the declaration is at least the actual bound. No
hashing is added: the identity is fixed width, and a plan that does not fit
overflows the profile, which readiness refuses rather than truncating. (The
bootstrap gates below compare exact bytes, except where a family's binding was
already a SHA-256 anchor before DR1: the metadata source, target, serving and
publishing bindings and `RetirementGuard`'s retirement anchor. Those gates are
exact up to SHA-256 collision resistance; DR1 adds no digest.)

**Per-stack budget.** The 192 plan words are shared by every layer of a
stack. Example: `LogicalActivationApplication` around a logical voter is
type-valid, but activation's own segment is 4 + 7m words for m mappings (116
at 16) and the largest logical voter's is 174 (a candidate with a 16-mapping
control profile), so that stack needs 290 words. Its profile overflows and
readiness refuses it for every replica (`UnsupportedSchema`, `Capability` at
the verifier); nothing is truncated. A logical voter's segment is 41 words
for a bootstrap voter without a control profile, 12 more for a candidate,
plus the profile's words when it has one. Host rule: size such a stack so
that the sum of its layers' segments is at most 192, for example fewer
activation mappings or no control profile on the inner voter.

**Bootstrap bindings (DR1 round 7).** Some construction inputs are too large
for the identity but are compared exactly by the family's committed bootstrap
entry: a Routed grant, local group and initial inner checkpoint, a directory
plan, a target, namespace or metadata binding. Readiness attests them through
the learner's applied state: `StateMachine::deployment_bound` is `true` only
once every such entry of the application and of each layer it wraps has
applied, and positive readiness is refused before then
(`ReadinessError::BootstrapPending`; the native exchange replies not ready).
That is sufficient because each family applies its bootstrap only when the
committed bytes equal its own binding (otherwise `Err(InvalidCommand)`, or for
`TransferTarget` an `OperationConflict` that does not stage it) and its restore
compares the checkpoint's binding with its own exactly: a replica whose flag is
set holds the binding of the committed entry, so every replica that gets a
receipt agrees on it. The check is a read of existing applied state; no
storage, wire field or byte declaration changes, and lifetime declarations do
not depend on it. A declaration that is incompatible on the identity is still
refused first (`Capability`).

| Family | Flag (`deployment_bound`) | Why it attests the binding |
| --- | --- | --- |
| `RoutedApplication` | `initialized`, and its inner application's | bootstrap applies only as the exact binding (grant, local group, limits, inner schema and initial checkpoint); checkpoint decode compares the binding |
| `TransferSource`, `ScopedTransferSource` | the Routed layer's | their own exact bootstrap embeds the Routed binding; a raw Routed bootstrap is refused |
| `RoutedControlReads` | the Routed layer's | no bootstrap of its own: it forwards the raw Routed bootstrap unchanged, and Routed compares it exactly |
| `TransferTarget` | `staged`, and its inner application's | only the exact binding stages it; restore compares the binding |
| `CreatedNamespace` | `ready`, and its owner's | only the exact namespace binding makes it ready; restore compares it |
| `Directory` | `initialized` | only the exact bootstrap initializes; restore compares authority, limits and every manifest |
| `LogicalApplication` (logical voter), bootstrap voter | parent image created (`parent` present) | the first claim (no expected epoch, the only claim that can create the parent image) carries the exact parent-bootstrap bytes; decode compares them with the replica's own, so admission refuses another and apply returns `Err(InvalidCommand)`; restore compares them through the configuration |
| `LogicalApplication`, enrollment candidate | enrolled (`enrollment` present) | the `EnrollmentClaim` carries the source snapshot, whose metadata holds the parent bootstrap; decode compares it with the candidate's own (admission refuses, apply `Err(InvalidCommand)`); restore compares it through the configuration |
| `MetadataAuthoritySource` | its recorded bootstrap operation | only the exact bootstrap (anchor, export bound, initial directory bootstrap) records it; restore requires the directory's bootstrap |
| `MetadataAuthorityTarget`; serving | `staged`; the target's | only the exact anchor stages it; the serving bootstrap is compared and forwarded as the target's |
| `MetadataPublishingSource` | the source's | its exact bootstrap is forwarded as the source's; a raw source bootstrap is refused |
| `RetirementGuard` | the owner's (retired: the applied retirement) | the anchor is the owner's initial checkpoint, fixed by the owner's binding; a retirement applies only with the exact anchor |
| `Maintenance`, `LogicalActivationApplication`, the service `Application` | the inner application's | no binding of their own |
| Counter, BucketCounter, transaction leaves | `true` | no committed binding entry |

`LifecycleDirectory` forwards `Directory`'s flag, but it declares no
`deployment_requirements` and no retention rule, so readiness refuses it with
`Capability` before the flag is read: it is never attested, and a learner of
a lifecycle directory gets no receipt.

A logical voter's binding entry is one per voter or candidate (its first
successful claim, or its enrollment). The claim's record is retained and
pruned like any claim; no state is added, so nothing grows. A bootstrap voter
whose first claim (fixed fields plus the parent-bootstrap bytes) would exceed
its `max_command_bytes` is refused at construction, for every replica alike.
A new learner waits until the first claim or enrollment commits.

A third-party wrapper must AND its inner application's flag; reporting `true`
around an unbound inner application hides it, the same trusted-provider
obligation as declaring `InnerProfile::LEAF`.

**What remains outside the identity.** After round 8, readiness attests every
numeric and data construction input of the in-tree families except these:

- *Inputs bound by a committed bootstrap entry* (the table above): attested
  once that entry has applied, which positive readiness now waits for, so
  they cannot differ between replicas that hold receipts.
- *Policy and codec code.* A partition policy's bucket function, and the code
  behind a codec or policy type, are identified by their scheme or format
  version only. Two implementations under one identifier are a
  trusted-provider obligation: readiness compares data, not code.
- *`Maintenance`'s group.* Bound by the host to the core's bootstrap group,
  which readiness requires to equal the request's group: equal on every
  replica.
- *A logical voter's child bootstrap.* It is checked against the Raft
  core's bootstrap when the bridge is built, so it equals the group's
  bootstrap, which the static-bootstrap gap below covers. (Its parent
  bootstrap was outside until DR1 round 8, for bootstrap voters and
  enrollment candidates alike; it is now bound by the first claim or the
  enrollment, in the table above. It is not in the plan: it is unbounded, up
  to 4,096 voters and a policy tree.)

Transaction participants' opening images are no longer outside: they are in
the plan (below); one that does not fit is refused, not truncated.

Classification (DR1 rounds 5 to 8). *Words* and *plan* are compared exactly;
*bootstrap-bound* inputs are attested by the flag above; *outside* is what
readiness does not attest.

| Family | Exact: schema | Exact: configuration words | Exact: plan | Bootstrap-bound or outside |
| --- | --- | --- | --- | --- |
| `Counter` | format 5 and session limits | none (all in the schema) | — | — |
| `Maintenance` | profile format over the inner schema | 0 record capacity | — | group (host binding) |
| `LogicalActivationApplication` | tag 8 over the inner schema | 0 intents, 1 command bytes, 2 checkpoint bytes | parent group; every store mapping (voter, store, child) | — |
| `RoutedApplication` | the mode's schema | 0 operations (high) and payload bytes (low), 1 semantic bytes, 2 inner checkpoint bytes, 3 scoped-fence limit, parent-adoption limit and adoption modes | (an inner layer's, propagated) | bootstrap-bound: local group, grant manifest, initial inner checkpoint |
| `TransferSource` | constant | 0 export bytes | — | bootstrap-bound: the Routed layer's (source bootstrap) |
| `ScopedTransferSource` | the mode's schema | 0 export bytes, 1 parent limit, 2 every enabled mode | — | bootstrap-bound: the Routed layer's (scoped bootstrap) |
| `TransferTarget` | the mode's schema | 0 import and application checkpoint bytes, 1 partial maximum and export bytes, 2 parent limit and modes | — | bootstrap-bound: group, operation, intent, initial inner checkpoint |
| `CreatedNamespace` | the owner's schema | none (its limits are the inner Routed layer's) | — | bootstrap-bound: namespace plan |
| `RetirementGuard` | constant | none | — | bootstrap-bound: anchor of the owner's initial checkpoint (the owner's) |
| `RoutedControlReads` | forwards the inner requirements unchanged | | | |
| `BucketCounter` | constant | 0 operations, 1 semantic bytes | scope; partition scheme (id, version) | outside: bucket function (policy code); inside Routed, the grant is bootstrap-bound |
| `Directory` | the mode's schema | 0 operations, 1 history bytes, 2 every enabled profile | — | bootstrap-bound: directory plan |
| Metadata source / publishing | the mode's schema | 0 export bytes; 1–3 the initial directory's operations, history bytes and profiles | — | bootstrap-bound: initial directory plan and history (anchor) |
| Metadata target | the mode's schema | as the source's | operation; source configuration | bootstrap-bound: move plan (target anchor) |
| Metadata serving | the mode's schema | as the source's | operation; source and target configurations | bootstrap-bound: move plan (serving profile, the target's anchor) |
| `LogicalApplication` (logical voter) | candidate or ordinary schema | 0–3 action count, retained action bytes, control actions, control action bytes | mapping; parent log, parent snapshot, command, capsule and checkpoint limits; codec format versions and wire limits; the enrollment plan; the activation control profile | bootstrap-bound: parent bootstrap (first claim or enrollment); outside: child bootstrap (checked against the Raft core's bootstrap by the host) |
| `TransferApplication` | constant | 0 transactions, 1 accounts | own group (decides the role); profile (coordinator, two participants); a participant's opening image, per account in id order: id, opening | — |
| `ProgramApplication` | constant | 0 coordinator transactions, 1 keys | own group; profile version, coordinators and participants; a participant's opening keys, per key in id order: id, opening | — |

Opening images are exact only where they fit: a participant's segment is 1
header word, 12 group words and 2 words per account, so at most 89 accounts
fit a bare or Routed `TransferApplication` (another wrapper's own plan words
reduce that); a Program participant's segment is at most 1 + 54 words before
its keys (2 each: 68 keys at the largest catalog, 89 at the smallest). A
larger image overflows the profile and readiness and configuration execution
refuse it (`UnsupportedSchema`, readiness `Capability`) for every replica,
rather than truncating it. Openings never change after construction
(Transfer restore requires each opening to be equal; Program restore rebuilds
the keys from the constructed catalog), so the declaration does not depend on
applied state.

Plan layouts are documented where each family declares its requirements. A
group or store is three words (id high, id low, incarnation), an operation two.

Wrappers outside this crate must declare their own family
(`ApplicationFamily::external`) and configuration words, and set
`inner_profile` with `InnerProfile::wrapping`/`InnerProfile::of` (adding their
own plan with `InnerProfile::planned`); a wrapper that adds no behaviour
forwards its inner requirements unchanged. Declaring `InnerProfile::LEAF`
around an inner application hides it, and its plan, from readiness. That is a
trusted-provider obligation, not something readiness can detect.

**Where the identity is built.** On the readiness, membership and wire
readiness paths, and also wherever an application reads its own byte bounds
from `readiness_requirements()`: several wrappers do so in proposal
admission (`RetirementGuard`, `TransferTarget`, the metadata leaves; once per
proposal, not per pending entry) and in restore (`RoutedApplication`,
`TransferSource`, `ScopedTransferSource`, `Directory` and others). The
Counter's admission and apply paths do not build it.

## What readiness does not cover

Learner readiness and leader configuration checks are the admission points
that compare identities. Two gaps remain; both predate DR1 and are owned by
DR2 (application deployment-recovery admission), with AI3 owning activation's
counterpart. (The third recorded in round 6, readiness before a bootstrap
entry commits, is closed by the bootstrap-binding gate above.)

- **Static bootstrap** exchanges no application requirements: the initial
  voters are not compared with each other.
- **Checkpoint-less recovery.** A voter reopening a WAL with no checkpoint
  replays it under its current configuration; the WAL stores no application
  identity. Example: entries admitted by `Counter::new(2)`, `(1, 1, ACK 0,
  +7)` and `(1, 2, ACK 0, +3)`, replayed by `Counter::new(1)` give 7 (the
  second is `Window`) instead of 10.

Host obligation: start every initial voter with the identical complete
application configuration (every layer's limits, modes and plan, and every
bootstrap-bound and outside input), build every replica of a logical voter
from one plan, and recover a replica without a checkpoint only under its
unchanged configuration. Learners no longer depend on the host for
bootstrap-bound inputs or opening images: readiness refuses them. A replica restored from a checkpoint is protected:
restore refuses another schema or other configured limits.

Counter, BucketCounter, Directory, RoutedApplication, TransferSource,
TransferTarget and RetirementGuard expose their existing enforced limits through
this same contract. Host providers must enforce what they report. This is a
trusted provider capability assertion, not physical resource reservation or a
proof about a dishonest application implementation.

ClientRouter's serialized configuration execution looks up the exact group
application and checks its envelope before invoking authorization or advancing
Raft. Rejection produces NotProposed(Admission(...)) and no configuration storage
effect. It does not fence an otherwise healthy Node. The existing selected wire,
placement, authenticated readiness and core membership checks still run for valid
requests. Low-level direct Raft/EffectOwner users remain responsible for host
application admission and authorization, as documented by those interfaces.

Learner readiness checks the same envelope in addition to actual durable/applied
progress, schema, checkpoint round-trip and selected provider capacities. An
undersized lifetime declaration produces Capability refusal even when today's
checkpoint fits. The negative response grants no promotion authority. No persistent
format or freshness generation is added. Restart reconstructs the selected
application/capacity and repeats validation; an old permission is not cached.

Tests: tests/application.rs injects a downstream provider and a legacy provider;
tests/support/node.rs (tests/effect_owner.rs) verifies before-authorization and
before-persistence refusal while retaining a running owner; tests/learners.rs
checks lifetime capacity and real TCP/QUIC readiness. Executable membership
histories in tests/counter_service.rs retain enrollment/promotion/retirement and
checkpoint/restart behavior. A general authenticated configuration mutation
endpoint and broad platform/fault validation remain separate work.
