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
and operations an application was built for. `InnerProfile::wrapping` carries
an inner layer's plan one layer deeper, so every wrapper that uses it
propagates the plan. Readiness compares schema, family, configuration and plan
of every layer exactly (`UnsupportedSchema`), so two families that reuse
schema numbers, or two replicas configured differently, refuse each other
before any entry. Only `command_bytes` and `snapshot_bytes` are storage
reservations, accepted when the declaration is at least the actual bound. No
hashing: the identity is fixed width, and a plan that does not fit overflows
the profile, which readiness refuses rather than truncating.

**What it does not attest.** The identity is exact for what it carries; it is
not injective over everything a replica is built from. Outside it: initial
images (a participant's opening accounts or keys, an inner application's
initial checkpoint), grant manifests, directory and namespace plans,
retirement anchors, the child and parent bootstraps of a logical voter, and
the code behind a policy or codec type (a partition policy's bucket function
is trusted code, identified only by its scheme). Where a committed bootstrap
entry compares one of them, a replica built differently fails that entry with
`Err(InvalidCommand)`; that protects a learner only if the entry committed
before its readiness index (see below). Otherwise keeping them identical is a
host obligation.

Classification (DR1 rounds 5 and 6). *Words* and *plan* are compared exactly;
*outside* is what readiness does not attest, with the committed entry that
binds it where there is one.

| Family | Exact: schema | Exact: configuration words | Exact: plan | Outside (bound by) |
| --- | --- | --- | --- | --- |
| `Counter` | format 5 and session limits | none (all in the schema) | — | — |
| `Maintenance` | profile format over the inner schema | 0 record capacity | — | group (host binding) |
| `LogicalActivationApplication` | tag 8 over the inner schema | 0 intents, 1 command bytes, 2 checkpoint bytes | parent group; every store mapping (voter, store, child) | — |
| `RoutedApplication` | the mode's schema | 0 operations (high) and payload bytes (low), 1 semantic bytes, 2 inner checkpoint bytes, 3 scoped-fence limit, parent-adoption limit and adoption modes | (an inner layer's, propagated) | local group, grant manifest, initial inner checkpoint (Routed bootstrap) |
| `TransferSource` | constant | 0 export bytes | — | (the Routed layer's; source bootstrap) |
| `ScopedTransferSource` | the mode's schema | 0 export bytes, 1 parent limit, 2 every enabled mode | — | (the Routed layer's; scoped bootstrap) |
| `TransferTarget` | the mode's schema | 0 import and application checkpoint bytes, 1 partial maximum and export bytes, 2 parent limit and modes | — | group, operation, intent, initial inner checkpoint (target binding) |
| `CreatedNamespace` | the owner's schema | none (its limits are the inner Routed layer's) | — | namespace plan (namespace bootstrap) |
| `RetirementGuard` | constant | none | — | anchor of the owner's initial checkpoint (checked at retirement and restore only) |
| `RoutedControlReads` | forwards the inner requirements unchanged | | | |
| `BucketCounter` | constant | 0 operations, 1 semantic bytes | scope; partition scheme (id, version) | bucket function (policy code); inside Routed, the grant |
| `Directory` | the mode's schema | 0 operations, 1 history bytes, 2 every enabled profile | — | directory plan (directory bootstrap) |
| Metadata source / publishing | the mode's schema | 0 export bytes; 1–3 the initial directory's operations, history bytes and profiles | — | initial directory plan and history (bootstrap anchor) |
| Metadata target | the mode's schema | as the source's | operation; source configuration | move plan (target bootstrap anchor) |
| Metadata serving | the mode's schema | as the source's | operation; source and target configurations | move plan (serving bootstrap profile) |
| `LogicalApplication` (logical voter) | candidate or ordinary schema | 0–3 action count, retained action bytes, control actions, control action bytes | mapping; parent log, parent snapshot, command, capsule and checkpoint limits; codec format versions and wire limits; the enrollment plan; the activation control profile | child bootstrap (checked against the Raft core's bootstrap by the host), parent bootstrap |
| `TransferApplication` | constant | 0 transactions, 1 accounts | own group (decides the role); profile (coordinator, two participants) | a participant's opening accounts (initial image; inside Routed, its bootstrap) |
| `ProgramApplication` | constant | 0 coordinator transactions, 1 keys | own group; profile version, coordinators and participants | a participant's initial keys (initial image; inside Routed, its bootstrap) |

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
that compare identities. Three gaps remain; all predate DR1 and are owned by
DR2 (application deployment-recovery admission), with AI3 owning activation's
counterpart:

- **Static bootstrap** exchanges no application requirements: the initial
  voters are not compared with each other.
- **Checkpoint-less recovery.** A voter reopening a WAL with no checkpoint
  replays it under its current configuration; the WAL stores no application
  identity. Example: entries admitted by `Counter::new(2)`, `(1, 1, ACK 0,
  +7)` and `(1, 2, ACK 0, +3)`, replayed by `Counter::new(1)` give 7 (the
  second is `Window`) instead of 10.
- **Readiness before a bootstrap entry commits.** An input outside the
  identity but compared by a committed bootstrap entry (the "outside" column)
  protects a learner only once that entry is in the committed prefix the
  learner verified. A learner verified before it commits can get a receipt
  and then fail that entry with `Err(InvalidCommand)`. Closing this means
  refusing readiness until the family's bootstrap entry has applied (DR2).

Host obligation: start every initial voter with the identical complete
application configuration (every layer's limits, modes and plan, and every
input in the "outside" column), build every learner from that same
configuration, admit a learner only after the group's bootstrap entries have
committed, and recover a replica without a checkpoint only under its
unchanged configuration. A replica restored from a checkpoint is protected:
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
