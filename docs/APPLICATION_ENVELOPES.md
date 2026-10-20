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
wrapped application, outermost first (at most seven). A layer identity is the
protocol family (`raft::ApplicationFamily`, whose constants are the registry
of in-tree families) and four configuration words. Readiness compares schema,
family and configuration of every layer exactly (`UnsupportedSchema`), so two
families that reuse schema numbers, or two replicas configured differently,
refuse each other before any entry. Only `command_bytes` and `snapshot_bytes`
are storage reservations, accepted when the declaration is at least the
actual bound. No hashing: the identity is fixed width.

Classification (DR1 round 5). *Exact* fields decide committed entries or the
checkpoint encoding. *Deployment plan* fields are shared by every replica of
the group and bound by its committed bootstrap entry; readiness does not carry
them (they are variable length), so keeping them identical is a host
obligation, owned by DR2 (activation: AI3).

| Family | Exact: schema | Exact: configuration words | Deployment plan (host obligation) |
| --- | --- | --- | --- |
| `Counter` | format 5 and session limits | none (all in the schema) | — |
| `Maintenance` | profile format over the inner schema | 0 record capacity | group |
| `LogicalActivationApplication` | tag 8 over the inner schema | 0 intents, 1 command bytes, 2 checkpoint bytes | parent group, store mappings (AI3) |
| `RoutedApplication` | the mode's schema | 0 operations (high) and payload bytes (low), 1 semantic bytes, 2 inner checkpoint bytes, 3 scoped-fence limit, parent-adoption limit and adoption modes | local group, grant manifest, initial inner checkpoint |
| `TransferSource` | constant | 0 export bytes | (the Routed layer's) |
| `ScopedTransferSource` | the mode's schema | 0 export bytes, 1 parent limit, 2 every enabled mode | (the Routed layer's) |
| `TransferTarget` | the mode's schema | 0 import and application checkpoint bytes, 1 partial maximum and export bytes, 2 parent limit and modes | group, operation, intent, initial inner checkpoint |
| `CreatedNamespace` | the owner's schema | none (its limits are the inner Routed layer's) | namespace plan |
| `RetirementGuard` | constant | none | anchor (derived from the inner initial checkpoint) |
| `RoutedControlReads` | forwards the inner requirements unchanged | | |
| `BucketCounter` | constant | 0 operations, 1 semantic bytes | scope, policy |
| `Directory` | the mode's schema | 0 operations, 1 history bytes, 2 every enabled profile | directory plan |
| Metadata source / target | the mode's schema | 0 export bytes | plan, operation |
| Metadata serving / publishing, logical voter, transfer and program transactions | constant or mode schema | family only; their limits are not yet audited (metadata: DR2; logical voter: LV; transactions: TX1) | |

The partition policy is a type (trusted code), not configuration.

Wrappers outside this crate must declare their own family
(`ApplicationFamily::external`) and configuration words, and set
`inner_profile` with `InnerProfile::wrapping`/`InnerProfile::of`; a wrapper
that adds no behaviour forwards its inner requirements unchanged. Declaring
`InnerProfile::LEAF` around an inner application hides it from readiness. That
is a trusted-provider obligation, not something readiness can detect.

## What readiness does not cover

Learner readiness and leader configuration checks are the admission points
that compare identities. Two paths admit a replica without them; both predate
DR1 and are owned by DR2 (application deployment-recovery admission), with AI3
owning activation's counterpart:

- **Static bootstrap** exchanges no application requirements: the initial
  voters are not compared with each other.
- **Checkpoint-less recovery.** A voter reopening a WAL with no checkpoint
  replays it under its current configuration; the WAL stores no application
  identity. Example: entries admitted by `Counter::new(2)`, `(1, 1, ACK 0,
  +7)` and `(1, 2, ACK 0, +3)`, replayed by `Counter::new(1)` give 7 (the
  second is `Window`) instead of 10.

Host obligation: start every initial voter with the identical complete
application configuration (every layer's limits and modes as well as the
deployment plan), and recover a replica without a checkpoint only under its
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
