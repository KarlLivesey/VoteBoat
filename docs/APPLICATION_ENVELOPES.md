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
missing/zero actual bounds, mismatched schema and declarations smaller than the
reported envelope. Larger compatible byte declarations are accepted, subject to
the selected storage/wire/snapshot providers' independent capacity checks.

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
