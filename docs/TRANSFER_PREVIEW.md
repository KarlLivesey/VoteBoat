# Transfer preview

`voteboat-directory split-preview PROFILE` reports a proposed split or merge
without submitting commands. It lists moved and retained ranges, exact target
replicas/configurations, payload upper bounds and the pause/retention conditions.

The CLI uses **configured native BucketCounter templates**, not live or recovered
application data. It checks export/import interface availability and declared
bounds; it does not test importing actual data or reserve target capacity. Output
explicitly says `source_view=configured_bounds` and `data_import_checked=false`.

## Profile

```text
voteboat-transfer-preview-v1 TOTAL_PAYLOAD_BUDGET
intent CANONICAL_TRANSFER_INTENT_HEX
source GROUP INCARNATION OPERATIONS SEMANTIC_BYTES EXPORT_BUDGET
target GROUP INCARNATION CONFIGURATION OPERATIONS SEMANTIC_BYTES IMPORT_BUDGET POLICY
replica GROUP INCARNATION NODE STORE STORE_INCARNATION DOMAIN voter|learner
```

Use one source row per `TransferIntent::sources()` group and one target row per
`targets()` group. Declare each target before its replica rows. IDs and store
incarnations are explicit; the command does not allocate or infer them. The
intent is the hexadecimal encoding returned by `TransferIntent::encode`, including
existing checked delegated/retained shapes. Do not replace it with an uncommitted
route hint and call that authority.

`OPERATIONS` and `SEMANTIC_BYTES` configure the native bucket application's
lifetime retry/outbox capacity. Payload budgets are byte ceilings: source budgets
cover all its exported images, target budgets cover all its incoming images, and
the total covers every transfer edge. They exclude WAL records and lifecycle
metadata, and do not prove a target can import arbitrary future contents.

The policy uses the same bounded prefix grammar as membership administration:
`v:N` is voter N, `m:COUNT` is a majority branch, and `w:COUNT` has a weight before
each child. For example, `m:3 v:11 v:12 v:13`, or
`w:2 3 v:11 2 v:12`. Voter rows must exactly match policy leaves; learner rows
never count toward a quorum. Placement checks use the target manifest's minimum
voting domains and single-domain-loss requirement, including recursive policies.

The profile is limited to512KiB,256 source/target routes,4096 total target
replicas, and64MiB aggregate export payload. The CLI supports application
adapter1/version1 and byte partition1/version1. Other providers use the Rust API.
Malformed input, incompatible applications, missing/duplicate assignments,
reused target store identities and insufficient budgets fail without output.

## Rust embedding

Use `transfer::preview_transfer(&intent, &sources, &targets, max_payload_bytes)`.
`PreviewSource` and `PreviewTarget` borrow applications implementing the existing
`ScopeStateMachine`, alongside the host's adapter binding and target configuration
and placement maps. Native and host providers use the same function. Serialize
the call with each application's owner; methods must retain their pure provider
contract. The function calls validation/export-bound methods, never data export,
data import, checkpoint publication, networking or persistence.

The report binds the encoded intent digest, expected epoch/generation and exact
target identities. Source applied indexes are local observations, not durability
receipts. Source applications must cover their original owned ranges, including
retained portions. Recompute the preview after an intent/configuration change or
restart; a prior preview never authorizes execution.

Moved ranges pause from source fencing until valid target activation. No finite
duration or WAL-retention byte count is promised: a missing quorum can prevent
progress. Post-transfer ordering is per target group, with no cross-group atomic
transaction. Execution still requires authorization/current generations, durable
source fencing and target data, publication, activation, retry/outbox preservation
and explicit retention release. The operator start/status/resume workflow is
separate work; this command only previews.

Tests cover native and downstream host applications, recursive target policies,
split/merge and retained insertion, schema/scope/budget refusal, unchanged source
and target state, forbidden data export/import, and real executable parsing.
