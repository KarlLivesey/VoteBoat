# P9 next-slice schema: ordered proposal batches, then depth-one persistence overlap

Status: read-only design audit, 2026-10-13. No implementation or tests performed for this schema. Priority is concurrent useful work and less waiting; scalar microbenchmarks are controls, not the objective.

## Recommended slice boundary

1. Add a **direct public Core batch proposal API and real framework batch-4 workload**, with four original log commands and four actual application results. This is a complete, reviewable Core-embedding capability, not Node/native-client batching.
2. Add batching to the ordinary owner/client path, with bounded ready collection and per-original invocation routing. Dispatch a ready singleton immediately; no mandatory collection timer. This is the slice that gives existing Node clients the throughput benefit.
3. Add depth-one replication overlap at the **exact Written/admitted ticket** of an ordinary command suffix. This requires coordinated Core, worker, effect-owner, outbound-currentness and failure tests. It is not a one-line change from `Persist` to `[Persist, Send]`.

The existing code deliberately permits one pending persistence dependency per Core. Do not remove that guard globally to get concurrency. The first two slices can gain batching while retaining the current single pending persistence transition and every durable-quorum gate.

## A. Minimal Core batch capability

Proposed shape:

```rust
pub struct Proposal { pub operation: OperationId, pub bytes: Vec<u8> }
impl Raft {
    pub fn propose_batch(&mut self, proposals: Vec<Proposal>) -> Result<Vec<Effect>, RaftError>;
}
```

Use a direct Core method initially, rather than exposing a new `Event::ProposeBatch` that existing Shard/Owner/ClientRouter silently mishandles. Factor the same fence/pending/role/transfer admission checks and post-step transfer progress used by `step(Event::Propose)`; do not bypass them in the new method. Keep single proposal behavior equivalent by sharing the entry-building helper. Hosts still own application admission validation, exactly as for direct Core single proposals today.

Bound the initial batch to 1..=64 commands (four must work; 64 matches the existing replication range cap), checked cumulative payload plus log-entry/framing overhead within the existing log/replication budget, per-command `max_command_bytes`, available retained-entry capacity and checked contiguous index arithmetic. Reject the complete batch before a new pending transition for invalid shape or overflow. Retain original IDs and bytes. Duplicate IDs must retain existing retry/dedup semantics; a batch API must not invent a new global uniqueness rule.

One batch produces **one `LogUpdate` suffix containing N ordinary `LogEntry::Command`s**, one exact persistence ticket, normal replication and commit, then the existing `Committed(Vec<LogEntry>)` application effect. Business results remain per command: this is not a new atomic cross-command application transaction. No opaque envelope interpreted as one command, no multiplication of one result into four, and no four unrelated groups counted as one batch.

Ownership: input Vec transfers into the Core transition. Rejection must be documented consistently with existing consuming `step`; a future public host convenience API can return rejected owned inputs if needed. No borrowing of caller buffers across async persistence. A batch reservation helper or documented `effect_reservation(total_owned_input_bytes)` must account for Vec metadata/capacity plus every payload, not merely the largest member.

Sequence:

```
N checked original proposals
  -> one ordered suffix/pending transition
  -> exact store append admission
  -> exact local barrier completion
  -> ordinary per-peer replication (up to 64 entries)
  -> durable voter acknowledgements / current-term quorum frontier
  -> durable local commit marker
  -> N original entries applied in order
  -> N original application results
```

Core files: `src/raft.rs` or a focused `src/raft/proposals.rs`; Core tests under the existing raft test module; `examples/framework_benchmark/{cluster,workload,tests}.rs` plus settings/reporting. `src/log.rs` already supports multi-entry suffixes; storage format, RPC shape and wire codec already carry Vec<LogEntry>, so no new wire format is needed.

The framework must allocate four distinct original operation IDs per client request, retain all four, and wait for all four individual applied results before that client submits its next batch. Throughput unit is command entries, not batch calls. Track batch latency and per-entry count separately. Requests/operations must divide by clients*batch (or publish an explicit rounded count). Retained-history limits include warmup plus actual entry count and leader no-op. Do not hide checkpoints or reset history between parts of one timed run.

### Core acceptance checks

- Batch of four produces four sequential original entries and four applied results on a three-voter majority, using one proposal persistence transition.
- No local append completion means no local durable matching evidence or early client result; follower written-only state is not a quorum ack.
- Invalid member payload, cumulative byte budget, empty/too-many commands, entry exhaustion and index overflow leave no partial pending transition.
- Singleton equivalence, duplicate operation retry semantics, nonleader/fenced/busy/leadership-transfer refusal.
- Restart before/after append and before/after commit marker retains ordinary suffix/commit behavior; stale ticket/session/generation cannot complete a new batch.
- Quorum policy/current-term commit rules unchanged, including joint/stable memberships on the serialized path.
- Framework request waits for all four original results; losing one, duplicating one, or reporting only a prefix cannot complete the batch.

### Capability expressly absent after this slice

`Node`, `ClientRouter`, native TCP/QUIC service request APIs and application admission cannot yet submit a batch. They remain correct single-proposal APIs. The performance report must say **Core embedding batch capability**, not full Node or network batching. The direct Core framework remains unmatched to the OpenRaft benchmark until worker scheduling, retention/checkpoint behavior, load settings and same-host execution are resolved.

## B. Owner/native ready-batch mapping audit

A new `Event::ProposeBatch` is only appropriate together with all relevant routes below. A wildcard match compiling is not sufficient evidence.

| Existing surface | Required change/check |
| --- | --- |
| `src/raft.rs::Event`, `step`, proposal helper | Batch enters identical leader/fence/transfer checks and shared bounded builder. |
| `src/runtime.rs::event_cost` | Classify as Data; charge batch Vec capacity, Proposal inline metadata, every payload capacity, Event and tracked admission overhead using checked arithmetic. Current wildcard classifies unrecognized events as Control. |
| `src/runtime/logical_intake.rs::proposal_class` | Batch must not gain protected control capacity because one member is control. First native ordinary batch path should exclude logical-activation owners, or classify all members explicitly with verified profile and complete bytes. |
| `src/raft.rs::{event_effect_reservation,reserve_effects}` | Reserve complete proposed entry set plus pending state and fanout/commit continuation. This is a whole batch bound, not N independent uncharged events. |
| `src/runtime.rs::step_next_checked`, `Stepped` | Currently extracts exactly one operation and only the first suffix entry's position. Needs ordered per-original invocation positions and validation, not a fabricated first position for every member. |
| `src/runtime/effects.rs::OwnerStep`, `finish_step` path | Currently transports one admission/operation/position. Extend with typed ordered results or define a batch step carrying member identities. Keep bounded ownership and failure routing explicit. |
| `src/runtime/clients.rs::{check_submit,submit,advance_authorized}` | Submission/admission checks every command against prior accepted log and earlier members in the batch; revalidate at execution. Current `admissions: admission -> single sequence` and `Pending.admission` are one-to-one and need an explicit one-to-many model, or ready collection must preserve all existing tickets. |
| `ClientRouter` position map, reconciliation, cancellation, cleanup | Every original invocation keeps its ClientTicket, term, retained bytes, receipt reservation and eventual position. A cancelled waiter does not cancel the whole batch or erase its dedup/application obligations. Individual NotProposed/Unknown/applied outcomes cannot leak other members' capacity. |
| `src/runtime/applications.rs` | Already supports multi-command `Committed` leases, receipt count bounds and ordered `ProposalPosition`s. Validate configured `batch_receipts`/bytes accommodate the chosen batch and preserve every original result. No receipt-count multiplier. |
| `src/runtime/replica.rs`, `src/runtime/node.rs` | Expose ready collection / public batch admission only after the plural mapping works. Keep unrelated control/receive/read work fairly schedulable and singleton immediate. |
| `src/runtime/clients/control.rs`, `src/runtime/logical_release/*`, logical Node paths | Explicitly exclude initially or implement whole-batch classification/release authorization. No logical owner bypass via direct ordinary path. |
| `src/runtime/connections.rs`, `src/raft/connections.rs` | Ordinary command batch adds no membership peers; inspect and assert that property. Continue to reserve preexisting/rollback peers. |
| `src/worker.rs`, `LogStore`, native/striped stores | Existing one-unit/many-entry suffix already fits. `max_batch_units` counts groups/updates, not proposals; do not confuse them. One batch still issues one exact unit ticket. |
| application wrappers / transfer / maintenance / program transactions | Existing per-command ProposalAdmission hooks must see each member in order. Do not relabel a program transaction's constituent commands or bypass semantic conflict checks. |

For ready collection, prefer a bounded owner operation that collects already queued **same-group ordinary data proposals** up to count/byte limits, retaining their original AdmissionTickets and priority accounting. Do not dequeue unrelated events or cross a configuration/control boundary. Before exposing this, define whether application admission rejects the entire collected set or retains eligible members; an explicit all-or-nothing admission decision is the smaller first contract. Even then business application outcomes remain per command.

Owner/client acceptance adds: four independently submitted same-group clients coalesce while retaining four tickets/results; cancellation of member 2; duplicate operation with same and conflicting contents; changed application admission between enqueue and execution; capacity exhaustion without partial consumption; group fairness/control reserve; shutdown while queued, appended, committed or awaiting receipts; lost terminal completion/recovery; native and no-default-feature builds.

## C. Depth-one ordinary append overlap

### Why the existing dependency is serial

- `Raft::persist_update` currently returns only `Persist` and retains `Pending { update, next, membership, ticket, after, reply }`.
- `After::LeaderAppend` broadcasts only from `complete`, after the local barrier.
- `append_for` and `request_window::range_matches` only obtain/validate bytes from `self.durable`.
- `step`, Shard next/step/finish and the active visit keep all further Core events suspended while the dependency is pending.
- `WorkerEvent::Written` only calls `admitted(ticket)` and yields no effects. `Durable` calls `complete`.
- `EffectOwner::submit_persists` requires the Persist lease be the last queued effect. Therefore `[Persist, Send]` is currently rejected.
- `EffectOwner::install` requires no queued effects and no current lease. If Written begins returning Send, a immediately following Durable event can collide with those sends (same worker poll may contain both). This must be designed, not ignored.

### Smallest supported overlap

Only one immutable **ordinary command** suffix, already accepted under an exact LogTicket, may be transmitted before its barrier. No term/vote changes, no configuration entries, no snapshot/read/leadership/learner-repair authorization effects escape this way. Initially keep logical-voter response-capsule owners on the serialized path until their release contract is separately qualified; ordinary relay transport still requires original current request scope.

Eligibility is checked from immutable old durable state plus the exact pending unit: established current-term leader, local voter, same hard state/ballot origin, same accepted membership, pure append at durable tip, bounded all-Command suffix, no suffix rollback/configuration transition, valid exact admitted ticket and current store session/generation. A conservative first eligibility policy may require a committed current-term entry and a stable configuration; make any restriction explicit in report counters. It never blocks functional progress: ineligible paths use existing serialization.

Depth one means **one pending append/local barrier**, not arbitrary multiple outstanding local WAL revisions. The Core keeps its normal event dependency. Incoming peer acks (including higher-term responses) remain bounded queued events and are not applied until local completion. Local progress is never advanced from Written.

Proposed dependency DAG:

```
existing durable term/vote/config + bounded ordinary batch
                      |
               stage one pending suffix
                      |
           exact store append / Written(ticket)
                /                         \
 immutable authorized Append sends       local barrier
 -> follower append/barrier -> acks       -> Durable(exact ticket)
                \                         /
          Core processes original queued acks only now
                      |
           current-term durable quorum frontier
                      |
           durable local commit marker
                      |
                apply -> original receipts
```

No new `DurableLog` token is invented. The admission ticket authorizes only sending these bytes under already durable leader identity, not voting, a local matching position, commitment, application, or client success.

### Required Core/currentness representation

Retain an explicit origin for an early replication window: exact original pending ticket + group/revision/generation + original request context + immutable start/end/term/configuration. The currently held payload may be matched against `Pending.next` **only for this origin**, not by globally replacing durable lookups with accepted state.

`RequestWindow`/`range_matches` must validate that exact origin while pending, transition to the same durable origin on exact completion, and become retired on failure, rollback, changed session/term/configuration, peer binding replacement, recovery or request-context replacement. Old early frames must not borrow validity from a later identical-looking append. Keep the original one-outstanding-request-per-peer rule and preserve outstanding request contexts across retries; never overwrite an older live request to accelerate a new batch. A minimal first implementation emits early only for eligible caught-up peers without an outstanding window; lagging peers continue normal catch-up.

`After::LeaderAppend` must not overwrite early contexts at durability. It updates local matching progress only then, emits any missing ordinary sends, and allows the existing quorum/commit flow. `maybe_commit` still caps by actual local durable last_index and actual durable voter acknowledgements. The invariant remains true regardless of which barrier finishes first.

Files: `src/raft.rs`, `src/raft/receive.rs`, `src/raft/request_window.rs`, `src/raft/request_window/replication.rs`; source/connection inspectors; `src/native/relay.rs` plus completion/currentness tests. Existing `RequestWindow::current` and relay encode/reply preflights must continue to enforce exact original source/store/term/configuration, not just index equality.

### Required worker/owner lifetime handling

Keep the initial Persist as the sole initial leased effect; begin early outbound continuation only after exact Written has bound the ticket. Preserve `admitted` compatibility if adding a distinct public `replicate_admitted`/accepted-continuation method. `persist_effect` remains valid as a synchronous fallback, even if it cannot overlap.

Before implementing, choose one explicit owner solution:

1. Reserve the sum of early-output + local-completion continuation bounds up front; allow a typed continuation to append after current queued/leased sends, tracking leased bytes and preserving the original lease ticket; or
2. Retain one exact bounded terminal worker completion per active visit until early sends transfer, then apply it. Preserve terminal worker input/output accounting and eventual failure fencing. This is simpler but may introduce output-backpressure delay.

Do not overwrite `Active.effects`, drop a held send, mark a visit drained with a retained terminal result, or turn local-barrier success into a new admission. Cover Written then Durable in one poll, Durable while send leased, rejection before outbound transfer and retry, send queue saturation, and store failure after an early follower has persisted. Existing `install` cannot simply be called unchanged.

Files: `src/worker.rs::Task::apply`, `src/runtime/effects.rs::{Active,submit_persists,deliver_worker,install,refresh}`, `src/runtime/replica.rs` effect collection/retry, selected outbound/relay paths, corresponding effect-owner/native integration tests. Reservation math needs the **simultaneously retained phases**, not just max(one phase).

### Failure and acceptance matrix

- With local barrier held, follower persists the actual suffix, but leader local matching index/commit/application/client count remain unchanged.
- Release exact local barrier, deliver original acks, commit and return every original result once.
- Written with wrong store/session/revision/generation fails; old completion cannot authorize retransmission.
- Crash after sending but before local barrier: recover only actual durable prefix; follower suffix is uncommitted and may be overwritten normally.
- Local persistence failure after follower success: original owner fences, no commit/client success; old outbound/currentness retires.
- Remote ack before local durable is bounded queued input; never counted early. Local barrier before/after remote barrier gives equivalent outcome.
- Higher term, membership replacement, peer restart and old-context duplicate ack cannot validate a stale window.
- Existing outstanding peer request is retained; no heartbeat/context churn or duplicate-window leak.
- Delay/fail/drop each worker and outbound completion; send lease held across terminal storage completion; Written and Durable in the same poll; retry and close/drain eventually release all original owners.
- Unrelated groups and control messages keep capacity while one group is blocked; no global mutex or hidden queue introduced.
- Existing joint/learner/snapshot/leadership/logical-capsule paths retain serialized behavior until explicitly qualified.
- Compare concurrent throughput curves plus p50/p99 and bounded queue occupancy; this optimization is justified by reduced overlap wait, not only lower scalar execution time.

## Scaffold follow-ups before a wider campaign

`Settings.retained_entries` is currently a user-provided usize with no explicit finite application cap beyond downstream LogLimits validation and platform allocation failure. Add a deliberate bounded cap (and validate it before preparation) before accepting wide benchmark input. This is benchmark robustness, not a production storage limit change. Current p50/p99 index formula uses `(len-1)*p/100`; label it or adopt documented nearest-rank `ceil(p*N/100)-1` with small-N controls. Neither change should be slipped into a quiet frozen measurement run.
