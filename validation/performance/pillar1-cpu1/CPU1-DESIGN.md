# CPU1 — implement explicit unacknowledged commit notices

Decision for the integration owner, 22 October 2026. Source audited exclusively at `0bf9271ecb5a34c4089c6bb10381303232624170`; later parent docs-only commit is not a source baseline change. **Implement this slice next.** No production changes, builds, tests, benchmarks, Mac work, worktrees or delegation were performed here.

## Choice and bound

Remove the two commit-notice replies from the ordinary three-voter completed-write cycle. Add a distinct `Rpc::CommitNotice`, not a flag inferred from an empty Append or from whether its commit happens to advance. Empty Append remains an acknowledged replication/probe/heartbeat operation.

V3g §6 reports scratch results: width-one driver 2,545→2,152 ns (−15.4%), single-16 20,479→19,086 ns (−6.8%). These are **prototype evidence, not qualified current-main results**. The report used combined V3e+V3f on an older source and an unsafe-to-generalize suppression heuristic. Current main has the relevant in-place/peer-slot paths, but needs fresh qualification.

Alternatives considered only to choose:
- Volatile commit-only staging removal: estimated 200–330 ns/round-cycle (8–13% driver; roughly 1–1.6% single-16). Real reusable library work, but preserves cross-actor messages/wakes. Second choice after notices, no N1d/WAL dependency.
- Smaller Effect/Message or a new embedding handoff: profile estimates only 60–100 ns single-16 library saving from size reduction. Matched `Actor::handle` moves and `PendingReplies` are largely adapter costs. A new public integrated executor/reply API has no demonstrated reusable saving sufficient to beat removing two actual message exchanges. Do not build a harness/instrument prerequisite.
- No map-first slice: estimated 0.6–0.8% single-16 cannot justify displacing the measured structural candidate.

First slice deliberately covers **bootstrap/static physical voter membership**, including normal static snapshots/compaction, and both volatile and durable core semantics. It does not require reconfiguration/logical/native-network rollout. Membership-bearing histories and nonnegotiated assemblies keep acknowledged Append. This excludes difficult consumers by a concrete state predicate, not by assuming compatibility must be preserved. No old PR19/23 code or policy is required.

## Data/API and exact eligibility

Public shape in `src/raft.rs` (there is **no** current `src/message.rs`):

```rust
Rpc::CommitNotice { index: u64, log_term: u64 }
// index is the sender's already installed contiguous commit prefix.
// Header retains group/configuration/from/sender/to/term/context.
pub fn enable_commit_notices(&mut self) -> Result<(), RaftError>;
```

Use a private startup capability bit, initially false for existing recover constructors. Enable before campaigning, on an unfenced idle follower with plain bootstrap membership, no persistence/snapshot/application dependency, and no replication authorization. Reject other calls; make repeated startup enabling idempotent. It asserts the selected host delivery path and every endpoint understand this RPC; it does **not** grant voter, durability or leadership authority. This is an equal-guarantee delivery optimization, not an optional weaker commit mode. Matched construction enables it on all three public cores. In-process library consumers can do the same without importing matched code. Native/logical assemblies remain off unless explicitly assembled with the capability.

Send a notice only when all hold:
1. Capability enabled; leader and local voter; `progress_sends_enabled()`; plain log summary (no configuration records/snapshot membership), stable bootstrap membership, no learners.
2. No pending transition; `C = commit_index = last_index > 0`; `term_at(C) = current_term`.
3. Recipient is an exact current bootstrap voter. Its **existing correlated durable** `progress >= C`, `next_index = C+1`; no primary request, admitted range or empty-Append alias for that peer.
4. This is a commit-progress obligation, not a heartbeat, ordinary append broadcast, probe, retry or transfer drive.

Use a purpose argument/private helper at `progress_broadcast.rs`: commit broadcasts may choose the notice; append broadcasts retain `append_for`. In `receive_appended_into`, after validating/retiring the actual data request, the `sent.commit_at_start < current_commit` catch-up branch may choose the same helper **only when no further entries remain**. Preserve `prefix < last_index` and every failed-ACK path as ordinary Append. The first responding follower gets its notice after leader commit installation; the other gets one after its original data ACK is consumed. Never cancel an outstanding request to make a peer eligible.

Any failed eligibility check uses current `append_for` behavior, including its exact request-context/range retries. `Event::Heartbeat -> broadcast_into -> append_for` stays acknowledged, even when the peer is caught up.

## State, provenance, ownership and failure

**Emission evidence:** add `PeerSlots<CommitNoticeEmission>` beside `requests`, not a response collector. Each Copy stamp records exact context, configuration, term, index/log_term and recipient StoreIdentity, written only when constructing an actual notice effect. Keep one latest emitted stamp per peer. `request_window/special/one_way.rs` classifies this RPC as OneWay only on exact stamp/header/payload/current-binding/current-role match. A request-sequence count, an empty request slot, or large `progress` alone is not an emission proof. A stamp is never accepted by `RequestWindow::accepts_reply`, never updates `progress/next_index`, and never satisfies quorum/read/readiness/transfer evidence. Superseded output can be retired; the next acknowledged heartbeat repairs it.

Clear stamps at term/role/configuration invalidation, fence/storage failure, compaction/snapshot replacement and recovery. Do not clear the stamp merely because another ordinary request is retired. Retained keys remain bounded by installed peers through existing PeerSlots lifecycle rules. Add their retained allocation to the resource accounting contract; use checked arithmetic and actual Vec capacity, not occupied count. Existing conservative effect reservation already dwarfs a scalar notice, but verify that explicitly rather than bypassing it. No retained entries/buffers per stamp, no new timer/retry queue, no per-operation wait.

**Follower:** authenticate destination/group/store/context origin/sequence/current exact configuration and voter identity before state changes. Unlike ordinary Append/Snapshot, this first slice does not bridge accepted configuration heads or use replication permits/retirement authority. Validate `index>0`, `0<log_term<=message.term` and plain static local membership. Old-term notices are benignly ignored, never acknowledged. For a valid current/higher-term notice, preserve ordinary follower transition: clear reads, vote context and old leader replication/repair requests; process higher-term HardState through existing persistence.

If `term_at(index)==Some(log_term)`, set planned commit to `max(old_commit,index)`; otherwise leave commit unchanged. No suffix creation/truncation, no snapshot staging, no min(last_index,index) shortcut. Missing/compacted/mismatching boundary receives no reply and awaits ordinary heartbeat repair. A matching notice may release only locally held contiguous entries. Changed commit/hard state goes through existing `persist_into(..., After::Reply, None, out)`; unchanged state needs no persistence/effect. Existing exact completion installs it and existing application machinery applies it. **CPU1 does not eliminate commit persistence rounds.**

**Contact:** extend `receive_into`'s valid leader-contact family to CommitNotice, and classify it as a sender-originated input in validation. After successful validation/handling, same/higher-term notice contact updates `leader_contact` and resets elections just as Append does, including a valid but nonmatching log boundary. Do not reset on malformed, wrong-store/configuration or old-term traffic. No new leader-side lease/check-quorum evidence: the audited receive path counts requests as follower contact, not Appended replies; `ReadAck` remains separately correlated.

**Loss/reorder:** dropped notice is repaired by the next existing heartbeat (and subsequent ACK/commit follow-up); there is no notice retransmission obligation to drain. State one-heartbeat convergence only for the caught-up matching case with delivered heartbeat and completed persistence/application; catch-up/snapshot/partitions need their normal rounds. Duplicate/delayed notices cannot regress commit or reapply an original. A duplicate same-boundary notice need not write a revision. A higher-term follower does not report that term through a notice reply; the next acknowledged heartbeat discovers it. This bounded delay is explicit.

**Shutdown/error/restart:** Send remains owned by the normal bounded effect/outbound owner until accepted or rejected; local completion releases credits only. A rejected/lost one-way frame can be retired and repaired by heartbeat. Do not spin/requeue it indefinitely or count it as delivered. Persistence failure fences via existing paths, with no committed interval escaping; application failure sends no client success. Shutdown drains/reports accepted local work but never waits for a nonexistent notice ACK. Durable recovery reconstructs commit/membership through unchanged store paths; stamps/capability require fresh startup configuration. Volatile restart has no cross-process persistence promise. No fence/import/activation or operation-ID/dedup rules change.

## Consumer audit and compile surface

| Current consumer | CPU1 obligation |
|---|---|
| `src/raft.rs:76,775,1043,1286,1515,1785,1935,1983`; `raft/{receive,source,progress_broadcast}.rs` | New enum/API/state; exact validation/contact; notice emission and receive. Keep completion/commit/apply release unchanged. Reservation includes scalar notice/stamp bounds. |
| `raft/{request_window.rs,request_window/replication.rs,request_window/special/one_way.rs,admitted_replication.rs,send_window.rs}` | Exact OneWay emission proof. No Boundary::Append, alias or original replication request for notice. Keep all ordinary reply families and granted-vote timing. |
| `raft/{leadership,readiness,configuration,authority,joint_repair,batched_repair,snapshot_repair,connections}.rs` | Fallback audit: transfer starts through acknowledged broadcast; readiness remains its own fresh exchange; learners/joint/Final/retirement and differing-head permits never generate notices. New variant does not import membership or enlarge connection roster. |
| `src/outbound.rs:message_cost`; `runtime/{effects,ingress,connections,ready_batch}.rs` | Existing wildcard costs scalar RPC as Control. Exercise bounded admission/rejection and control reserve; no newly routinized batching or treating notice as Appended. Effect leases and normal event reservation remain mandatory. |
| `src/wire_format.rs`, `wire_format/capacity.rs`, `wire.rs` | Encoder tag/validation/decoder/counting pass and exact version gate; capacity fixtures still ordinary Append. Preserve bounded sizing and checked decode. |
| `native/relay/network/{frame.rs,forwarder/ledger.rs,provider/ingress.rs}` | OneWay recognizes notice; Request explicitly excludes it. No ReplyScope or RequestRecord created, no return ticket. Reject relabeling notice as Request/Reply. Actual sent original retains normal credits. Frame envelope needs no new RecordKind. |
| `logical_voter/application/authorize.rs:125`; `runtime/logical_release/{capture,types}.rs`; logical bridge/node | Exhaustive RPC match must explicitly **refuse** notice in CPU1 logical authorization; these assemblies stay capability-off. Their existing acknowledged path and child-durable ballot windows stay intact. Do not authorize merely because index<=parent.commit. Later logical support needs captured emission plus child-committed parent image. |
| `benchmarks/matched/src/voteboat_engine.rs`, `voteboat_engine/actor.rs`, actor tests | Enable public capability at construction only. `handle/send/persist/apply` continue their generic paths; no suppressed client reply, altered collection width, oneshot/ring rewrite or direct peer mutation. |
| Tests: `raft/{progress_broadcast/tests,request_window_tests*,source_tests,volatile_tests,append_prefix_tests,completion_tests,admitted_replication/tests,membership_tests}.rs`; public tests/examples and wire/relay/logical format tests | Update only changed expected traces; add cases below. Build every public consumer in all features, including external injection examples and separate matched workspace. |

Compatibility choice: add native wire **9**, constructor `NativeWireCodec::with_commit_notices`, unused RPC tag **19** (18 is TimeoutNow). Encode two u64 fields; header/context stays unchanged. Formats1–8 refuse it; do not reinterpret their empty Append. Extend `NativeTlsConfig::with_wire_version`'s current 1..=8 acceptance to9 for explicit assembly; retain exact-version HELLO refusal (shared TLS/QUIC use), existing ALPN and default native configurations. No auto downgrade, per-peer negotiation subsystem or network performance work. Persistent log/snapshot formats do not change. Update production format/API docs/inventory if required, never the original design pack.

## Deterministic acceptance and causal controls

1. Warm a static three-voter group, fully drain setup, complete one unique command to quiescence. Assert every original replies exactly once after leader apply, all followers reach the same committed/applied prefix, no pending dependencies/requests/stamps beyond declared bounds. Compare with capability-off control.
2. Pin a schedule: data to both followers; exact durable ACKs; leader commit install; both notices; follower commit installs. Assert **8→6 messages, 9→7 ordinary event steps, 14→12 effects, persistence rounds 6→6** per width-one completed cycle. Effect counts use matched persist completion handling, not an invented Effect::Committed count. Three append and three commit rounds remain. Startup/heartbeats/snapshots and measured concurrent schedules are not asserted to have those exact totals.
3. Drop/delay/duplicate each notice; interleave a new data Append, old notice and heartbeat. Repair through existing ACK/commit path; no wrong reply, duplicate application, invented progress or unbounded retained output. Lost data ACK is a negative control: retry exact ordinary request and do not turn it into a notice.
4. Wrong identity/session/configuration/origin/zero fields; higher/lower terms; matching tip versus missing/mismatching/compacted boundary; higher-term persistence failure; stale completions; source loses leadership after output capture. Verify unchanged commit/authority on invalid input and expected contact/election behavior on valid input.
5. Force capability-off, transfer, learner, uncommitted configuration, joint/Final/retirement, authorization bridge and snapshot-recovery cases. Assert ordinary acknowledged semantics, including alias handling and TimeoutNow contact. A fake Appended carrying the notice context cannot retire another request, advance progress or satisfy a read. Logical authorization rejects it.
6. Mutations worth killing: classify by empty Append; accept notice as Request; infer emission from sequence<=counter; skip boundary match; update progress on local send; omit contact reset; route heartbeat through notice helper; omit stamp invalidation; publish committed interval before persistence; suppress ordinary ACK. Each has a targeted test above.

Use existing full matched successful outcomes/frontiers/phase-failure/shutdown checks as the score guard. R3/R4 `tools-r*/drive.py` is a diagnostic patcher: it only prints unreplied and calls process::exit before normal owned shutdown. Timing alone is not qualification. If retained by parent, apply identical minimal controls to both artifacts: reject unreplied/wrong results, compare exact all-replica committed/applied frontiers and no known phase failure; full matched still owns shutdown qualification. No new instrument PR.

## Commands, evidence and next two

Future execution belongs to parent, through its absolute nice19/core/disk guard; none run here. Existing matched binary:
```text
target/release/voteboat-matched-framework --engine voteboat --clients 1 --batch 1 --server-workers 1|16 --operations 131072 --warmup 1024 --maintenance retain-all
target/release/voteboat-matched-framework --engine voteboat --clients 4096 --batch 4 --server-workers 1|16 --operations 4000000 --warmup 1024 --maintenance retain-all
```
Run original default-retention outcomes too; keep engine/cores/workload equal, including OpenRaft controls if ratios are reported. Existing diagnostic `R3_DRIVE=rounds,1|231|515[,per_request]` and `target/claude/research-r4/{v3g-profile.md,tools-r4/drive.py}` supply the fixed-driver comparison; do not silently treat patched/attributed timings as scored full-path results.

Expected production gain budget: width-one driver roughly **10–15%**, single-16 roughly **3–7%**, allowing exact emission accounting and guards absent from prototype. Bulk likely **0–2%**; singleton one-worker CPU can change sign through parking thresholds, so preserve raw result/instructions/context-switch evidence and causal control. These are hypotheses, not minimum-floor claims. **Do not add** notice and volatile staging estimates: their dispatch/order costs overlap.

Keep baseline/candidate source/artifact hashes, command/affinity/nice evidence, raw stdout/stderr/exits, exact original counts/frontiers, phase failures, effect/message/persist counts, complete shutdown result and interleaved pair table. Existing disabled/attribution runs suffice; no broad profiling campaign. Gate on correctness plus repeatable core/instruction and completed-path gain; investigate a new failure causally, not with unchanged retries.

Run relevant deterministic/public/wire/relay/logical/matched checks, then repository required formatting and strict Clippy for default/all/no-default/native-only and matched's independent lock. Sequential configurations share the parent's single target. CI is background feedback; enabled pre-push hook remains required. Parent records actual evidence, commits/pushes a checked slice and cleans its owned build products.

Only next two, after observing CPU1 residual:
- **CPU2:** collapse volatile **commit-only** staging/completion handoffs at `persist_commit_into -> stage_pending_into -> finish_volatile_into`. Preserve checked revision/generation/install/application ordering; leave append/term transitions and every durable backend alone. No N1d/WAL work.
- **CPU3:** choose residual original-level work: `ProposalBatch::push` capacity-accounting fast path if bulk remains material; otherwise consolidated membership/progress lookup. Do not precommit to a cache/API redesign before CPU1/2 measured residual establishes its value.

Open implementation risks are bounded: (a) exact emission stamps add small work versus the prototype, (b) ignoring higher-term notice replies delays term discovery until heartbeat, (c) static/plain eligibility may reduce coverage under some checkpoint representation—assert retain-all and default-retention reachability, do not relax membership fences for score, (d) retirement/joint/learner paths must remain excluded even if a helper is later reused. These are concrete checks, not reasons to defer implementation.

## Read-only/resource record

Read repository/design-pack AGENTS, README, chapters02/03/04/07/11/17, component contracts, and focus/API/safety/decisions memories. Only this report was written; parent docs/memory/design originals are untouched. No owned build tree was created and no parent target content is to be deleted.

Resource caveat: initial read commands used `nice -n19`, which inherited a negative parent nice and therefore was not absolute19. Once observed, switched to explicit `setpriority(PRIO_PROCESS,0,19)` under affinity0–3; a shell exec subsequently changed nice, so final controllers avoid that shell and verify Python/controller children at19. No builds/benchmarks ran under the earlier reads. Own automatic CUA host pair was identified by this session's 23:43:51 start versus 23:43:50 process creation; cleanup recorded below after writing. Parent/foreign older hosts and shared code-mode host are not owned.

Cleanup: 2159753: verified own 23:43:50 CUA host, set nice19/cores0–3 and SIGTERM; 2159776: verified own 23:43:50 CUA host, set nice19/cores0–3 and SIGTERM; 2159753: exited; 2159776: exited. No owned persistent processes remain if both show exited.
