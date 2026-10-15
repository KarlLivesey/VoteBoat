# Client reconciliation audit for the next P9 candidate

Read-only audit; no production edit, build, test or measurement was performed. The historical479 Linux reconciliation context is 14.306% of native perf-period weight (11.827% of sample counts), after saved-data Samply recovered ancestry. It is a whole-launch, offer-capped profile with at most 65 pending requests. It is not a current480 profile or proof of the largest remaining481 cost.481 reduced loaded CPU/poll work but has mixed rates/tails; the full P9 objective remains open.

**Finding:** the repeated scans can be collapsed for ClientRouter, because its owner is immutably borrowed for the whole call and its per-ticket transition reaches a local fixed point. A plain `min(visits, pending.len())` is not equivalent. The smallest conservative option is one actual pass, a conditional second pass for newly published takeover-eligible Unknown outcomes, then projection over stable rotations. This can keep every externally visible outcome, reservation, cleanup opportunity, returned logical-visit count and cursor position, without a persistent cache or new allocation. It requires the equivalence checks below before implementation.

**Current contract and call sites.** `clients.rs:707` checks exact owner identity/authorization before the65536 visit limit, then cyclically chooses the next pending sequence strictly after cursor, wrapping to the first live key. It sets cursor and increments the returned count before checking that ticket. Empty pending stops the loop. With an unchanged nonempty population, `reconcile(owner,128)` currently returns128 even when it rechecks the same one ticket128 times. The return is a count of loop visits, not evidence of newly useful progress; there is no separate explicit public prose defining it as distinct tickets. Preserve its numeric behavior if changing the implementation; document any folded repetitions as legacy logical visits rather than reporting them as physical checks.

The physical and logical ReplicaDriver poll paths call client reconciliation after owner advancement, effect collection/retry and persist submissions (`replica.rs:534`, `replica/logical.rs:105`), with default reconcile budget128. They discard its count. Failed-owner cleanup calls it with65536 (`replica.rs:996`) and also discards the count. Direct embedders can call the public method and may use the returned value; changing it merely because current in-tree callers ignore it would change the composition contract. Existing selected downstream tests call reconciliation with1,4,64,128 and selected quantum sizes, but do not provide a complete repeated-visit oracle.

**Per-ticket state transitions under this immutable owner.** No owner callback, provider admission, application callback, I/O or await occurs in ClientRouter::reconcile. EffectOwner::is_failed reads its failure field; core returns an immutable Raft reference. Owner failure, role, term, retained entry term and pending-dependency status therefore do not change inside the call. External workers may finish concurrently, but their completions cannot be delivered into this immutably borrowed owner until a later mutable-owner call. Binding/generation/session checks stay first. The algorithm can still mutate router fields:

| Condition at a visit | Router effect | Repeat significance |
| --- | --- | --- |
| Owner failed | Mark terminal, publish OwnerFailed only if no output, cleanup | One visit reaches its fixed point |
| Not failed, existing output, exact takeover proof | Mark terminal, cleanup | Released completion may be removed on this visit |
| Leadership changed, no output, takeover false | Publish LeadershipChanged once | Later same-call checks are stable |
| Leadership changed, no output, takeover true | First publish sets has_output; a second visit sets terminal | Must preserve whether the old budget reaches that second visit |
| Healthy stable owner, no new output | No new transition | Repeated immutable lookups are redundant |

Takeover is exactly the existing predicate: stepped, position present, no pending Core dependency, and either a retained matching entry term or a strictly newer current term. Do not replace it with commitment, a compacted-base guess, an operation-ID match, position alone, or owner liveness. Pending command bytes, identity, admission and position are not mutated by reconciliation.

`publish` is sticky: it adds exactly one ready sequence and never overwrites an existing Applied, NotProposed or Unknown outcome. `cleanup` removes only released AND terminal entries, removing their admission/position indexes and whole global/group/bulk count/byte charges. `poll` exports the stored outcome; `complete` sets released and invokes cleanup. `cancel_wait` publishes an observation-only Unknown without making the original operation obsolete. Close does not replace fencing; abort explicitly fences and marks all pending terminal. No proposed optimization may turn Unknown into NotProposed or reclaim a reservation from cancellation alone.

**Concrete counterexamples.**

1. One valid stepped request has takeover=true, no output, and the immutable owner has changed leadership. Old reconcile(visits=2) publishes Unknown on visit1 and marks terminal on visit2. It does not remove the newly published request during that call, because a newly published output cannot already be released. Immediately following `poll`/`complete` then releases the full reservation. A one-pass cap leaves terminal=false, so that same host sequence retains capacity until another reconciliation. The observable regression is cleanup after host completion, even when no map removal occurred inside the first call. Conversely, eagerly marking terminal on the first visit changes the old visits=1 behavior.
2. A blanket two-visits-per-ticket transformation also changes partial-budget order. With two fresh Unknown/takeover tickets and visits=2, old order visits each once and marks neither terminal. Finishing the first ticket twice would change output fairness and its subsequent cleanup timing.
3. Returning distinct count or leaving the cursor at the first-pass end changes repeated-budget/fairness behavior. With live keys10/20/30, cursor5 and visits=4, stable old order is10,20,30,10, return4/cursor10. One-pass return3/cursor30 changes the next one-visit call.
4. A modulo-zero cursor shortcut fails when the first-pass final key was removed. With keys10/20/30, cursor15, first order20,30,10; if10 is released and removed on its visit, survivors20/30 and cursor10 remain. Two further old visits end at30. Computing remaining%survivors=0 and keeping the deleted cursor10 differs from the old final cursor30. With the same fixed survivors their immediate successor is coincidentally the same; after a new larger sequence40 is admitted between calls, the old cursor selects40 but the stale cursor selects20. Preserve the actual final cursor, not merely today's successor.
5. OwnerFailed precedence and sticky outputs matter: a CancelledWait already exported/completed stays that original outcome while owner failure can make its retained reservation terminal and removable. Unread Applied/NotProposed outputs must remain held until actual completion, even under failure. Marking all failed entries released would fabricate consumption.

**Recommended bounded schema: preserve the old trace while folding fixed-point rotations.** No public data/API shape needs to change. Keep scalar local state only: initial pending length N, original budget, actual visits, remaining budget, and whether a fresh Unknown+takeover transition requires a later visit. Retain the existing per-ticket transition/cleanup order for all actual visits.

- Perform up to min(budget,N) old visits. There are no insertions, and cleanup removes only the currently visited key, so this covers each initially live sequence at most once before wrapping. If the budget is consumed or map becomes empty, stop exactly as before.
- A new output implies released=false: only `complete` writes released, it requires an exported output, and no host completion can interleave with this mutable-router call. Thus a request that survives this first full pass cannot become newly removable during later same-call passes. Existing released, takeover-eligible/failed entries were already removed. Survivors and their order are now fixed for the remainder of the call.
- Only fresh Unknown+takeover can still change terminal at its next visit. If any occurred, run up to min(remaining,current survivor count K) further old visits. Preserve the same cyclic ordering and partial budget. After a full survivor rotation, every remaining per-ticket transition is idempotent. If none occurred, the fixed point already follows the first pass.
- Fold only the remaining stable visits. Preserve the old returned logical visit count. For remaining R>0 and K>0, advance the final cursor by `((R-1)%K)+1` successors from its current position over the fixed live set. This handles a deleted cursor and whole rotations correctly; it does not require R owner/core checks. A bounded ordered iterator can find that successor in at most K steps. R=0 and K=0 need explicit branches, with no modulo-zero, arithmetic overflow or stale-key indexing.

This performs at most one original-population pass plus one survivor pass, then at most one key-only cursor projection. The no-transition healthy path needs only the first pass. It grants no new durable authority, stores no revision across calls, adds no pending ownership and requires no per-request scratch allocation. Its main expected scope is low pending populations with budget128; if N>=budget, it does no fewer predicate checks. Do not promise a loaded-backlog throughput gain from that mechanism alone.

An exact one-predicate-pass alternative could retain per-entry eligibility and project only second-visit terminal changes, but needs bounded scratch/state or more complicated ordering/rank bookkeeping. It is a larger ownership/accounting plan than the constant-memory conditional second pass. A simpler one-pass API with eager terminal normalization and distinct-ticket return is possible only as an explicit lifecycle/return-contract change; it does not meet the requested exact capacity/cleanup preservation and is not recommended for this slice.

**Read path is a separate case.** The similar cyclic method is ReadRequests::reconcile in `read_requests.rs:906`, not the ReadRouter executor in reads.rs. It takes a mutable owner, publishes leadership unavailability and, for stepped observed reads, admits a tracked CancelRead. That can mutate owner queues, scheduler readiness and admission sequence, and error out after partial router progress. A ReadRequests cleanup additionally requires no outstanding cancellation and releases its active-per-group query slot through terminal(). Therefore the immutable-owner client proof does not transfer.

A concrete interchangeable-provider counterexample is a ReadyScheduler whose enqueue rejects Overloaded once without adding readiness, then accepts a repeated request. The public scheduler contract requires rejected enqueue to leave readiness unchanged, not every later retry to return the same rejection. With one stepped/canceled read and visits=2, old reconciliation can enqueue the exact CancelRead on the second attempt; a cap at one misses that same-call transition. FairScheduler happens to have deterministic capacity rejection until its state changes, but that cannot justify changing all providers. Successful cancel admissions also compete for global/control capacity across groups. Keep ReadRequests unchanged unless separately proving admission retry/error order, exact original-read-before-cancel order, stale cancellation handling and active/query/result credit lifetimes.

**Acceptance checks to propose, not executed here.** Preserve a test-only original loop as the independent reference. Compare complete router state, ready order/outcomes/tickets, all usage/index maps, cursor, and returned count after every action, not only final Counter values.

- Actual stepped/durable histories for the one-ticket leadership-change counterexample at visits1 versus2, and the two-ticket visits2 versus3 boundary. Immediately poll/complete to detect capacity timing differences.
- Existing CancelledWait versus Unknown/Applied/NotProposed stickiness, externally held versus completed envelopes, failed owner, retained matching entry, newer term without an entry, pending dependency blocking takeover, and compaction without fabricating same-term proof.
- Zero/empty, invalid owner/authorization/generation/session, visits65536/65537 and validation order; budgets around N and survivor-count boundaries, partial second rotation, and the deleted-cursor/full-cycle counterexample. Verify the next one-visit call both without and after a newly admitted greater sequence.
- Mixed groups and ordinary/control reservations: exact bytes/counts, group/bulk maps, admission/position correlation, no duplicate publication, close/abort/discard_failed, physical and logical ReplicaDriver callers, and restart's fresh router/owner identities. Inter-call Core changes must be reconsidered, never cached.
- A finite generated valid-state/history matrix against the old loop, with no output released before it exists. Do not mistake invalid private flag combinations for public-history equivalence evidence.
- Focused unchanged read cancellation/recovery regressions to ensure the client-only change did not touch ReadRequests. If ever optimizing reads, add the transient scheduler rejection counterexample first.
- Then unchanged enabled formatter/four strict Clippy profiles/standalone checks, publish the checked slice, fresh exact artifacts and paired useful rates/tails/refusals/fairness. A current native profile is needed before calling this the largest remaining stage. No new instrumentation framework, broad helper prerequisite, default change or timing is authorized by this audit.

**Input identity.** Audit observations are source/contract reasoning, not executed counterexample tests. Hashes/modes below identify the actual read inputs; the referenced tests are existing evidence only and were not run here. Some exploratory reads used nonexistent guessed paths (`reads/observe.rs`, `docs/component-contracts.md`, `runtime/shard.rs`) and failed harmlessly; actual files were found with rg and are listed below.

- Inspected Git HEAD: `875666103aa26e72bd923fc366cb123b497faa85`.
- Recorded UTC: 2026-10-15T15:13:57.881536+00:00.

| Input | SHA256 | Mode |
| --- | --- | --- |
| `AGENTS.md` | `b851fc08f1108db77964b3aac903387bcd28c665470fd2e3219560543f77ff82` | `0o644` |
| `VoteBoat-Design-Pack/AGENTS.md` | `1e3f5bc2ea8263c16d45a26047d13310eb4e37a03cd7248106a5a2a06fc7e426` | `0o644` |
| `VoteBoat-Design-Pack/specs/component-contracts.md` | `a522f73081d50591d4881c6cbcb4bf137ca648becdd228c977949028387beccb` | `0o644` |
| `src/runtime/clients.rs` | `37b983a1b67f7f5496b74ee65332c702b7a1e74554077af100e170fdfae07c6c` | `0o644` |
| `src/runtime/clients/ready.rs` | `430eabeda3d3042b5d5253660b1785256ed346bc5b6ce47a5ba71b2bcaf7b6e9` | `0o644` |
| `src/runtime/clients/observer_clear.rs` | `c6a278d611210be81d276cf6bca178a39a631c6ffc7595c7b00e7edfbf03aa76` | `0o644` |
| `src/runtime/clients/tests.rs` | `d0095e07063ab5c5e050a0dba951e36f8ff28f3b3b43b9da1051b6fcefdd8851` | `0o644` |
| `src/runtime/clients/tests/lifecycle.rs` | `2b117897b267eaee8c50e397d224dcf5127a933653a7103c0285f6d62d1eb478` | `0o644` |
| `src/runtime/read_requests.rs` | `98e268b03002ccce39709fc54940cdebb4ff460b81dacc596a1448e5f9c22955` | `0o644` |
| `src/runtime/read_requests/tests.rs` | `af53862b8cf91e1360e62007b7be01e63d8d91d41e4c452e5c03fadf371fe4b0` | `0o644` |
| `src/runtime/read_requests/tests/lifecycle.rs` | `3ab6f412658b10ef8b98406a55236e44b743bff987e9b3ae05ee12e1390cad00` | `0o644` |
| `src/runtime/reads.rs` | `17eab8f6063013f876b9b9c5b511a3a3db1d5d24605bf3ee8ed414a1afe826cf` | `0o644` |
| `src/runtime/effects.rs` | `ce4e2f7909f938159df1bb6c01baae0e825c4c3faea252c02c74ca4dff916a50` | `0o644` |
| `src/runtime.rs` | `22b6a4e63c62445ef6067da067ea51411cf8e6465ea067836b02bef06b45a9b6` | `0o644` |
| `src/runtime/timed.rs` | `5699b09dd660dee9c174d7edd333ed9960d6dfadee3fd0e1959de4e8a00cef9d` | `0o644` |
| `src/runtime/replica.rs` | `741208e04f4b35ce6d569605e4fc225a3abdcc9953280f097930f640a6cfba1d` | `0o644` |
| `src/runtime/replica/logical.rs` | `ff6041f13cf19a2e5e9809ecbb78a0d53ac0e09d52ce8f01933b760265f0a77c` | `0o644` |
| `src/native/runtime.rs` | `91282b74deb11e814210383d5baec89785f2b9e6dd26d22964acf5d6788241d4` | `0o644` |
| `tests/effect_owner.rs` | `79c884a6a373dba5513a7b55501a3dc7d801265e7ecb5c4ac3ad8713767fbf0d` | `0o644` |
| `tests/client_admission/ready_batch.rs` | `13bfb07dd56cbd0827060f977955fd818fcd7027f6fbc101944a18d1819aa39b` | `0o644` |
| `tests/client_admission/ready_batch/budget.rs` | `4a51cacebb5dda8ed42dbe65d21435478dd5b603b08b1f84bc8827cbc645dd22` | `0o644` |
| `tests/client_admission/ready_batch/wrapper/checks.rs` | `94195184353b30f6f773f6c7f4040d2c92c8d1a7b281fe09afb7e19014819ec2` | `0o644` |
| `docs/component-contracts.json` | `976165b5e6b0dcc247b7fe75fb01c20dc11d6eb9a0d86c26c2a6aff1df12f871` | `0o644` |
| `docs/provider-conformance.json` | `f27a1ecca9e848d233a0da18d223d484237f8d9f2ec2ac5463ee7f3361418c6f` | `0o644` |
| `src/raft.rs` (pure pending-dependency getter follow-up) | `63f20f9f5ef04e6a29d6aebe27e35c84ad1c3a499a3854d591a5fcee35b3d91f` | `0o644` |
