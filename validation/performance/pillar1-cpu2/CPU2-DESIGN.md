# CPU2 implementation contract and evidence

Base: docs-only main `03f3ffd9d013ee02e115fb5c17cd256b4372eea5`, production
`0bf9271ecb5a34c4089c6bb10381303232624170`. Branch `cpu/p1-volatile-turns`.
This is one bounded pillar-1 candidate; no performance or integration acceptance.
CPU1/PR25 is rejected and none of its source is used. The original design pack
is immutable. Public package licence remains RPL-1.5.

## Accepted design inputs

Read the complete parent `target/claude/pillar1-20261022/CPU2-DESIGN.md` and
`CPU2-PARENT-ADDENDUM.md`, repository/pack AGENTS, original README,
chapters 02/03/04/07/11/17, component contracts and the six named memories.
The addendum overrides the architect's resource cleanup examples: harness,
Codex, T3, code-mode hosts and ancestors must never be adjusted or killed.
Only proven task-created ephemeral children and this isolated checkout are
cleanup candidates. Reports/raw commands belong in the parent's designated
`target/claude/pillar1-20261022/cpu2-builder` directory, not tracked main.

## Schema plan before implementation

The public selected event and ProposalBatch methods consume their inputs,
accept the matching `VolatileLog` and an empty reusable `VecDeque<Effect>`,
and return a core-constructed `VolatileTurn` with immutable committed-range,
FIFO-position and completed-local-unit accessors. Queue/mode refusal is typed,
pre-admission and leaves state/queue untouched; consuming input ownership is
explicit. A mismatched binding is completion-domain misuse: fence, discard
pending dependencies, preserve last installed state, release no output.
Busy/Fenced and ordinary protocol/input errors retain existing semantics.

One private scoped delivery context holds the exact incarnation/revision/
generation due key and the remaining bounded local allowance. Its guard restores
Host dispatch on return/error/unwind, before constructing a public borrow. No
global/thread-local/unsafe code, callback, new dependency or resource owner.
The existing Pending owns the same checked plan throughout the event body and
finish hook. Only then can the existing `finish_volatile_into` and sole
`install_pending_plan` install it. There is no new commit-release rule and no
durable amendment. Installation/continuation errors truncate this turn's
outputs, fence as completion failure, and retain actual last installed state;
there is no fictional rollback of a successful earlier installation.

Selection before entry requires an unfenced static volatile recovery domain,
no dependency/read/readiness/authority/transfer state, and an ordinary proposal
or same-term Append/Appended. Static recovery already validates the physical
bootstrap policy and forbids configuration ingress; do not rewalk voters or
rescan entries to recreate that authority. Weighted/recursive policies use the
unchanged frontier. At stage, inspect the sealed plan kind and scalar metadata:
only same-hard-state/ballot-origin/generation extension or commit plans qualify.
Full replacement and all snapshot/configuration/control work use existing Host
dispatch with the same pending plan, without replay. Compacted static logs may
qualify: base zero/absence of a snapshot is not a prerequisite.

The stage queue must also be empty. A preceding Send retains the public token
at its original FIFO position, so failure prevents later commit installation.
Completion returns its interval at the pre-completion queue length, preserving
the existing helper's treatment of already queued effects. The matched consumer
applies a position-zero borrow through disjoint application/pending fields;
a cold nonzero interval is materialised at exactly that position before any
host effect that can mutate the core. There is no unconditional hot-path Vec.
Client success still requires actual ordered application and original receipt.

## Audited actual completion graph (before editing)

* `step_into -> step_effects -> begin_step -> receive_into/propose_into ->
  persist_into -> stage_pending_into -> finish_step_into`. In particular,
  `receive_appended_into` examines Pending after `maybe_commit_into`; eager
  completion inside staging changes its send/context behavior.
* `propose_entries_into -> stage_entry_batch -> begin_step -> role/batch
  validation -> persist_proposal_entries_into -> finish_step_into`.
* `finish_volatile_into -> install_pending_into -> install_pending_plan ->
  SummarizedLog::install -> OwnedNextLog::install_in_place`. Exact key and
  binding checks precede this sole checked installer; stamps/predecessors remain.
* `After::Reply` emits the already prepared reply; `After::Commit` broadcasts
  commit progress. Neither stages another plan. `After::LeaderAppend` updates
  local progress, broadcasts data, then invokes unchanged `maybe_commit_into`.
  That can stage only one scalar `After::Commit`. Campaign/compact/snapshot
  continuations are outside selection. Thus at most two candidate installs and
  one nonempty interval can exist in this graph.
* Singleton leader proposal: append then scalar commit, at most two installs.
  Multi-voter turn: one local allowance. A self-quorum weighted continuation
  keeps its second scalar commit as a real Host dependency; preceding sends
  independently require this. Even when all peer requests are already active
  and broadcast emits nothing, the explicit exhausted allowance defers that
  scalar commit with its normal token (no lost/truncated pending work).
  Any other continuation or multiple intervals is a boundary violation to
  stop and report, not a reason to merge intervals or widen the graph.

## Consumer/source audit and acceptance plan

Changed consumers are limited to matched Actor's event and collected-proposal
public callsites, ordered interval consumption and honest local-unit accounting.
Six local installs remain six `persistence_units`; external handoffs are counted
separately in focused histories. Scheduling, ready collection, topology,
workload, queues, command identities, envelopes, maintenance, warmup and owned
shutdown remain unchanged. A public-only downstream Counter host must exercise
both APIs, actual application/results, fallback, bounds, two independent groups
and host resource lifetime. All production/native/durable/logical consumers keep
their existing public calls. A complete search audit will record their paths.

Planned focused acceptance (not executed evidence):

| Area | Checks required |
| --- | --- |
| Ordinary selected/deferred | Exact histories B=1/231/515: same logs/roles/commit/application/frontiers, contexts/messages/order/results; 8 sends, 9 public events, 6 installs, 3 application intervals; external helpers 6 versus 0. Force Host mutation restores 6 without workload changes. |
| Policy/bound/visibility | Weighted and recursive insufficient-flat-majority controls, exact evidence release; singleton two-install bound; weighted self quorum with failed preceding send; pending visibility and extra-send/context controls. |
| Correlation/partial failure | Binding/incarnation/revision/generation, changed/foreign plan/predecessor, duplicate token/recovery; fencing and last successful partial state; validation-disabled mutation dies. |
| FIFO/refusal | Preceding Send/application failure/dropped wait; apply-before-position mutation dies; queue/mode/Busy refusal/fallback; no early output/runtime delivery. |
| Feature preservation | Focused durable membership learner/joint/final/retirement, transfer/read/readiness/authority, identity/content/dedup, logical original Written then consumed Durable/child release, ownership lifecycle/snapshot/native bounded public two-group tests. Distinguish actual execution from unchanged-source inspection and future full-suite coverage. |
| Default retention | Actual default Actor maintenance through at least two 5000-entry checkpoint triggers, B=1 and wide; advancing base, retained 1000 suffix, pins/reconcile, all-three selection before/after; lagging replica snapshot catch-up and resumed selection. Reachability-disabled mutant dies. |

Focused green precedes root formatting/four strict Clippy configurations and
matched formatting/strict Clippy, all locked/offline, sequential, zero diagnostics.
Publish a checked slice through the enabled actual seven-check pre-push hook,
nonforce push, draft PR against main and immediate thread link. Parent owns
fresh dual reviews, fixed-driver nine pairs, conditional canonical four cells,
full five configurations and integration. No full/heavy relay/Mac measurement
or CPU/allocation/instruction gain claim belongs to this builder.

## Implemented evidence

The selected public methods are `Raft::step_volatile_into` and
`Raft::propose_entries_volatile_into`, returning `VolatileTurn`. Ordinary
`step_into`, proposal and persistence APIs remain functional. Private `Delivery`
and its scope guard hold only the temporary due key/allowance. The same Pending
and `finish_volatile_into -> install_pending_into -> install_pending_plan` path
performs every checked install. No new provider or replaceable subsystem was
introduced: these methods extend the existing volatile event/completion seam.

Plan representation correction from actual source inspection: `append_eligible`
also admits changed ballots on plain histories. `is_append_or_commit` names
representation only; the separate same-hard-state, ballot-origin and generation
checks are mandatory. A test explicitly exercises that representation with a
higher ballot and requires the unchanged Host token, pending state and later
normal completion. Initial fixture errors (ReadRequestId constructor, then two
incorrect representation assertions) and original sources/patches are retained
in raw reports. Assertions were corrected against this actual contract; no
installer, predecessor, validation rule or lint was weakened.

The actual completion graph remains the audited graph above. Tests observe
Pending before the original callback returns, restored Host dispatch on unwind,
one multi-voter local append followed by a retained self-quorum commit token,
two singleton installs/one interval, and a synthetic nonzero FIFO position after
the original finish-hook stage. A failed continuation retains the successfully
installed append while fencing and releasing no outputs. Borrowed delivery
ends before host core mutation. Queue/mode refusals precede admission; wrong
binding fences even already accepted pending work. Exact key corruption,
changed predecessor and stale/recovered token checks fail closed.

Final production build-input manifest (1535 paths):
`137193155770f08f8eee348aecc275782086cda7f7d8de7aea96f65fc098ed4b`.
The full manifest, source patch, commands, exits, mutation inputs/output and
resource samples are in the designated `cpu2-builder` report directory. Exact
published commit/PR and final cleanup receipts are recorded there after push.
Cargo manifests/locks, toolchain, feature definitions, lint configuration, hooks,
wire/RPC, scheduler, runtime ownership and durable/logical pipelines are unchanged.

### Executed focused semantic matrix

Counts below are actual root-default and normal matched-workspace focused
execution, not five full configuration suites. Final total: **621 passed, one existing timing report
ignored, zero unexpected failures**. The ignored case is
`logical_voter::application::tests::formats::restore_cost_report`; no timing
measurement or performance acceptance is claimed.

| Feature / consumer | Actual focused execution | Inspection / remaining boundary |
| --- | --- | --- |
| Public static ordinary embedding | Public-only `tests/volatile_turns.rs`: 5 passed, including B1/231/515 exact deferred/selected logs, roles, requests/messages/order, application/frontiers and Counter receipts; two groups use both APIs, proposal-context validation and shared host-owned application telemetry | 8 sends/9 public events/6 installs/3 intervals in both arms; external helpers 6 versus 0; no ancestor step/resource owner |
| Weighted/recursive policies and local bound | Same public host: flat majority fails weighted and depth>1 policies, changed request context cannot commit, exact node3 evidence releases; singleton 2 installs; weighted self-quorum actual injected send error leaves later commit pending/external | Existing policy frontier and current-term commit check unchanged; no new release rule |
| Correlation/pending/FIFO/failure | `--lib raft::`: 194 passed, including 10 new selected-turn tests, existing foreign-plan/stamp/predecessor and install-in-place tests; matched actor tests: 28 passed, including FIFO send/application failure and selected dropped-original/application failure | All fallible work precedes public borrow; cold materialisation at the precise FIFO position only |
| Durable membership/learner/joint/final/retirement | `tests/membership.rs`: 32 passed; focused Raft configuration/retirement histories above; `tests/volatile.rs`: 11 passed, including durable/volatile refusal and compaction parity | New selected API refuses nonvolatile mode before mutation; existing durable member callsites stay on original methods |
| Transfer/read/readiness/authority/identity | 11 public leadership histories, 15 public read histories; 9 direct readiness tests including compacted actual worker exchange; 2 host/native witness-authority tests; Raft source/session/request/permit/retirement controls above | Public selected transfer/cancel/higher-term and read-in-flight fallback executed; no supported control operation becomes an unsupported fast-path error |
| Logical Written/consumed Durable and child release | `--lib logical`: 132 passed/1 timing report ignored, including both original Written-ticket tests and 11 logical-release capacity/classification tests; one actual three-node child-cohort original client/read consumed-capsule history passed | `runtime.rs` still refuses volatile registration; one explicit refusal test passed. No logical assembly converted to volatile |
| Ownership/lifecycle/wrappers/dedup | Application 23, scoped source 5, metadata transfer 42, import target 9, publication 7, activation 9, logical activation application 7, transaction application 6 passed | Source fence/export/import/publication/activation, checkpoint/retry and context forwarding exercised; no new split/merge roadmap claim |
| Snapshots/default retention | Snapshot 27 passed; actual matched all-three B1/B515 selection before/after two normal5000 checkpoints, advancing base/exact1000 suffix/pins/reconcile passed within actor28; actual lagging follower snapshot install and resumed all-three selected writes passed (1) | Original `maintain_inner`, threshold, retention, pin/reconcile and owned shutdown unchanged; no retain-all or fresh-core substitution |
| Native/public composition and lifetime | Provider conformance 42 passed; runtime pending-dependency isolation and shared-store/other-owner drop each passed | Actual native/durable/logical consumer source audit below; complete configurations remain parent-owned |

Counts are per focused execution selection and exclude the intentional mutant
failures. The ordinary public host tests materialise intervals for inspectable
history; the actual matched consumer uses the borrowed hot path. No adapter-only
shortcut or test-only route is used by the matched production callsites.

### Causal controls and source consumer audit

Five intentional mutations each compiled and failed its required behavioral
assertion; original bytes were restored after each. Forcing Host returned the
same history and `(8,9,6 external,6 installs,3 intervals)`. Premature installation
produced an extra commit Append/reply in the exact history. Disabling predecessor
validation violated fail-closed refusal. Applying before FIFO position advanced
application from1 to2 before a failing send. Requiring base zero lost actual
selected coverage immediately after the first checkpoint (`next=5000`, local
units0 instead of2). Raw failed assertions are preserved; these are controls,
not outstanding candidate failures. Final control receipts refer to the frozen
build-input manifest above.

Only matched `actor.rs` event and collected-proposal callsites select the new
public seam. Its `finish_turn` applies through disjoint application/pending
fields or cold-materialises at the exact FIFO position. `persistence_units`
adds `completed_local_units`, so six installations are still six units. A
cfg(test)-only counter verifies selection through retention; it adds no release
counter. Attribution places folded core work in Core; PersistHelper phase alone
must not be presented as a CPU saving.

Search/read audit retained in `consumer-audit.log` and
`source-matrix-audit2.log`: `runtime.rs` event ingress, `runtime/ready_batch.rs`
batch ingress, `runtime/effects.rs` persistence and `runtime/replica.rs`
completion dispatch retain their original methods/leases. Logical bridge
`bridge/{release,consume}`, persistence progress and original capsule consumers
retain Written versus consumed Durable semantics. `snapshot.rs` retains its
ordinary volatile helper for compaction/import/install. Native/service and
standalone `examples/framework_benchmark/cluster.rs` still use their existing
owned/durable methods; they are not silently selected or relabelled volatile.

### Publication and qualification boundary

All seven prepublication strict checks passed via the unchanged enabled hook:
root fmt, four default/all/no-default/native-only locked offline all-target
Clippy checks with `-D warnings`, matched fmt and matched locked offline
all-target Clippy. Zero diagnostics; no lint/threshold/hook change. Actual
nonforce push repeats the enabled seven-check hook after commit; its receipt
and exact remote head/PR are recorded in the final safe report.

Parent still owns fresh dual Sol source reviews, the first fixed-driver nine
pairs and its adapter/telemetry control, conditional canonical four cells/eight
pairs, and all five complete configurations. No full suite/heavy relay/Mac
benchmark, instruction/allocation instrument or performance campaign was run.
This bounded candidate is not merged, goal-qualified or roadmap-complete.

Resource limits: direct functional child execs set absolute nice19 and cores0-3;
Cargo jobs4, incremental0, debug symbols0, normal debug opt0/assertions preserved.
The corrected stock guard and actual Mac SSH check remain unchanged. All builds
use only this checkout's normal `target/`, explicitly shared by matched Cargo;
observed size remained below15GiB. Initial inventory ran before containment;
harness processes were never retuned/signalled. Initial oversized reads were
re-read in bounded form. Two same-configuration focused invocations overlapped
once (no configuration change/executable overwrite); all feature configurations
and subsequent focused runs were sequential. These limits are disclosed rather
than claiming whole-task historical containment. Cleanup removes only this own
target/checkout/registration after durable publication; uncertain harness
processes are left alone and reported.
