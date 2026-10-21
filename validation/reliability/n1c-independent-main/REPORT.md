# PR23 independent-main source preparation — qualification pending

Pre-edit [plan](PLAN.md); existing PR https://github.com/KarlLivesey/VoteBoat/pull/23.
Original N1c44 + qualified mainb77 by real merge; no rejected ST2/DR1 production.
Desired source merge `7970d4df1de9e6110973ea935225c6ed9d41f866` has parents
plan `f2170cfefa631a259a84aaa69bd156dcb1d00bf1` and mainb77. The subsequent
history-only join `1d5f692a10d51b80d53e86ed1bb58941b178e2f6` has parents
7970 + publishedb959; both have exact tree `71dd68a076c04a7c817733831327b82a780ebe9d`.
[Ancestry/tree proof](ancestry-proof.json) follows the desired-tree audit and
confirms original44,mainb77,b959 ancestors. No revert-and-no-op-main-merge.

Desired source audit: [source-audit.json](source-audit.json), full protected-path
blob/SHA256 inventory in source-manifest.tsv.gz. The union covers2621 protected paths (2607 present,14 absent rejected-only
inputs). Of these,
2591 equal main,20 equal original N1c,4 are real three-way composition with
exact original executable added/deleted lines,1 differs only in hold comments,
and5 are exact reviewed fixture blobs from66fc. Four compositions are raft.rs,
admitted_replication.rs,progress_broadcast.rs,effect_owner.rs. No other input
changed; Counter and live8B benchmark metadata equal main. No ST2 maintenance,
stall/noop admission propagation,DR1 retention/readiness/session16B code enters.
Hold module/timed runtime/core tests/runtime tests executable identity equals44.

The five test-only exceptions are ports.rs,fixture_isolation.rs,transfer_service.rs
and its private root.rs/root_tests.rs. Pre-existing files match558 preimages;
all five resulting blobs match66fc exactly. These fixes have independent Sol
source approvals A5594849864/B5594850693; their original negative identity
control is historical, not rerun or attributed to this head. The reviewed checked-layout fix is
needed for the known65200+487 overflow; independent directory identity prevents
reuse/delayed-cleanup/stale-store aliasing. No raft comment fromDR1 is imported.
Main/parent PR14+PR15 rejection/closed/parked and PR24 kept/merged records remain.
Original N1c and supersededb959 reports/performance remain labelled historical.

Confirmed nonzero prefixes, data>=batch_floor/notices>=last, exact ack/current-
term/frontier commitment, one request/follower, peer-bounded holds and all
learner/catchup/probe/config/transfer/compaction/reject/heartbeat paths remain.
In-flight extensions are scheduling only. Correlated rejection releases spares;
stepdown/config/close clear holds. Two-voter commit notices may be held; data
requires quorum coverage without the peer. Byte-limit splits may rehold a spare
mid-batch. Frozen fixtures explicitly release, facade heartbeat only follows
owner drain; runtime tests separately cover deadlines/per-group/late poll.
Cancel is appended before transfer starts. No new hold policy/N1/N1d/d2/unsafe.

## Actual focused validation

Every expected named outcome/result/count was audited; all16 required commands
ran nonempty, with0 failed/ignored and no omitted core category.

| Focused set | Default | All features |
| --- | ---: | ---: |
| Core N1c18 + V3e slots/V3f in-place/refusal/retry/EO1 boundaries | 40/0 | 40/0 |
| Runtime hold deadlines/heartbeat/per-group/late poll | 7/0 | 7/0 |
| Original native effect-owner/early-send/retained-charge failures | 7/0 | 7/0 |
| Logical-voter network histories | 2/0 | 4/0 |
| Terminal Cancel-before-transfer | 1/0 | 2/0 |
| Real-core EO1 snapshot ordering | 1/0 | 1/0 |
| Full fixture-isolation target | 9/0 | 9/0 |
| Full transfer-service target | 28/0 | 36/0 |
| **Total passed/failed** | **95/0** | **106/0** |

[Exact commands/exits/counts/resources](focused-results.json), compressed full
logs in logs/. Corrected focus resource audit27564 sampled tasks/threads,0
violations, actual19/cores10-11, <=2Cargo jobs/incremental0, sequential features,
only own root target (including matched later). Focused own target max860202144B.
No hold logic changed; no new mutation is justified or run. Original44 negative
mutations remain historical. No relay BrokenPipe or benchmark-lock repair,
causal attribution or rerun occurred; prior PR15 full failures remain unproven
and are not cleared by this selected gate.

## Launch failures and limits (not silently masked)

The initial core run passed40 assertions but its aggregate resource rc91 is
FAILED: samples saw actualnice-4 despite the initial absolute19 setter. It is
not a compliant gate. The corrected runner restores19 after the guard, in child
preexec and at Cargo exec; the fresh40-test repeat passes with0 sampled
violations. Early inspection shell helpers also inherited the T3 default-4/unpinned state;
subsequent shell commands explicitly set absolute19/affinity10-11. This task
does not receive an all-attempts resource PASS. The compliant repeat gates and
initial failure are reported separately.
The initial default-core raw filenames were inadvertently reused
before archival; only the observed failure summary from the tool transcript
remains, explicitly labelled, rather than invented complete raw samples.
An initial fmt invocation reached no Cargo: runner used a nonexistent cargo
path (rc127); corrected to observed /usr/bin/cargo. Both stock guards passed,
observed priority/affinity compliant, no source or product check ran in that
invocation. This launch error is retained in raw reports.

Parent still owns two fresh pinned Sol reviews, full five-configuration gate
and fair exact-baseline/head performance. No full green/performance clearance,
Done,keep/merge or closed candidate dependency is claimed. No full matrix,
relay/perf/nat.sh/Mac workload/new research/delegation runs in this task.

## Historical performance (not this head's qualification)

Standalone accepted CPU ratios Mac .866/.946/.951/.939/.878, Linux .992
(7/10 pairs<=1, neutral) remain historical, including roughly+5% slow-regime
p99 and the original TCP4k paired-p99 caveat (old table medians18.0/19.9ms,
not a new paired-ratio or clearance claim). No universal latency win is implied.
Later old-mainb4e36472 versus old-N1c44 completed70/70 valid runs,686000writes:
CPU .976676/1.089646/1.250908/1.052628/.879391;
p99 .919071/.964362/.932294/1.174531/.991563. This did NOT clear performance;
oldB lacked A's EO1/V3e, so causality is not isolated. Original four campaign
files remain byte-identicalb9598B evidence in ../n1c-final-n0; only explanatory
README reflects the superseded16B plan. Parent must compare fair exact stable
baseline/candidate using matching qualified-main8B command semantics and flags.

## Strict checks and publication

`sh .githooks/pre-push` passed all7 checks, rc0 in127.569s, zero diagnostics:
root formatting; strict default/all/no-default/native-only Clippy, each locked/
offline/keep-going/all-targets/-Dwarnings; matched fmt/strict all-target Clippy.
[Strict receipts](strict-results.json), log in logs/strict-seven.log.gz.
All8 stock guards (outer launch plus7 Cargo invocations) PASS;5 compiler-finished
records;1416 sampled task/thread rows,0 violations. Environment: absolute19,
cores10-11,2jobs,incremental0,dev/testopt0/debug-symbols0, no RUSTFLAGS override.
Own target1163328620B (1.084GiB), below60GiB; global stock caps unchanged.
`core.hooksPath=.githooks`; hook executable and byte-identical main. No hook,
lint or cap weakened. The actual nonforce push repeats this enabled hook; its
exact final-head/push/hook/remote/cleanup receipt is recorded in parent REPORT
and existing PR CURRENT/comment6304682074 after publication, avoiding recursive
self-SHA evidence. No source changes after checked join1d5, only docs/evidence.

## Ownership and remaining work

Only this T3 checkout/target were used. Main source/docs/memory, original design
pack, foreign conformance, shared port namespace and other owners' processes
remain untouched. Raw reports only are in parent
`target/claude/research-r4/pr23-independent-main/`. Parent owns fresh exact-head
source approvals, full5config/exhaustive relay/doctests and fair exact-head8B
performance; pending, not Done/keep/merge. Cleanup after checked publication is
mandatory own sweep/process audit/stock guard; actual outcome is recorded in
parent raw REPORT and durable PR CURRENT/comment to avoid a self-referential
post-push evidence commit.
