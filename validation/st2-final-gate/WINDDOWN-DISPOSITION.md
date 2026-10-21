# PR14 — Parked/rejected, not Done

Final disposition,21 October2026: parent exercised delegated keep/reject authority
to reject/close this existing candidate as parked. This agent records branch-only
evidence; parent owns closure and the main record. No merge/main/close grant.

Final checked source: **3aed5d671e735f7cd622b4ae4441a54ca5b3d5e7**,
parent219ad7bb8489c19012c8298d1021ddd959861782, cumulative main
bd714771e181d59c2cfaece3fc93dc411ce41ddb. The evidence commit containing
this record changes docs/nonbuild validation artifacts only. Its exact SHA and
enabled-hook publication result are recorded in PR CURRENT and the main safe
report directory after commit; no self-referential commit hash is asserted here.

## Rejection reason and acceptance limits

The complete final cumulative default gate FAILED; the sole failure remains
unclassified after two explicitly bounded diagnostics. No cause-specific repair
is justified. Source approvals and accepted performance history cannot clear a
failed full gate. Later all-feature/no-default/native/matched full configurations
were held; no final matrix is claimed. No further implementation, diagnostics,
full suites, roadmap work or main writes are authorized for this wind-down.

| Run/input | Actual result | Limit |
| --- | --- | --- |
| Full final default, clean published3aed | **2667 passed /1 failed /1 ignored, outer rc1**; Cargo build rc0/build-finished=true;129/129 all-target artifacts each executed once; no missing/extra/duplicate exits or abort; separate docs0/0 rc0 | Sole failed binary startup55/1 rc101,817.21s; absent-follower case at backpressure.rs253: `hang at 8576 ms`, unchanged600s wall guard. Existing panic emitted no phase/count/last real progress age/node/worker state. Original cause is unproven |
| Same full run, other evidence | Native benchmark58/0; routed110/0 rc0,1668.64s; complete normal-debug exhaustive relay9/0 rc0,3242.37s, unskipped alone core12; both retention cases and other backpressure cases passed | These passes do not cancel startup failure. One ignored existing library timing report is not an exhaustive relay omission |
| One isolated absent-follower diagnostic, base3aed + preserved patch |1/0 rc0,290.07s; all49152 Applied/window64, term1, zero refusals, max retained10284<16384,5 checkpoint advances; write quarters at host51.361/118.648/195.902/289.994s; joined289.998s, last real Applied age3ms | **INCONCLUSIVE** for full-run failure; isolated cores12–15 differ from original within-binary context13–15. Original5000ms protocol/600s wall/cap/cleanup checks unchanged |
| One full startup context diagnostic, same exact patch/binary |56/0/0 ignored/0 filtered, binary and aggregate rc0,484.24s; all56 original names once, no missing/extra/duplicate; original argv(no args), affinity13–15, normal libtest concurrency | **INCONCLUSIVE**, not qualification or causal proof. RUST_TEST_THREADS currently unset/inherited with no override; original runner also inherited without override, but historical literal value was not snapshotted. Three concurrent test threads observed. Default capture suppresses passing diagnostic receipts; no guard fired or failure-state evidence obtained |

The isolated diagnostic observed finite progress: both nodes term1, checkpoint
bases advanced, and quarter/drain samples had persistence/snapshot/connector
usage0. Its election predicate initially returned before the leader noop was
durable (last/commit0,pending_dependency=true,worker1); unchanged setup, no
causal attribution or setup repair. These observations belong only to that
isolated run and must not be imported into the original failure/context run.

No clock admission, yielding, setup-noop, serialization, timeout, retry, public
API or library changes were made in either diagnostic. All original assertions
and cleanup stayed intact. No repeated unchanged retry series was performed.

## Preserved inputs and evidence

[DIAGNOSTICS-ONLY.patch](DIAGNOSTICS-ONLY.patch) is the exact SAME captured
input used by BOTH diagnostics: source/docs observations only, uncommitted at
execution, SHA256 **7bfa83cb786169884304c8203fc3d06c265d4fe17f65fa57dc3b9c9819817c8d**.
It is stored once as a nonbuild artifact, not an applied or qualified repair.
Existing diagnostic binary SHA256 was
ce00bcd2e5aec70c33cff47135157c3b239724f61b24113a4431232031d74d48.
After preserving it, only its two uncommitted build-input/doc edits were restored;
this disposition then updates docs. All src/tests/examples/Cargo inputs match3aed.

Compact raw evidence is in [winddown/](winddown/): full default per-binary audit,
completeness JSON and original startup failure log; isolated log/audit/report;
context prelaunch schema, input/log/audit/terminal/report; post-run guards.
The complete original raw logs, executable inventory, resource scans, command
rc/head/guard records, failed219ad7bb archive and invalid/preliminary controls
remain in the main safe report directory for parent archival:
`target/claude/research-lv/pr14-final-suites` (main checkout).
No main report/tool directory is removed during this agent's cleanup.

## Valid historical approvals and remaining P3

Independent Sol source approvals on exact3aed: A5593950615 and B5593951317,
no newP1/P2. Historical219ad7bb source approval5593130349; earlier12a23d6d
approval5591363667 and3fa6435b approval5591644090 remain historical. Root fmt,
four strict all-target Clippy configurations and matched fmt/Clippy passed7/7
on3aed and its actual enabled nonforce push hook; no hook was bypassed.
Source approval is separate from failing acceptance. The new docs/artifact
record must also pass its enabled hook; it does not rerun/clear full acceptance.

Accepted historical serving CPU cost+2% (allowance+4%), singleton+0.9%, remains
accepted history. No new performance measurement or protocol claim is made.

Retained nonblocking P3: COUNTER_SERVICE counter wording when WAL and retained-
entry limits exhaust together must name the actual admission counters and
recovery path, rather than assume the retained-entry counter necessarily advances
when the WAL gate refuses first. Distinguish WAL refusal/admission counters from
`refused_entry_proposals`, and explain recovery through checkpoint/WAL reclaim
or correcting the configured capacity as applicable. This requested integration
wording fold was NOT done: the candidate is rejected, not integrated. It is not
the cause of the observed hang and does not change the rejection reason.

## Resources and final handoff

Full failed default:132 command resource audits,38549 sampled task entries,
zero priority/affinity violations. Isolated:39 sampled entries,zero violations;
context:331 sampled entries,zero violations. Actual nice19, assigned12–15
(relay alone12/rest13–15; context13–15),max4 Cargo jobs,one own target;
all owned controllers/executables joined, including deleted-cwd scan.
All recorded pre/post guards passed. Final publication runs enabled hook only,
then the existing sweep removes ONLY this task's worktree/target and owned
scratch. Exact publication/cleanup/disk results follow in PR CURRENT/main safe
report and parent handoff. No new full-suite run or main write is implied.
