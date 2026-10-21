# PR15 bounded transfer gate repair — schema plan (before fixture edits)

Pinned input: 558643b39908828e7bc53082b302fd8421f05973, existing lb/dr1-session-ledger / PR15.
Parent owns main 7dbd932db307f3c2915bcc6def26c2e1de0ef2ff and final reviews/matrix.
This repairs the established PR15 gate cause, not a new reliability slice or PR14 revival.

Data/API: extract only the three-file diff from 3fa6435be2e55352665a1a712299b44443348c7d
(tests/support/ports.rs, tests/transfer_service.rs, tests/fixture_isolation.rs).
Test-only offset_ports checks every u16 addition; acquire_offsets reserves the exact
24-listener layout (four slots, nodes 1–3, peer/command offsets, maximum offset 487)
using the existing shared locks, safe candidate pool and ephemeral exclusions.
No public/library API, dependency, unsafe, new lint suppression or weaker assertion.

Ownership/state: finite candidates -> complete TCP+UDP reservation -> socket handoff
while retaining registry/file locks -> cluster startup/reopen -> kill/wait children ->
lease Drop. Preserve existing fork/reservation gates and add the approved isolation
SPAWN guard to protect release assertions from inherited fork descriptors.
Collision or partial layout drops every acquired reservation before the next candidate.
Namespace errors retain the existing diagnosed fallback; pool exhaustion fails closed,
without retrying another namespace for AddrNotAvailable. Never unlink shared lock files.
Restart/handoff retain the lease for the full Cluster lifetime; no production transition changes.

Source boundary: ALL src/, examples/, Cargo manifests/locks, build.rs, .cargo/,
.githooks/, lint/toolchain inputs and comparison workspace remain byte-identical to 558.
DR1's eight rounds, first-claim binding and corrected 16-byte benchmark metadata stay intact.
Rejected ST2 admission/default maintenance/storage/backpressure/retention stay excluded.
Only these three test files plus docs/evidence may change; preserve main/DR1 file differences.

Known-before evidence: original 798 all-feature transfer 32 passed/1 failed, panic line89
in revocation::interrupted_split_resumes_under_replacement_administrator_tcp.
14000 + 50*1024 = 65200; 65200+487=65687 > 65535. Historical isolated 33/0 did not
clear the failure. Reuse pr15-suites/{transfer-overflow-analysis.md,handoff-validation.md,
test-all-rest.log}; no broad reproduction or unchanged full-gate rerun.

Acceptance: exact-layout length/max-base/next-base/65200 boundary regression; full
fixture_isolation; full transfer_service default then all features, retaining all counts,
assertions and process cleanup. Focused same-helper callers only if needed.
Seven strict steps: root fmt, default/all/no-default/native all-target locked/offline
keep-going Clippy -D warnings, matched fmt/Clippy with its own lock in the same own target.
Aggregate fails on ANY step, missing/empty required result or resource violation.
Enabled unchanged actual pre-push hook must pass on nonforce publication retaining 558
and all published history. No exhaustive relay/full matrix; fresh reviews and exclusive
whole five-config gate remain parent-owned and outstanding.

Resources: cores 0–3, absolute kernel nice19 for controller/descendants/observed threads,
max4 Cargo jobs, incremental0; own checkout target only (also matched), <=60GiB.
Stock root diskguard before every launch including each hook Cargo step, unchanged
global 120GiB worktree /150GiB root, <=5 worktrees, >=500GiB free. No browser, CI watcher,
delegation, detached jobs or other owner's cleanup. Root pr15-transfer-gate reports only.
Commit compact validation/pr15-transfer-gate/REPORT.md and implementation evidence,
push same PR15, update CURRENT + one durable comment with exact published head.
Then immediately sweep own target/worktree/scratch; audit owned/deleted-cwd jobs and
final stock guard. Leave main source/memory and /tmp/voteboat-test-ports untouched.

Linked wind-down mini plan:
| Item | Purpose | Dependency | Completion checks | Macro |
| --- | --- | --- | --- | --- |
| Current PR15 gate repair | Remove known test-fixture overflow so existing DR1 can be judged | Exact 558 and approved three-file fixture diff | Boundary/isolation/transfer suites, strict7 and enabled hook; then parent fresh reviews/full final-head gate | Incomplete LB bounded application sessions |
| Next PR23 N1c | Judge existing candidate without rejected ST2 | Independently qualified DR1/main | Parent fresh reviews, full suites, existing fair performance gate | Existing network milestone |
| Then PR22 N1b | Finish existing candidate after PR23 | Final reconciled main | Parent recorded review/validation gates and documentation caveats | Existing network milestone |

Macro scope unchanged: no milestone completed; ST2 remains rejected/parked.
