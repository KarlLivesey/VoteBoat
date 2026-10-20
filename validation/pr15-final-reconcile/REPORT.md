# PR15 cumulative source preparation — focused evidence

This is source preparation for existing PR15, not a green merge gate. Starting
PR15: `79829e871b2c4131700f51a7c2df978db8fc7618`; exact merged dependency:
`219ad7bb8489c19012c8298d1021ddd959861782` (PR14), containing main
`bd714771e181d59c2cfaece3fc93dc411ce41ddb` and fixture fix `3fa6435b`.
Both histories are preserved; GitHub's PR15 base remains main.

**Dependency is unqualified.** Parent reports actual failures in PR14's full
default startup run: `backpressure::follower_with_a_slow_rewrite_declines_new_data_instead_of_failing`
and `backpressure::absent_follower_does_not_stop_the_leader_checkpointing`.
Cause/fix is not known here. ST2's owner owns diagnosis; the DR1-adapted focused
passes below do not diagnose or qualify ST2. A changed dependency must be
reconciled and checked before final qualification.

## Reconciliation and causal checks

Only `docs/IMPLEMENTATION.md` conflicted textually (two sections). Both tracks'
plans/evidence remain. The application mini plan pins PR14; the inherited ST2
mini plan is marked historical. The required first-committed-bootstrap consequence
sentence is added before APPLICATION_ENVELOPES' family table.

The automatic merge's readiness fixtures used the former contiguous log API;
initial strict checking found the type errors. They now use the existing
`EntryBatch` and range/application seams. A logical application's inherent
`apply_entries(&[LogEntry])` is avoided with an explicit StateMachine trait call.
All real-verifier assertions and both rr7 regressions remain.

ST2 retention/backpressure probes, main's EO1 and snapshot-anchor fixtures, and
N0 cluster warmup/offering emitted pre-DR1 commands. They now use session-1
Counter IDs and 16-byte Counter commands. The storage padding remains 4096
bytes; receipt/window counts, retry schedule, wire version 8, pressure/capacity
assertions and continuation order are unchanged. No assertion is removed from
the manual reconciliation delta. No new allocator or production protocol.

Before repair, the new storage-protocol regression fails on 8 versus 16 bytes;
EO1 and the snapshot anchor fail InvalidCommand; the existing cluster TCP test
fails warmup admission with Application(InvalidCommand). Their unchanged
acceptance assertions pass after the focused repairs (logs retained here).
The new storage regression also pins admission, checkpoint/restore, duplicate
replay of the unpadded command and malformed/old-byte refusal.

`source-preservation.json` records byte comparisons: nine of ten selected DR1
invariant sources are identical to the approved old head; the only difference,
codec/enrollment, is main's EntryBatch conversion and cloning its sole decoded
entry, preserving complete bootstrap comparison. Ten selected ST2/main files
are byte-identical to the exact dependency, including admission/receive,
summarized log, accepted storage, peer slots, EO1 continuation tests, transfer
fixture/port leases and benchmark process ownership. The actual ledger,
Counter, readiness verifier and first-claim binding are unchanged.

## Actual focused checks

Linux stable, locked/offline; cores 0–3, nice 19, CARGO_BUILD_JOBS=4,
CARGO_INCREMENTAL=0; feature configurations sequential. Root and matched use
only this isolated checkout's existing target. A passing stock disk guard ran
before each Cargo invocation; the owned target ceiling is 40 GiB.

Default: 27 commands / **254 passing test executions / zero failures**.
All-features: 14 commands / **136 passing test executions / zero failures**.
Every acceptance command ran at least one test. `results.json` records exact
argv/counts. The two mistaken diagnostic selectors (one zero-test selection,
one nonexistent target) are not counted as acceptance evidence; their original
logs remain in the main artifact report directory.

| Focused command label | Passed | Failed |
| --- | ---: | ---: |
| default-app | 33 | 0 |
| default-learners | 41 | 0 |
| default-rr7 | 2 | 0 |
| default-claim-bound | 1 | 0 |
| default-ledger | 16 | 0 |
| default-runtime | 58 | 0 |
| default-node-retention | 4 | 0 |
| default-node-maintenance | 7 | 0 |
| default-snapshot-routes | 15 | 0 |
| default-startup-probe | 1 | 0 |
| default-startup-pressure | 4 | 0 |
| default-startup-partial | 1 | 0 |
| default-counter-sessions | 1 | 0 |
| default-counter-maintenance | 6 | 0 |
| default-counter-noquic | 1 | 0 |
| default-transfer-boundary | 1 | 0 |
| default-transfer-revocation | 1 | 0 |
| default-portleases | 9 | 0 |
| default-snapshot-worker | 14 | 0 |
| default-cluster | 8 | 0 |
| default-held-maintenance | 1 | 0 |
| default-aggregate | 1 | 0 |
| default-u2 | 4 | 0 |
| default-core-aggregate | 7 | 0 |
| default-leadership-profile | 2 | 0 |
| default-maintenance-schema | 1 | 0 |
| default-leadership-wrapper | 14 | 0 |
| all-app | 33 | 0 |
| all-learners | 42 | 0 |
| all-rr7 | 2 | 0 |
| all-claim-bound | 1 | 0 |
| all-runtime-charge | 1 | 0 |
| all-counter-sessions | 1 | 0 |
| all-counter-maintenance | 8 | 0 |
| all-leadership-profile | 2 | 0 |
| all-leadership-wrapper | 14 | 0 |
| all-transfer-boundary | 1 | 0 |
| all-transfer-original-failure | 1 | 0 |
| all-cluster | 8 | 0 |
| all-held-maintenance | 2 | 0 |
| all-wire | 20 | 0 |

## Required lint and publication

The enabled `.githooks/pre-push` was invoked locally after all focused tests:
**7/7 checks passed, exit 0, zero diagnostics**. Root formatting; strict Clippy
for default, all-features, no-default-features and native-only; matched formatting
and strict all-target Clippy with its own lock all pass. Exact commands are in
`results.json`; the hook log and invocation are retained here. All strict checks
use locked/offline/keep-going/all-targets/-D warnings. Matched shares only this
checkout's target as explicitly authorized. No lint threshold or hook check is
weakened. Git `core.hooksPath` remains `.githooks`.

Publication is a nonforce update of the existing PR15 branch and runs that same
enabled hook again. The final commit ID and actual publication-hook result are
recorded in the durable PR comment and main artifact report (they cannot be
self-recorded in the commit being published).

## Full qualification remains pending

Old-head approvals are Sol review 5586620689 and Opus review 5587177102 for
79829e87 only. The old exclusive full gate was NOT green: all-features
2,890 passed / 1 failed at transfer-service's u16 port-addition overflow; its
unchanged 33/0 diagnostic rerun did not clear that gate. The exact shared port
repair is inherited, and both default/all-feature boundary and original TCP
cases pass here, but these are focused checks. Old default 2,577/0,
no-default 1,350/0, native 1,921/0, matched 73/0 and 7/7 checks remain historical.
No old matrix qualifies this source.

No full matrix, exhaustive relay run, new research, new slice or new PR was
launched. Parent owns fresh independent source confirmations and an exclusive
whole matrix after ST2 finishes and final cumulative source is stable. Static
bootstrap/checkpoint-less-recovery limitations and planned DR2/TX1 work remain
as documented; this reconciliation does not close them. No CPU measurement or
macOS test was made (the required stock guard only reads its disk snapshot).

Raw commands/logs and the final pushed head/cleanup audit are saved under main
`target/claude/research-lv/pr15-final-reconcile`; no main source/docs/memory edits.
The isolated checkout and build output are swept after recording publication.
