# VoteBoat wind-down outcomes — 21 October 2026

All six existing candidates are decided. No new research or roadmap slice was opened. N1d was not built. Qualified production remains exactly PR24 main `bd714771e181d59c2cfaece3fc93dc411ce41ddb`; subsequent main commits contain disposition documentation and retained evidence only.

| PR | Outcome | Gate evidence |
| --- | --- | --- |
| [24 V3f](https://github.com/KarlLivesey/VoteBoat/pull/24) | KEEP / MERGED, merge1e56e9f5 | Both approvals; exact-head full default2603/all2915/no-default1346/native1924/matched75 passed, no failures; strict checks and enabled push hook passed |
| [14 ST2](https://github.com/KarlLivesey/VoteBoat/pull/14) | REJECT / CLOSED as parked | Source3aed both approvals/strict checks; full default2667/1/1 failed, subsequent bounded diagnostics inconclusive |
| [15 DR1](https://github.com/KarlLivesey/VoteBoat/pull/15) | REJECT / CLOSED as parked | Independent source66fc both approvals/strict checks; full default2686/2/1 failed; failure causes unproven |
| [19 N1](https://github.com/KarlLivesey/VoteBoat/pull/19) | REJECT / CLOSED as parked | Existing durable-save CPU gate failed; unresolved actual-visited completion and pre-existing same-peer FIFO review obligations; no N1d or rescue slice |
| [22 N1b](https://github.com/KarlLivesey/VoteBoat/pull/22) | REJECT / CLOSED as parked | Dirty reconciled source Python15/0/default example56/0/all-feature example63/1rc101; cause unproven; no later qualification/publication; full reconstructable patch archived |
| [23 N1c](https://github.com/KarlLivesey/VoteBoat/pull/23) | REJECT / CLOSED as parked | Independent1c source approvals/strict checks; full default2639/1/1 failed,129/129 audited; fair Mac performance incomplete0/70 quiet prerequisite unmet |

These are wind-down dispositions, not unsupported production-defect findings. PR23 has no fair-head measured CPU loss; its metrics are unavailable. PR22's original remote remains463 and the tested reconciliation was never committed or pushed. The full patch is archived, never applied to main. Accepted historical ST2 CPU cost, V3e judging rule and N1d contract amendment remain history, not grounds to bypass failed gates.

## Evidence and cleanup

- [GitHub states](github-dispositions.json): PR24 merged, five rejected PRs unmerged/closed. GitHub may expose a synthetic merge_commit_sha for an unmerged PR; the explicit merged boolean is authoritative.
- [Task terminal audit](task-terminal-audit.json):36/36 completed, no pending child runs.
- [Ownership audit](ownership-audit.json): no owned workload/deleted-cwd processes or extra worktrees; foreign application-conformance checkout atd6adc44d preserved.
- [PR15 evidence](pr15/REPORT.md), [PR19 review/decision](pr19/REPORT.md), [PR22 reconstructable archive](pr22/ARCHIVE.md), [PR23 fair performance](pr23/REPORT.md), [PR23 complete failed default](pr23/linux-default/REPORT.md). PR14 branch-only disposition062daf90 and main implementation record preserve its controls/failure history.
- Every delegated task-created target/worktree/scratch/helper was swept. The parent also closed three positively identified idle automatic tool hosts; [receipt and resource limitation](parent-helper-cleanup.json). Late cleanup does not establish all-task resource compliance. Native test/build receipt limits and earlier helper violations remain separately recorded.
- Enabled root fmt/four strict Clippy/matched fmt+Clippy run on the final documentation push. Main inherited build artifacts are removed after that push; main safe tools/raw reports under target/claude are retained. Final post-push cache/process/disk receipts are retained there. Shared /tmp/voteboat-test-ports and foreign processes/worktrees are untouched.

The lifetime and network macro goals remain incomplete; this wind-down is complete once the checked record is pushed and the post-push artifact sweep is verified. No successor deliverable or detached continuation is scheduled by this owner.
