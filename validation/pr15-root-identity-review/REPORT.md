# PR15 root identity review repair — bounded evidence

Existing PR15, branch `lb/dr1-session-ledger`; direct source parent
`dec5c5d79fa66340b86a6936fa8e420ce9320217`. Parent-owned main stays
`7dbd932db307f3c2915bcc6def26c2e1de0ef2ff`. This report accompanies the
checked repair; exact published head, actual enabled publication-hook receipts
and final cleanup are recorded in PR CURRENT/comment6307300373 and raw
`target/claude/research-lv/pr15-root-identity-review/FINAL.md` in main (reports only).

## Review responses and implementation

[A5594734109](https://github.com/KarlLivesey/VoteBoat/pull/15#pullrequestreview-5594734109)
and [B5594735675](https://github.com/KarlLivesey/VoteBoat/pull/15#pullrequestreview-5594735675)
both request changes on dec5. B amended its initial approval in the same review;
these are not two approvals. Fresh independent source reviews remain required.

P2: each transfer fixture now exclusively creates a PID/checked-sequence root,
independent of recyclable port bases. AlreadyExists skips stale directories,
files and other existing names without opening/deleting/adopting them. The
private Directory owns the path and cleans it on unwind, including setup before
Cluster construction. Cluster Drop still kills/waits children before field
destruction; the lease remains held through child shutdown. Normal callers'
post-drop remove_dir_all(...).unwrap() checks are unchanged. Unwind cleanup
errors are diagnosed without a second panic. Hard-crash leftovers are skipped.
No socket/fork/handoff/restart/reader-join behavior or shared namespace changes.

P3: remove only the obsolete numeric readiness identity size estimate in
src/raft.rs. There is no replacement platform-size claim; size_of charging and
all executable library lines remain unchanged.

Before-edit schema/lifecycle/partial-construction/cleanup plan and linked
current/next-two mini plan: [PLAN.md](PLAN.md). The original before-edit version
is retained raw; the final test detail strengthens the initial helper-only
controls with real Cluster/PortLease Drop and an isolated stale-collision parent.
Initial helper-only passes are excluded from final-source counts.

## Actual focused validation

Linux stable, sequential feature configurations, locked/offline:

| Positive check | Passed | Failed | Ignored |
| --- | ---: | ---: | ---: |
| Two final deterministic root controls | 2 | 0 | 0 |
| Identical fresh-root command after mutation restoration | 1 | 0 | 0 |
| Existing exact layout/overflow boundary control | 1 | 0 | 0 |
| Full fixture_isolation default / all | 9 / 9 | 0 | 0 |
| Full transfer_service default / all | 28 / 36 | 0 | 0 |
| Existing startup native-pair reconnect lease caller | 1 | 0 | 0 |

**87 passing positive test executions; no unexpected failures or ignored tests.**
Transfer baseline26/34 gains exactly two tests. The original TCP failure case
and max-valid65048/invalid65049/known65200 assertions remain and pass.

Deterministic reuse control: two unstarted fixtures use the real Cluster and
PortLease Drop paths with a private lock-only reservation, address port0 (the
existing isolation-control pattern; no sockets/shared namespace). A drops and
releases its base lock while its root/stores remain; B reacquires that same
base lock and gets a fresh root. After B writes marker/store bytes, A's delayed
external cleanup runs and B's bytes survive. No sleep-based race or host port
contention is needed. The other control forces exclusive stale-directory/file
collisions, catches partial-setup unwind, checks owner-only cleanup, then checks
a clean next root and untouched stale/collision bytes. Tests clean owned scratch.

The **single expected negative-control execution fails 0/1 rc101** at exactly
“recycled base must get a fresh root identity”: only the fixture root helper is
mutated to old PID/fixed-reused-base create_dir_all behavior. No production or
port allocator mutation. The exact source is restored in finally, SHA verified,
and the identical command passes1/0. Raw patch/log/rc/assertion proof retained.

**Seven strict checks passed, zero diagnostics**, through the unchanged enabled
qualification hook: root fmt; four root Clippy configurations (default/all/
no-default/native-only); matched fmt/Clippy with its own lock. Clippy commands
use --locked --offline --keep-going --all-targets -- -D warnings. Actual enabled nonforce push repeats the same seven with
a guard before every Cargo step. Publication receipts are recorded externally
after the push; no hook bypass or aggregate failure masking.

## Hash scope, resources and limitations

Compared against exact dec5: only tests/transfer_service.rs, its new private
root.rs/root_tests.rs and the explicit src/raft.rs comment exception change
build inputs. **1,849 of the prior 1,850 protected files are byte-identical**;
src/raft.rs is the sole comment-only exception, with identical non-comment lines.
The audited 2,615-input corpus includes tests/configuration and new test files.
Manifest SHA256: `7439af3e0b1b6cf04997cf0467c152846998c4c9e7c92cae4d977171d0923434`.
All ten critical files still match original79829e87 byte-for-byte. Eight-round
semantics, session1/16B adapters/metadata and the exact first-claim consequence
sentence remain. Executable src/examples/Cargo/locks/hooks/matched are unchanged;
rejected ST2 production/default maintenance/startup retention/backpressure remain
excluded. Assertions/bounds/timeouts/lints/dependencies are not weakened.

Controlled build/test/hook processes: cores0–3, absolute kernel nice19 (not a
relative increment), max4 Cargo jobs/incremental0; observed-thread audits,
sole owned target (also matched), own<=60GiB. Stock diskguard before launches
and each hook Cargo step, caps unchanged (root150/worktrees120/live<=5/free>=500).
The runner fails on unexpected nonzero/empty/failed checks or resource violations;
the negative control explicitly requires its expected failure. No delegation,
research/new slice/PR, main source/memory writes, merge/closure, full matrix,
exhaustive relay, performance/macOS qualification, browser or CI watch.

Both prior reviews remain REQUEST CHANGES on dec5. Parent owns fresh independent
reviews and exclusive whole final-head default/all/no-default/native/matched
suites, eligible doctests and unskipped normal-debug exhaustive relay. Old PR15
full-all and rejected PR14 full-default failures remain failed history. No full
qualification/Done/LB-completion claim. DR1 bounds application sessions, not
automatic end-to-end lifetime maintenance; static-bootstrap/checkpoint-less
admission and DR2/TX1 stay deferred.

Immediately after nonforce publication/PR updates, remove own target/worktree/
registration/scratch through tools/sweep.sh; positively-owned process audit and
final stock diskguard receipts survive in the raw report directory.
