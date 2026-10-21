# PR15 established transfer gate repair — bounded validation

Existing PR15 / lb/dr1-session-ledger only. Input and direct parent:
`558643b39908828e7bc53082b302fd8421f05973`.
Parent-owned main remains `7dbd932db307f3c2915bcc6def26c2e1de0ef2ff`.
This report accompanies the checked repair commit; its exact published hash,
actual publication-hook receipts and cleanup are recorded in the PR CURRENT,
durable report comment and raw `pr15-transfer-gate/FINAL.md`.

## Known before failure and scope

The original all-feature gate on `79829e87` failed transfer_service **32/1** at
`revocation::interrupted_split_resumes_under_replacement_administrator_tcp`,
line89, debug addition overflow. This allocator remained on exact 558:
AtomicU16 starts14000, step1024; candidate51 =65200, maximum offset487,
65200+487=65687 >65535. The historical unchanged 33/0 isolated rerun did not
clear the failed full gate. Existing root `pr15-suites/transfer-overflow-analysis.md`,
`handoff-validation.md` and `test-all-rest.log` are the before evidence;
no unchanged broad reproduction/full-gate rerun was made.

The before-edit schema, ownership, partial-progress/failure and acceptance plan
is [PLAN.md](PLAN.md), linked from the implementation record. Only the three-file
diff from approved fixture commit `3fa6435be2e55352665a1a712299b44443348c7d`
was extracted with git show + apply/check, never a whole-commit cherry-pick/tree.
ports.rs and fixture_isolation.rs match its blobs exactly. transfer_service.rs
keeps 558's excluded ST2 maintenance assertions/probes excluded.

The finite existing safe pool and common per-port TCP/UDP locks now reserve
all 24 listener offsets together, with checked arithmetic before any bind.
Collisions drop partial layouts before advancing; exhaustion fails closed;
namespace failures retain the existing diagnosed fallback. Ephemeral/legacy
exclusions and the shared namespace remain. Socket handoff retains locks until
Cluster Drop has killed/waited every child. Existing reservation/fork guards
stay; the approved SPAWN guard protects adoption/reclaim release assertions
against descriptors briefly inherited by concurrent forks. The layout regression
checks both socket halves before handoff and disjoint leases afterward.
No shared lock file or /tmp/voteboat-test-ports was unlinked.

## Source preservation

All **1,850 protected files** (src, examples, manifests/locks, build.rs, lint/
toolchain inputs, hooks and the entire matched workspace) remain byte-identical
to 558. SHA-256 of the sorted `sha256  path\n` protected manifest:
`cdfbbf11962b14985157f5ce8df81c2bdc1c646c901b966a09be4917a519b3c7`.
The prior authoritative **2,611-file** input manifest was compared independently:
only the three authorized test files differ. Raw per-file hashes and patches
are retained; no production source, public/library API, dependency or unsafe
was added. Strict lint policy/thresholds and assertions/bounds are unchanged.
The selected approved helper-module dead_code annotation follows the existing
startup caller's shared-fixture convention; no production lint change.

DR1's eight rounds, exact ledger/identity/first-claim binding and 16-byte
benchmark correction remain untouched. 558 and all published b0ff history are
retained. Rejected ST2 production/admission/default-maintenance/startup
backpressure/retention remain excluded. This is a PR15 fixture gate repair,
not PR14 revival, a new feature, a new reliability slice or a production claim.

## Actual after validation

Linux stable, sequential feature configurations, locked/offline:

| Check | Passed | Failed | Ignored |
| --- | ---: | ---: | ---: |
| Exact transfer layout boundary (default) | 1 | 0 | 0 |
| Full fixture_isolation default | 9 | 0 | 0 |
| Full transfer_service default | 26 | 0 | 0 |
| Full fixture_isolation all features | 9 | 0 | 0 |
| Full transfer_service all features | 34 | 0 | 0 |
| Existing startup native-pair reconnect lease caller (default) | 1 | 0 | 0 |

**80 test executions passed**, including overlapping configurations and the
separate boundary run. Full transfer counts are prior25/33 plus one boundary
test. Boundary checks: 24 offsets, max valid base65048 reaching65535,
base65049 rejected, known65200 rejected. Original TCP failure case passes in
both complete transfer runs. Exact stdout, argv/env, result blocks and exits
are retained per step; no test assertion/count/budget or timeout was weakened.

**Seven strict checks passed, zero diagnostics**, through the unchanged enabled
qualification hook: root fmt; default/all/no-default/native-only Clippy with
`--locked --offline --keep-going --all-targets -- -D warnings` and the respective
feature flags; matched fmt and strict all-target Clippy with its own lock.
Actual nonforce publication runs the same enabled `.githooks/pre-push` again;
per-Cargo guarded rc records and aggregate push status are recorded externally
after that action. `core.hooksPath=.githooks`; no bypass/suppression of failures.
The aggregate runner fails on any failed step, failed/empty required test,
resource violation or missing strict-step evidence; suites.sh was not used.

## Resources, publication and qualification limits

Absolute kernel nice19, cores0–3, observed task/thread audits with zero violations;
maximum four Cargo jobs, CARGO_INCREMENTAL=0. Builds use only this checkout's
target (also matched), with <=60GiB own cap. Stock diskguard before every
launch and every hook Cargo step; unchanged shared120GiB/root150GiB targets,
<=5 worktrees and >=500GiB free. No delegation, research, browser/CI watch,
detached jobs, source-tree copies, extra target directory, main source/memory
edit, merge, closure or new PR.

Fresh independent pinned-source reviews and the exclusive whole final
default/all/no-default/native/matched matrix are **parent-owned and outstanding**.
No full matrix, exhaustive relay, macOS or performance qualification was run.
The known allocator cause is repaired and targeted checks pass; the old failed
full gate is not reclassified as green. DR1 bounds application-session retention,
not automatic end-to-end maintenance or the full lifetime-bounds roadmap.

After publication and PR CURRENT/one durable comment update, immediately sweep
the owned checkout/target/scratch, audit owned/deleted-cwd processes and run the
final stock guard. Exact publication/cleanup receipts survive in main
`target/claude/research-lv/pr15-transfer-gate/` (reports only).
