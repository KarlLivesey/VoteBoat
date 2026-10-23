# CPU5 borrowed LogView: focused checked candidate

23 October 2026. Candidate only; machine elimination, CPU benefit and release
qualification remain **PENDING**. Parent base is `8c0fe77fba43020ce0cdcaccdc883bbffd5502c7`,
with unchanged production baseline `0bf9271ecb5a34c4089c6bb10381303232624170`.

## Implemented change

Only production `src/log/transition/view.rs` changes. Summarized views keep no
owned metadata snapshot and return the exact metadata of their existing immutable
summary borrow. New and trimmed/accepted overlays hold `Some(LogMetadata)`.
The existing private constructors establish that `None` always has an associated
summary. All view metadata readers use the accessor; `Copy` remains available.
There is no new public/matched API, allocation owner, unsafe code, generation,
queue, provider or persistent state. The required owned predecessor, next summary,
Pending/completion pipeline, validation, stamps, charges and entry ownership stay
at their original boundaries. See the pre-edit [schema](CPU5-DESIGN.md).

The historical source-only NO-GO resolved only after a parent static audit
identified canonicalA's304-byte summary-to-temporary snapshot. This candidate
adds metadata access selection and changes layout/codegen opportunities. It can
still introduce full-view movement or other replacement work. Neither source
borrowing nor a pointer witness establishes instruction removal or CPU savings.
Required predecessor/672-byte staging/272-byte effect/336-byte summary work is
not credited as removed.

## Actual focused checks

All test commands use stable, locked/offline dependencies, one own normal target,
max4 Cargo jobs, incremental0, dev/test debug0/opt0 and debug assertions enabled.
Configurations execute sequentially. Counts are test cases, not proof strength.

| Executed filter/targets | Default | All features |
| --- | --- | --- |
| Library `log::transition` |39 passed;39 again after restoration |39 passed |
| Library `log::entries` |20 passed |20 passed |
| Library `raft::` |184 passed |184 passed |
| Library `native::log_store::accepted::tests` |23 passed |23 passed |
| Public `log_owned / log_staging / log_store / raft / volatile` |5/2/19/81/11 passed |5/2/19/81/11 passed |
| Matched `voteboat_engine::actor::` |29 passed |29 passed |
| Matched `application::` |2 passed |2 passed |

Additional default targets passed: application23, learners23,
logical_voter_application8, membership32, retirement7, scopes12. No selected
test was ignored. Matched has no declared feature switches; its requested
`--all-features` run executes the same comparison feature graph. These are
focused checks, not the parent's exclusive full5 or heavy relay campaign.

Five direct tests prove exact summary alias/value and copied-view behavior;
independent exact new/accepted/trimmed metadata; equal-value and foreign/stale
summary validation; both owned and borrowed views against owned/copy/full-oracle
planners across valid/refused ballot/commit/suffix/snapshot cases; and an owned
plan usable after its source summary is dropped, with foreign-stamp checking.

The old-owned construction control changes only summarized construction to
`Some(summary.metadata)`. It compiled and failed the exact alias witness at
`tests/view.rs:23` with exit101; its full-oracle comparison still passed. The
exact control patch, mutated source, command and raw failure are archived.
Restoration matched all63,308 recorded checkout file size/SHA256 rows exactly
(including historical tracked reports; target/.git/task helpers excluded).
Before/restored manifest SHA256 is
`0cb1114ea925a90afcda2b2024bf6d74d37b4b15d8c714d9400e3b9a346080a9`.
This is a borrowing causal control, not a machine-copy or performance control.

Strict7 passed with zero diagnostics: root formatting, four strict all-target
Clippy graphs (default/all/no-default/native-only), matched formatting and its
separate locked/offline strict all-target Clippy. The hook configuration is
`.githooks`. The actual enabled push hook remains the publication check; its
raw outcome and exact published head are recorded in the ignored builder report
and PR. No pending parent qualification is represented as a completed gate.

## Semantic preservation matrix

| Implemented obligation and unchanged source/consumer | Actual focused evidence |
| --- | --- |
| One ordered prefix and exact durable evidence: `raft.rs`, `raft/completion.rs`, transition/incoming; public persistence/volatile helpers | Both core graphs, public raft81/volatile11; Written/stale callbacks cannot install or supply quorum/application evidence; owned/copy/full-oracle and refused-install histories |
| Weighted/recursive/joint and logical-child voting: existing quorum/membership and LogicalApplication consumers | Core membership joint/learner/recursive histories; raft recursive election/commit/read tests; membership32, learners23, logical application8. Full logical-native/relay recertification remains parent work |
| Learners, joint/final rollback, retirement and transfer: existing `raft/membership.rs`, receive/proposal and leadership consumers | Configured transition and core tests, public learners/membership/retirement, raft leadership persistence and restart histories |
| Reads, readiness and scoped authority: existing core read/readiness, public Counter/read helpers | Raft read-context/current-term/applied-prefix/stale-session histories; learner readiness and queued promotion refusals |
| Full IDs, dedup, outbox and lineage: existing public application/Counter/scope owners; normal matched Actor delivery | Application23/scopes12/retirement7; matched nonmonotonic full-width IDs, duplicate replacement, missing/dropped waits and partial-delivery tests |
| Default maintenance, snapshot and recovery: existing native accepted overlay, core snapshot and matched maintenance helpers | Accepted23; public log-store/raft snapshot and power-cut histories; matched checkpoint-scope and historical Noop restore tests |
| Bounded public/native composition and independent groups: `log/owned.rs`, `native/log_store/accepted.rs`, original runtime/provider boundaries | Cross-group refusal and subset barriers, exact charges, accepted overlay ownership, host/native conformance and scoped provider failures |
| Other implemented networking/relay/striped/lifecycle extras | Production outside the one private view file is source-unchanged; no API or feature is removed. Focused evidence is not exhaustive certification; original conditional full5/relay/docs gates remain mandatory |

Independent ship/fleet/department groups keep their own ordered logs and durable
evidence. This representation adds no ancestor commit, root counter, shared
per-write authority or new global owner. Existing implemented extras above
OpenRaft remain; unfinished roadmap features are not advertised as implemented.

## Publication, limits and handoff

Only one candidate is built. No benchmark, CPU timing/profile, new codegen
campaign, full suite, heavy relay, subagent or roadmap task runs here. Parent
alone owns fresh dual source reviews, exact Apple codegen against attributedA,
original9pairs and conditional64/full5. CPU1-4 remain rejected/unmerged and
P2/P3 stay paused. Historical resource HOLDs remain uncleared and unattributed.

Evidence destination is the explicitly authorized ignored main path
`target/claude/pillar1-20261022/cpu5-builder/`: raw commands/logs, mutation and
restoration manifests, final source/full patches/frozen inputs and terminal
report. Publication goes through `.githooks/pre-push` and one draft PR against
main, followed immediately by own target/worktree/registration/temp-seed cleanup.
No main tracked file or inherited cache is changed by the builder.

Visibility limits are explicit: the tool catalog was inspected before creating
the initial payload, without shell/filesystem access. First exec used the fixed
bootstrap and precreated payload; its observer preceded substantive reads and
joined. Initial observer rows had less executable detail than later rows.
Import/control-file/bootstrap/tool/T3/model startup, between-sample/reparenting
and foreign/inaccessible/unknown infrastructure are not certified. Unknown
deleted-cwd matches are neither attributed nor signaled. Subsequent owned
descendants/tasks are sampled with recorded identities and absolute nice19/
cores0-3, direct-child preexec and DEVNULL, full stock actual-Mac guards and
explicit joins. This is no whole-task/global/historical clearance claim.

Preparation faults are retained: the worktree lacks the untracked design pack,
so its original main copy was read without edits; a guessed `chapters` directory
and `src/raft/tests.rs` read/search failed before focused corrected paths;
oversized historical displays were bounded; one file-edit request was rejected
for attempting delete/add on the same path. None is a product test pass or an
attributed resource violation. Raw meaningful failures remain immutable.
