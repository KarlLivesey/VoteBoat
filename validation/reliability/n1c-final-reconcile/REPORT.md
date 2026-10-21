# PR23 cumulative source preparation — qualification pending

Existing PR: https://github.com/KarlLivesey/VoteBoat/pull/23, base `main`,
head branch `net/n1c-held-appends`. This records checked source preparation,
not a keep decision, full-suite pass, dependency qualification or merge.

## Exact composition and manual delta

Start: `44b95083e96f7b09ab4e900ed4e1c25042896c2a` (old N1c).
Pre-edit plan commit: `b5e4ff9c` (first merge parent). Second merge parent:
`b0ff8c0fa9b2f777363352ebc38ae4f477030431` (exact DR1), which contains
**unqualified** ST2 `219ad7bb8489c19012c8298d1021ddd959861782` and main
`bd714771e181d59c2cfaece3fc93dc411ce41ddb` (V3f retained). Both histories
are preserved. The published source SHA is recorded in the PR body/comment
and the parent report directory's `SOURCE_HEAD`.

Three conflict files were resolved:

- `src/raft.rs`: retain V3e PeerSlots/cached peer order and N1c held state;
  heartbeat keeps ST2 stall accounting/no-op retry/step-down gate, then N1c
  broadcast and configuration-notice reset while still leader.
- `src/raft/progress_broadcast.rs`: retain N1c hold dispatch, plus V3e's
  cached peer-order path with holds off.
- `docs/IMPLEMENTATION.md`: preserve both implementation histories and the
  pre-edit reconciliation plan; label old N1c plans/measurements historical.

The only manual production-source delta beyond conflict resolution is a
`hold_due` comment correction: byte-truncated release can safely rehold the
spare mid-batch; "at most two batches" is not an unconditional wire-size claim.
The N1c hold module's executable source is identical to old N1c, excluding
comments. `source-preservation.json` also records 17 exact file comparisons:
original core/timer/parent regressions and selected DR1/main invariant modules,
including Counter, retention, readiness, peer slots, log stamps/in-place install,
EO1, WAL accounting, worker, proposals, request correlation and quorum logic.
The timed module differs from old N1c only by ST2's existing ten-line forwarding
for storage pressure, WAL budget and WAL account; hold/deadline logic is intact.

Automatic merges retain existing Counter session-1 IDs/16-byte command helpers
and EntryBatch fixture conversions. History/index/window/retained-charge
assertions, intentionally frozen clocks, explicit owner/parent release, facade
Heartbeat only after owner drain, and Cancel-before-transfer ordering remain.
No child-only benchmark metadata correction, new API, policy, batching strategy,
N1 same-poll flush, N1d release or d2 deadline change is added.

Quorum-first data holds still require a confirmed, caught-up voting spare,
nonzero progress even at batch-floor zero, acknowledged prefix >= batch floor,
and quorum coverage without that peer. Commit notices require acknowledged
prefix >= last and may be held in any group size, including two voters.
In-flight coverage must extend the confirmed prefix; it is scheduling evidence,
never durable commit evidence. Exact ack correlation, frontier/current-term
commit rules and all learner/catch-up/probe/configuration/transfer/compaction/
rejection/heartbeat paths are preserved. One request per follower, peer-bounded
held state, actual deadlines, per-group and late-poll liveness remain.

## Actual focused validation

The resource-verified repeat (`absolute19-*`) passed every selected test:

| Focused set | Default | All features |
| --- | ---: | ---: |
| Core boundaries: N1c18, V3e slots, V3f in-place install, progress retry/correlation, EO1 continuations, ST2 WAL admission | 41/0 | 41/0 |
| Runtime hold deadline / heartbeat / per-group / late-poll | 7/0 | 7/0 |
| Original native effect-owner and early-persist/retained-charge failures | 7/0 | 7/0 |
| Original logical-voter network histories (TCP; TCP+QUIC) | 2/0 | 4/0 |
| Terminal Cancel-before-transfer (TCP; TCP+QUIC) | 1/0 | 2/0 |
| Actual executable Counter session/window/expired-retry protocol | 1/0 | 1/0 |
| Real-core EO1 snapshot-send/commit-persist ordering | 1/0 | 1/0 |
| Total selected (pass/fail) | **60/0** | **63/0** |

All14 child launches verify kernel nice19, with controller affinity10–11 and
at most two Cargo jobs. A live all-thread audit is also preserved. Initial
runs passed the same checks before the parent's priority correction (plus an
extra18-test core-only invocation); their actual kernel nice was unverified,
so they are supplementary evidence rather than compliant resource gates.
No assertion, deadline, resource ceiling or test semantics was weakened.
Exact commands/exits, compressed full logs and guards are in `logs/` and
`results.json`; they include both initial and corrected-priority runs.
No full matrix or exhaustive relay run is authorized or executed in this task.
`sh .githooks/pre-push` exits0 (102.436s): root formatting, all four strict
Clippy configurations (default/all/no-default/native-only; locked, offline,
keep-going, all-targets, -D warnings), matched formatting and matched strict
all-target Clippy. Zero diagnostics; no lint level or hook is weakened.
`core.hooksPath=.githooks` remains enabled. This precommit hook and its live
14-thread audit verify actual19 and affinity10–11. The actual push runs the
enabled hook again; its final result/log is recorded externally in the parent
report and PR body/comment, avoiding a self-referential evidence commit.

One invocation error is retained: `--test snapshot_routes` does not exist;
EO1 is nested under `effect_owner`. That invocation exits 101 before running
any test. Correct command is `--test effect_owner snapshot_routes::eo1_repro`.
No product assertion failed in that invocation, and no source fix was needed.

## Dependencies and remaining gates

Both independent Sol source confirmations approve exact DR1 `b0ff8c0f`
(reviews `5593265778` / `5593269910`), with no P1/P2. DR1's reported 390
focused tests and lint/hook checks pass; its full gate remains pending.
Shared P3: cluster/offered benchmark output still labels commands 8 bytes,
although actual commands are now 16 bytes. Parent owns the upstream metadata
correction in the next dependency update; it is deliberately not duplicated.

ST2 `219ad7bb` is unqualified: the current default run has four actual failures,
startup 52 pass / 3 fail (clock hypothesis unverified), plus native-benchmark
uneven-three-lane `AddrInUse`. Its owner finishes that default run and owns the
causal clock/port repairs. This task neither duplicates that work nor claims a
qualified baseline. Parent supplies any replacement dependency and owns fresh
independent Sol confirmations of the final composition, the exclusive full
matrix, exact cumulative performance, keep/reject and main integration.

Old N1c `44b95083` has two Sol round-3 approvals (`5589750084` / `5589751355`),
with earlier round-2 Sol `5585615339` / Opus `5585838531`. Its nine failures,
18/7/1 default and 18/7/2 all-feature focused tests, seven mutations and seven
lint/hook checks are historical evidence under `../n1c-round3/` and durable
comment `6298126102`; they are not re-labelled as this composition's results.

## Performance stays unqualified

Historical standalone CPU .866/.946/.951/.939/.878 and Linux .992 (7/10,
neutral) retain their +5% slow-regime p99 and original paired TCP4k p99 caveat.
A distinct later campaign, old main `b4e3647250d6b3ae6c770bae6ae98e88f0b3c04c`
versus old N1c `44b95083`, completed 70/70 valid runs / 686,000 writes with
CPU .976676/1.089646/1.250908/1.052628/.879391 and QUIC paired p99 +17.45%.
It **does not clear performance**. Main included V3e/EO1 absent from that
candidate, so it cannot isolate N1c/round 3 or qualify the cumulative source.

The original 8-byte campaign report/detail/aggregate/commands are preserved
byte-for-byte in `../n1c-final-n0/`, with an explicit historical-context README;
original raw evidence remains at parent `target/claude/research-r4/pr23-final-n0/`
and durable comment `6299071370`. No performance or Mac build/workload runs
here. Parent will compare exact stable cumulative sources with the same actual
16-byte Counter/session protocol and flags. No keep/performance-clearance claim.

## Ownership and cleanup

Only the T3 isolated checkout and its `target/` are used for source/builds;
matched reuses the same target. Cores 10–11, two Cargo jobs,
`CARGO_INCREMENTAL=0`; feature configurations run sequentially. Initial relative
+19 launches did not verify actual kernel priority; they are not actual19 evidence.
The bounded set is repeated after absolute19 correction, with per-child checks
and live thread audits; see RESOURCE-NOTE.md for the observed discrepancies. The original
design pack and parent main source/docs/branch are untouched. Reports/raw logs
are in parent `target/claude/research-r4/pr23-final-reconcile/`. The checked
nonforce push to existing PR23 runs the enabled `.githooks/pre-push`.
After recording the exact head, patch and evidence, mandatory own-worktree
sweep, process audit and final disk guard follow; actual cleanup is recorded in
the parent report. No delegated tasks, new PR, detached jobs, schedules,
performance rerun or post-report work is started.
