# PR14: three observed validation repairs

Base: `12a23d6d9a443da167ce91c3e2dd26d95a8267ef`, exact
`origin/lb/st2-maintenance-defaults` when fetched. The approved production
rebase, its history and the accepted CPU cost are unchanged. Scope: test
fixtures, their causal controls and validation records only. No new programme,
slice, PR, dependency, lint exception, assertion relaxation or timeout increase.

The schema plan and ownership explanations are in `docs/IMPLEMENTATION.md`,
"PR14 final merge-gate fixture repairs". ST2 is still in review.

## Causal evidence

- Transfer: the old base ladder reaches 65,200, whose largest listener offset
  (487) overflows u16. Shared port leases now reserve all 24 TCP/UDP pairs with
  checked offsets, finite collision/exhaustion handling and locks retained
  through child shutdown/reopen. The boundary regression fails with unchecked
  `base + offset` substituted: **0 passed, 1 failed**, overflow panic. This
  mutation uses local inputs and never changes a global concurrent allocator.
- Lanes: the close/join drops the original WAL owner, but a concurrent fork can
  temporarily retain its copied CLOEXEC `laneN/replicaN/LOCK` descriptor until
  exec. The causal control deliberately extends that window by passing a copied
  real `LOCK` as child stderr. It logs path/PID, observes typed
  `TryLockError::WouldBlock` after the original closes, explicitly releases/reaps
  the child, then successfully recovers and reopens the WAL. The test-only gate
  shares lane owners and excludes fork/exec across their complete lifetimes;
  children execute outside that gate. Stdout readers are now joined too.
  Six pre-repair batches of 14 lane/cluster cases passed without reproducing
  the spontaneous failure. The deterministic control establishes the ownership
  mechanism; it does not identify the vanished child from the earlier full run.
  An adjacent default-feature lease regression initially returned **8 passed,
  1 failed**: the adoption release check reported port 61,004 still in use after
  owner drop while concurrent fixture forks were unexcluded. That check omitted
  the fixture's existing `SPAWN`
  guard. The adoption and new layout-release checks now use that existing guard;
  both socket-release assertions remain. The failed run is retained in logs.
- Held snapshot: the old compound predicate could consume recovery receipts
  while waiting for the foreground election. Arming now stops at the first
  accepted request (with **zero installs**), then campaigns the foreground group
  using the healthy quorum while the stale replica stays unpolled. The actual
  router request remains held through foreground apply, checkpoint and a later
  WAL reclaim. Restoring the old leader conjunction under the delayed-election
  ordering reproduces the original **arming timeout / caught-up follower /
  recovery requests=0** failure: **0 passed, 1 failed**, 128.48 s with the
  unchanged 120 s progress guard. An earlier conjunction control instead reached
  `Unknown(LeadershipChanged)` in foreground (20.13 s); that result is retained
  as a mutation limitation, not treated as the arming reproduction.

Both mutations were reverted before final validation. Successful maintenance
histories still require real snapshot installs for all eight groups, bounded
recovery usage, foreground quorum progress, checkpoint/reclaim, reopen and
deduplication. No invented held state or sleep establishes arming.

Baseline accounting: the finite controller requested eight batches. Its first
six used the unchanged old executable (50 filtered tests). A local rebuild
preceded batch seven; the last two (51 filtered tests) are excluded from the
baseline count. The raw controller command and all output are retained, without
a saved executable or a second source/build tree.

## Execution constraints

Linux, `taskset -c 12-15 nice -n 19`; `CARGO_BUILD_JOBS=4`,
`CARGO_INCREMENTAL=0`, `CARGO_PROFILE_DEV_DEBUG=0`,
`CARGO_PROFILE_TEST_DEBUG=0`, `CARGO_TARGET_DIR=$CHECKOUT/target`.
`TMPDIR=$CHECKOUT/target/scratch` owns all task scratch. No source copies or
other build directories. Feature configurations execute sequentially. Guards
run before each build/task; the push uses a cargo wrapper that runs the unchanged
shared diskguard plus the 45 GiB own-target bound before each hook command.

## Final focused validation

Commands and raw output are in `logs/`; counts describe selected cases, not a
whole configuration. Required lint/hook results are recorded after execution.

| Selected check | Configuration | Passed | Failed |
| --- | --- | --- | --- |
| Lanes + cluster spawning/cleanup + held maintenance + descriptor control | All features | 17 | 0 |
| Same selection (including the no-QUIC argument check) | Default | 15 | 0 |
| Transfer boundary + TCP split + QUIC crash/recovery | All features | 3 | 0 |
| Transfer boundary + TCP split | Default | 2 | 0 |
| Shared lease, offset layout and ownership regressions | All features | 9 | 0 |
| Same lease regressions after adding the existing spawn guard | Default | 9 | 0 |

The lease allocator preserves per-lock namespace-error skipping and permits its
diagnosed namespace fallback only when no candidate could use the namespace;
ordinary layout exhaustion stays in the common namespace and fails finitely.
After this final error-path refinement, all-feature lease 9/0 and transfer 3/0
passed again. Panics printed by the successful lease negative controls are
expected and caught; the separate 8/1 run above is an actual failed test.

The tracked `.githooks/pre-push` was invoked directly and passed all seven
checks below with **zero diagnostics** (exit 0). The first instrumentation
attempt exited 127 before Cargo because the guard wrapper used a nonexistent
Cargo path; it was corrected to `/usr/bin/cargo`. No hook/check was bypassed.

```sh
cargo +stable fmt --all -- --check
cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
```

Observed own-target use after qualification: **923 MiB**, below 45 GiB. Shared
diskguard stayed green with unchanged caps. The actual push runs the enabled
hook again; its result, final head and cleanup guard are retained in the durable
PR14 comment and the parent's `target/claude/research-lv/pr14-final-fixes` report.

The parent owns the final exclusive full matrix and final source confirmation.
No full suite or soak is repeated here. The rebase result remains default
2,657/0/1 ignored and all-features 2,966/3/1 ignored until that gate is rerun;
isolated/focused passes do not clear those full-run failures or authorize merge.
