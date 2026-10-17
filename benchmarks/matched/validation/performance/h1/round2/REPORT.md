# H1 builder round 2 — 17 October 2026

Reverted `80cab34e` with `git revert --no-commit`, then retained and relabeled
the rejected experiment's raw evidence. `benchmarks/matched/src/main.rs` matches
`297aec15` exactly; all matched source matches `b58f57bf`. The one-worker
multithread client runtime, thread name/stack/drivers and cleanup are restored.
The worker flag and fixed ID buffer remain. No alternate mode, new dependency,
unsafe code, observer edit or library edit was introduced.

## Attribution diagnostic

Exactly one bulk run on the fixed tree; one flag-only and one rejected-binary
control, in that order, sequentially after this builder's builds/checks ended.
All three binaries are release+debuginfo, stable Rust 1.98.1, on the Linux
9800X3D host. `binary-hashes.txt` identifies the binaries; the saved controls'
hashes match `../buffer-pairs.txt` and `../current-pairs.txt`. Source for the
fixed binary equals `b58f57bf`; its build is isolated in `target/h1-r2-release`.
Other workloads were left running. Observed load 95.15–95.30 (see
`attribution-load.txt`), substantially above the anticipated 40–60.

Each binary was invoked with:

```sh
BINARY --engine voteboat --clients 4096 --batch 4 --operations 4000000 \
  --server-workers 16 --warmup 1024 --attribution true --campaign-timeout-secs 600
```

Default retention and upstream-default envelopes. Full stdout JSON is retained
as `flag-only.json`, `fixed-runtime.json` and `rejected-current.json`; stderr was
empty and all exits were zero. No outlier was removed and no sample was rerun.

| Binary | Measured proposals | Mean width | Whole-run persists | Persists/original | Sends/original |
| --- | ---: | ---: | ---: | ---: | ---: |
| Flag-only `297aec15` | 7229 | 553.01 | 24999 | 0.006252 | 0.007339 |
| Restored runtime + fixed buffer | 8111 | 492.87 | 27354 | 0.006841 | 0.007540 |
| Rejected `80cab34e` | 7987 | 500.53 | 26840 | 0.006712 | 0.007461 |

Mean width is completed measured originals divided by the leader's
`proposal_batches - settings.warmup`; warmups are singleton proposals. Persists
sum `diagnostics[].persistence_units` across all replicas. Sends sum calls of
the attribution `send` phase across all replicas. Whole-run counters divide by
`completed_originals + settings.warmup` (3,998,720); startup work remains included.
Full proposal-width histograms remain in the JSON.

Every run completed 3,997,696 originals / 999,424 requests, all frontiers
`[3998721,3998721,3998721]`, maximum proposal width 16384. **Numeric equivalence
to flag-only is not established:** the fixed sample is 10.87% narrower with
9.42% more persists per original, and is close to this rejected-runtime sample.
Do not report the requested numeric matching acceptance as passed. The runtime
restoration is exact in source; this single loaded-host sample does not establish
statistical workload equivalence or a portable causal effect. Quiet-Mac runner
qualification remains necessary. No CPU/speed or latency win is claimed.

For context only, GPT-6.1-Sol round 1's three-pair fixed-buffer→current-thread
attribution-on medians were width 687.36→512.33 and persists/original
0.005866→0.006617; attribution-off 715.28→554.08 and 0.005997→0.006287. Different
load/experiment windows must not be combined into a performance result.

## Actual checks

`core.hooksPath=.githooks`; no hook bypass. `checks.log` is a successful direct
invocation of the tracked pre-push script before committing. The actual push
also runs the enabled hook. All commands below returned zero unless the
expected expiry result is stated.

| Check | Result |
| --- | --- |
| `cargo +stable fmt --all -- --check` | Pass |
| `cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check` | Pass |
| Root strict Clippy, default | Pass, zero diagnostics |
| Root strict Clippy, `--all-features` | Pass, zero diagnostics |
| Root strict Clippy, `--no-default-features` | Pass, zero diagnostics |
| Root strict Clippy, `--no-default-features --features native` | Pass, zero diagnostics |
| Matched strict Clippy, `--locked --offline --keep-going --all-targets -- -D warnings` | Pass, zero diagnostics |
| `cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline` | 71 passed, 0 failed (`matched-tests.log`) |
| Matched release build, `--locked --offline` | Pass (`release-build.log`) |
| Production executable: both engines, 1/2/16 server workers | Six controls pass: both worker JSON fields match, 128 originals / 32 requests, frontiers `[133,133,133]` |
| Production executable: both engines, one-second campaign expiry | Both exit 1 with campaign expiry, no cleanup error |
| Source equality / whitespace | `git diff 297aec15 --exit-code -- benchmarks/matched/src/main.rs`, `git diff b58f57bf --exit-code -- benchmarks/matched/src`, `git diff --check`: pass |
| Flag-only numeric workload-shape match | **Not verified**, figures above |

Root Clippy commands use `cargo +stable clippy --locked --offline --keep-going
--all-targets [FEATURES] -- -D warnings`. Matched commands specify
`--manifest-path benchmarks/matched/Cargo.toml`. No lint levels were weakened.

Executable controls use `--clients 2 --batch 4 --operations 128 --warmup 4
--server-workers N`. Expiry uses `--clients 1 --batch 1 --operations 20000000
--warmup 0 --campaign-timeout-secs 1` for each engine. Both expiry stderr streams
contain only `Error: Expired { phase: "campaign", budget: 1s, source: Elapsed(()) }`.
These are manual controls of `main`'s two-runtime path; the worker/JSON unit test
alone does not certify that topology or scheduler equivalence (review P3).

Not run/verified here: macOS, quiet scaling/capacity campaigns, root test suites,
remote CI outcomes. No merge performed. Independent review remains pending.
