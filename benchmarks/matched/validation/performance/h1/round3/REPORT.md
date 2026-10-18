# H1 builder round 3 — 17 October 2026

## Decision and scope

Revert `b58f57bf`, retaining only the `297aec15` worker flag in matched source.
The previous round already reverted the current-thread runtime (`80cab34e`).
Neither overhead candidate is retained as an option. No library source, manifest,
lock, dependency, unsafe code, lint threshold or attribution code changes.
The worker flag remains default 16 and reports its effective value in both JSON
fields. No performance claim accompanies the flag.

Ran `git revert --no-commit b58f57bf`. Its documentation conflict was resolved
by amending the current H1 record; the raw `buffer-pairs.txt` and `environment.txt`
that the original commit added were restored unchanged rather than deleted.
All existing raw JSON, text measurements, hashes and logs remain byte-identical.

The [initial quiet-Mac report](https://github.com/KarlLivesey/VoteBoat/pull/2#issuecomment-6239967157)
was inconclusive: eight bulk pairs had mean width +5.779%, wider in 7/8,
exact sign p=0.070312. The independent fixed
[30-pair quiet-Mac report](https://github.com/KarlLivesey/VoteBoat/pull/2#issuecomment-6240057138)
classified **WORKLOAD CHANGED**. Thirty fresh AB/BA pairs (15 of each order),
60 retained runs, no exclusions or selective reruns; pre-run 1-minute load
1.31–2.54 on 12 CPUs. Earlier Mac and loaded-Linux runs were not pooled.

| Metric | Median head/flag ratio | Change | Head lower / higher / tied | Exact two-sided sign p |
| --- | ---: | ---: | ---: | ---: |
| Mean proposal width | 1.058583 | +5.858% | 0 / 30 / 0 | 1.8626451e-9 |
| Median proposal width | 1.068793 | +6.879% | 1 / 29 / 0 | 5.7742e-8 |
| Persists/original | 0.983757 | −1.624% | 28 / 2 / 0 | 8.6799264e-7 |
| Sends/original | 0.998246 | −0.175% | 21 / 9 / 0 | 0.042773945 |

A is flag-only `297aec15`; B is prior head `2fe7fe16` (buffer with the original
runtime restored). Each run completed 3,997,696 originals / 999,424 requests,
frontiers `[3998721,3998721,3998721]`, zero attribution failures, 16 server and
one client worker. Cell: VoteBoat, 4096 clients, batch 4, requested 4M, 1024
warmups, default retention/upstream envelopes, attribution on, 600s campaign
budget. Mean/median widths exclude warmup singletons; persistence and send
lifetime counters sum replicas and divide by completed originals plus warmup.
Ratios are medians of within-pair B/A ratios. Significant mean widening beyond
the symmetric ±3% band changes the benchmark instrument despite equal totals.

[Claude Opus round 1 N2](https://github.com/KarlLivesey/VoteBoat/pull/2#issuecomment-6239604979)
already found the historical buffer CPU gain was noise. The later Mac CPU
observation does not qualify a changed workload. The prior buffer keep decision,
and the loaded-Linux null interpretation, are superseded by this Mac result and
the binding decision to preserve the scoring instrument. Raw historical data
remain available; no library speedup or p99 result is inferred.

Full local runner reports remain in the main checkout:
`target/claude/research-r1/runner-pr2-mac-attribution.md` and
`target/claude/research-r1/runner-pr2-mac-attribution-30.md`, with original JSON,
time/stderr and reproducible scripts under `runner-pr2-artifacts/` (fresh series
in `round30/`). The published comments above preserve both report summaries.
This builder read those reports; it did not rerun the Mac campaign.

## Exact restoration

```text
$ git diff 297aec15 --exit-code -- benchmarks/matched/src
exit 0; stdout 0 bytes; stderr 0 bytes
Byte-for-byte git blob audit: 26/26 matched source files equal 297aec15; no added or missing files
Raw evidence preservation: 12/12 existing .txt/.json/.log files unchanged from 2fe7fe16
```

The audit hashes every source file's actual bytes against its `297aec15` git
blob, checks the complete path set, and separately compares all existing raw
`.txt`/`.json`/`.log` files against `2fe7fe16`. Thus the retained flag's entire
source tree, including both runtime constructions and all workload code, is
restored exactly. No numeric requalification of the discarded buffer is claimed.

## Actual checks

`core.hooksPath=.githooks`; no hook bypass or lint weakening. Direct invocation
of the unchanged tracked pre-push hook returned zero and ran both fmt checks and
all five strict Clippy configurations sequentially. All returned zero diagnostics.

| Check | Round 3 result |
| --- | --- |
| `cargo +stable fmt --all -- --check` | Pass |
| `cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check` | Pass |
| Root strict Clippy, default | Pass, zero diagnostics |
| Root strict Clippy, `--all-features` | Pass, zero diagnostics |
| Root strict Clippy, `--no-default-features` | Pass, zero diagnostics |
| Root strict Clippy, `--no-default-features --features native` | Pass, zero diagnostics |
| Matched strict Clippy, own manifest/lock offline, all targets | Pass, zero diagnostics |
| `cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline` | 71 passed, 0 failed |
| Source equality against `297aec15` | Pass, 26/26 files byte-identical |
| Existing raw evidence against `2fe7fe16` | Pass, 12/12 files unchanged |
| `git diff --check` | Pass |

All strict Clippy commands use `cargo +stable clippy --locked --offline
--keep-going --all-targets [FEATURES] -- -D warnings`; matched additionally uses
`--manifest-path benchmarks/matched/Cargo.toml`. Local build parallelism was
limited with `CARGO_BUILD_JOBS=4`; no feature-configuration executables ran
concurrently. Logs are local under `/tmp/voteboat-pr2-r3-checks/`:
`preflight-hook.log`, `matched-tests.log` and `source-verification.txt`.
The actual push reruns the enabled hook; its result is recorded in the PR
builder response rather than predicted here.

This builder did not rerun macOS benchmarks, a release build, executable controls,
root test suites or remote CI. The Mac evidence is the runner's completed check;
it is not CPU/throughput qualification for both engines. Independent review of
this reduced round-3 scope remains outstanding. No processes were stopped and
no merge was performed.
