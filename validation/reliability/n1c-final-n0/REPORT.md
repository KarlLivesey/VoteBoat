# PR23 final-head N0 performance qualification — 20 October 2026

**Disposition: KEEP standalone N1c in the wind-down reconciliation queue under its previously accepted qualified tradeoff. DECLINE final-head performance clearance from this campaign.** The requested net-head comparison has CPU regressions at TCP4k and TCP15k, and a QUIC p99 regression. These losses must remain in the integration record. This is a performance result, not a source-correctness finding or a full-suite gate.

**The pinned heads differ in more than N1c.** A is main `b4e3647250d6b3ae6c770bae6ae98e88f0b3c04c`; B is exact PR23 final `44b95083e96f7b09ab4e900ed4e1c25042896c2a`. Their merge-base is `12fb21de87cf31e3799fc9bc93d56a6da726f2b7`. A includes merged V3e/PR21 and EO1/PR20 absent from B. The campaign therefore cannot isolate N1c, cannot isolate the round-3 zero-progress guard from round 2, and cannot qualify a future rebased or merged head. No causal regression in N1c or its startup guard is established. The parent will reconcile the candidate onto cumulative main before a final merged-head gate if needed; this runner did not change or remeasure another head.

## New evidence: pinned Mac comparison

Mac `karllivesey@192.168.192.76` / `Mac.localdomain`, Apple M2 Max, macOS 27.0 (26A428). Rust stable 1.98.0 (88d9e12ae), release, locked/offline, all features, `native_benchmark`. Both exact Git archives built sequentially in their own permitted roots and own `target/`; no `CARGO_TARGET_DIR`. Source-input blob checks: A 764 and B 763 files, zero mismatches. Same committed `cluster.py` bytes on A/B (SHA256 `682c189a919ed92001bac1e5e5eca2b28dd186e22708cf8ae0226593b24063fd`), same Cargo files/lock and cluster node implementation. No instrument, hold, production-source or ready-items changes.

Existing N0 settings: one group, three processes, 8-byte commands, 64 warmups, window 1024, production HostWait, fresh native WAL on APFS RAM volume `/Volumes/vbram`, unchanged native WAL/durability defaults. TCP1k/4k use 16,000 offers; TCP15k and QUIC15k use 2,000/16,000 offers; idle20/s uses 600 offers (30 s horizon). All CPU of all three node processes/threads is summed via existing wait4 logic, including startup, campaign, warmup, offerer, WAL workers, measurement, quorum read, drain and exit. Startup is not removed from CPU or counters. p99 covers the offered phase's intended-time-to-completion.

Five existing rounds per cell, using unmodified schedule AB/BA/AB/BA/AB; within 15k rounds the offer-count order also reverses. This matches the requested historical `--pairs 5` schedule (five rounds, not five independent ABBA quartets). It has one extra A-first round; no extra control or rerun was added to rebalance it. Mac load1<1 and no competing detected benchmark/build were required before each round; waits and live process snapshots are retained. Busy cores are summed node CPU divided by the common first-spawn-to-last-exit interval. This is average accounted node CPU, not peak concurrency or all host/kernel/RAM-disk helper CPU.

**Runs:** 70 attempted/completed; 0 invalid; valid paired rounds per cell: tcp1k=[0, 1, 2, 3, 4]; tcp4k=[0, 1, 2, 3, 4]; tcp15k=[0, 1, 2, 3, 4]; quic15k=[0, 1, 2, 3, 4]; idle20=[0, 1, 2, 3, 4]. No run or failed pair was silently dropped.

Ratios are medians of **within-round B/A ratios**, not ratios of separately rounded medians. The 15k decision ratios use `(CPU16000 − CPU2000)/14000` within the same complete round for each binary. A single invalid run drops the entire affected round, including both offered sizes and binaries; no cross-round pairing is permitted. No rounds were dropped in this completed campaign.

| Cell | Total µs/write A → B (large offer count) | Decision CPU B/A (pairs ≤1) | Marginal µs/write A → B, if used | Busy cores A → B | Paired busy B/A | Completed-rate B/A |
|---|---:|---:|---:|---:|---:|---:|
| tcp1k | 465.554 → 454.906 | **0.976676** (3/5) | — | 0.4638 → 0.4529 | 0.976607 | 1.000031 |
| tcp4k | 112.478 → 122.561 | **1.089646** (2/5) | — | 0.4437 → 0.4816 | 1.086476 | 1.000252 |
| tcp15k | 29.983 → 36.361 | **1.250908** (2/5) | 30.595 → 36.862 | 0.4227 → 0.5140 | 1.242247 | 1.003484 |
| quic15k | 52.548 → 51.412 | **1.052628** (2/5) | 50.535 → 50.482 | 0.6940 → 0.6787 | 1.008432 | 0.998821 |
| idle20 | 7361.063 → 6501.850 | **0.879391** (5/5) | — | 0.1469 → 0.1298 | 0.879287 | 1.000050 |

Completed-rate ratios in this table use the large offer count; all its individual pairs are within ±1.75%. The matched offered rate is unchanged for every run. The short 2,000-offer drain-inclusive completed-rate ratios range from 0.9511 to 1.0343; these valid N0 runs remain in their original marginal pairs and are shown separately in DETAIL.md. Harness validity does not itself imply a performance-gate pass.


CPU at the large offer count, per node (µs/completed write; node 1 is leader). Per-node medians need not add to the separately calculated median total. Exact per-node ratios, busy cores, 2,000-offer totals and per-node marginal values are in [DETAIL.md](DETAIL.md).

| Cell | A leader / follower2 / follower3 | B leader / follower2 / follower3 |
|---|---:|---:|
| tcp1k | 266.40 / 99.45 / 99.71 | 278.74 / 89.33 / 88.08 |
| tcp4k | 65.53 / 24.27 / 24.10 | 74.55 / 23.84 / 23.33 |
| tcp15k | 18.31 / 5.91 / 5.80 | 23.44 / 6.52 / 6.40 |
| quic15k | 38.75 / 6.71 / 7.12 | 42.40 / 5.67 / 5.59 |
| idle20 | 3151.94 / 2148.37 / 2063.18 | 3045.15 / 1568.73 / 1961.20 |

### Latency and every paired ratio

| Cell (large offer count) | p99 ms A → B | Median paired B/A | r0, r1, r2, r3, r4 p99 B/A |
|---|---:|---:|---|
| tcp1k | 18.685 → 18.124 | **0.919071** | 0.882, 0.919, 0.975, 1.017, 0.913 |
| tcp4k | 19.115 → 18.763 | **0.964362** | 0.964, 1.135, 0.932, 0.943, 0.976 |
| tcp15k | 26.102 → 25.795 | **0.932294** | 1.063, 0.751, 0.987, 0.932, 0.907 |
| quic15k | 18.356 → 26.947 | **1.174531** | 1.495, 1.510, 1.175, 0.883, 1.126 |
| idle20 | 16.858 → 16.713 | **0.991563** | 1.012, 0.992, 0.963, 0.995, 0.981 |

TCP4k is explicit: its paired p99 median improves here, but r1 is +13.5%; this does not establish a universal ≤5% latency gate. QUIC15k has +17.45% paired median p99, with four of five ratios above 1. TCP15k CPU ratios straddle widely: `1.966, 0.596, 1.251, 0.561, 1.291`; the record shows order/regime sensitivity, and no attribution control was added. All short 2,000-offer p99 values and their own round ratios remain in DETAIL.md.

Decision CPU ratios by round (all retained):

| Cell | r0, r1, r2, r3, r4 |
|---|---|
| tcp1k | 0.941, 0.911, 1.042, 0.977, 1.015 |
| tcp4k | 1.097, 0.875, 1.090, 0.819, 1.123 |
| tcp15k | 1.966, 0.596, 1.251, 0.561, 1.291 |
| quic15k | 1.053, 1.061, 0.898, 0.823, 1.059 |
| idle20 | 0.874, 0.883, 0.878, 0.910, 0.879 |

### Counters supported by the unchanged harness

Whole-process totals per completed write at the large offer count. These include startup/warmup/read/drain, matching CPU attribution. Full node splits and the wait/immediate/ready/timeout split for both offer counts are in DETAIL.md and raw stdout.

| Cell | Poll rounds A → B | Peer sends/frames A → B | Received frames A → B | Persistence batches A → B |
|---|---:|---:|---:|---:|
| tcp1k | 4.982563 → 4.946250 | 0.770750 → 0.686375 | 0.770750 → 0.686187 | 0.865812 → 0.860187 |
| tcp4k | 1.275062 → 1.236563 | 0.198000 → 0.171750 | 0.198000 → 0.171687 | 0.220500 → 0.215500 |
| tcp15k | 0.329000 → 0.305250 | 0.051375 → 0.041875 | 0.051312 → 0.041875 | 0.053875 → 0.051687 |
| quic15k | 4.418750 → 5.015062 | 0.068750 → 0.044125 | 0.068687 → 0.044062 | 0.093875 → 0.068937 |
| idle20 | 60.555000 → 54.066667 | 11.970000 → 9.966667 | 11.970000 → 9.966667 | 6.023333 → 5.148333 |

The pinned cluster mode prints no Append-specific RPC/entry counters or leader data-versus-commit-only persistence split. These are **unavailable in this new campaign**, not zero. No PR22 overlay or new instrumentation was added to manufacture them, and peer frame counts were not relabelled as Append counts. Historical Append/persist breakdowns in PR23 are not new final-head evidence.

## Historical acceptance — separate from the new evidence

The PR's accepted standalone N1c round-2 ratios were **.866/.946/.951/.939/.878** for TCP1k/TCP4k/TCP15k/QUIC15k/idle20, with Linux marginal ratio **.992 (7/10 pairs ≤1), neutral**. Its acceptance already carried approximately +5% slow-regime TCP15k p99 and the TCP4k caveat. Those figures are copied from the existing PR evidence; this runner did not repeat, recalculate or qualify them as measurements of the final head against b4e36472. Their older baseline and head must not be conflated with this net-head campaign.

The failed **N1c+N1** combined gate remains a separate historical decision. This campaign runs standalone pinned N1c, contains no N1 combination or hold-duration controls, and neither clears nor re-rejects that combined gate.

No final-head Linux nat measurement ran. Historical Linux neutrality remains historical; exact-final-head Linux CPU is unverified. No additional Linux measurement is requested by this handoff because the parent first needs the cumulative reconciled head. If a final Linux CPU claim is required there, its missing check is the existing matched 2k→16k marginal native loop on that exact reconciled head against its cumulative baseline, queued by the parent inside the exclusive suite checkout; do not run raw nat.sh or create another tree/target.

## Validity, boundaries and cleanup

Raw stdout/stderr, result.json, exact node argv/base ports, all exit/rusage records, compiler/build logs, source manifest, quiet waits and live host snapshots are retained under [measurement/](measurement/), with [COMMANDS.md](COMMANDS.md), [aggregate.json](aggregate.json), [DETAIL.md](DETAIL.md), [record-audit.txt](record-audit.txt) and [pinned-comparison-provenance.txt](pinned-comparison-provenance.txt). The independent artifact audit checks raw summary/counters, 70 unique scheduled records, applied/read values, refusals/term, child exits, common wall stamps and wait4 CPU cross-checks. Valid runs apply all offers with zero refusal/unknown/pending and unchanged leader term.

One **pre-launch infrastructure block**, not an invalid benchmark run: required diskguard failed before idle20 at 301 Linux `/tmp/voteboat-*` directories (cap 300). The runner launched no idle process then and deleted no shared/other-owner scratch. The parent reported 74 safely owned dead-PID fixture removals; its audit is copied as [owner-cleanup.json](owner-cleanup.json). Fresh guard passed at 218 directories, 115 GiB worktree targets, 14 GiB Mac loop; only then did the final idle cell launch. The original failed guard and successful recheck are both preserved.

No source correctness or full-suite conclusion follows. Both round-3 source reviewers' approvals and builder focused/mutation/lint results were read as existing evidence, not independently rerun. Exclusive final suites belong to the integration parent. This runner did no Linux Cargo build/test, checkout, worktree, main source/docs/memory change, production edit, new research, delegation, schedule or detached job. Only evidence in this report directory was written on Linux.

Both named Mac source/build roots and the owned WAL scratch are deleted after raw logs are copied and checked. See [cleanup.json](cleanup.json), [source-verification-after.txt](source-verification-after.txt) and [diskguard-after-cleanup.txt](diskguard-after-cleanup.txt) for actual terminal state; cleanup does not remove other Mac trees, reference tools, shared port leases, or any Linux target/worktree. No owned Mac node, controller or build process remains. No further workload or rerun is scheduled.

Reporting-only note: the first report render failed on an unescaped percent sign. The formatter was corrected and regenerated from the same audited records; no workload was repeated. See report-generation-notes.txt.

Endpoint host names observed: Mac.localdomain at setup and Karls-MacBook-Pro.local at the final source/binary audit; the SSH endpoint stayed192.168.192.76 and all pinned source/binary hashes matched.

