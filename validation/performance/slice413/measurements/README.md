# Configured 413 volatile staging measurements

Linux and M2 Max each completed 96/96 cases with unchanged source/artifact hashes and identical paired sample/codec shapes. Five diagnostic controls per production variant passed on both hosts before timing. All raw results and guarded build proofs are retained.

Compare production 8d50b0e with cab9d6d, each overlaid with the exact same five-file configured/checkpoint diagnostic. Frozen source manifests have 1346/reference and 1350/candidate inputs; later repository HEAD changes do not enter these runs. Separate genuine fresh:false library/example release builds use opt3 with assertions and overflow checks enabled. Build proofs and immutable-original hashes are in builds/. Executables, rlibs, source/build trees and transfer archives are omitted.

## Linux results

Durations are median of three per-case mean total durations, including provider append and its simulated barrier; ratios are reference/candidate. Each case measures 256 transitions after 16 warmup transitions: four history shapes × two histories × two modes × three paired trials × two variants. Retained history grows identically in each pair; unrelated groups 0.

| History/mode | 256 entries: reference→candidate µs | Ratio | 4096 entries: reference→candidate µs | Ratio |
| --- | ---: | ---: | ---: | ---: |
| plain/append | 1.984→1.960 | 1.01× | 2.015→2.033 | 0.99× |
| plain/append-commit | 2.152→2.222 | 0.97× | 1.585→2.059 | 0.77× |
| stable/append | 9.768→1.878 | 5.20× | 96.620→1.536 | 62.89× |
| stable/append-commit | 9.239→1.595 | 5.79× | 92.472→1.529 | 60.49× |
| joint/append | 9.355→1.545 | 6.05× | 90.673→1.530 | 59.24× |
| joint/append-commit | 9.499→1.531 | 6.21× | 88.463→1.533 | 57.71× |
| checkpoint/append | 9.311→1.588 | 5.86× | 85.237→1.554 | 54.86× |
| checkpoint/append-commit | 9.260→1.587 | 5.84× | 88.919→1.551 | 57.33× |

Configured/checkpoint paths improve 5.20–6.21× at 256 entries and 54.86–62.89× at 4096. Plain 4096 append-commit regressed 1.585→2.059µs (~30% slower), so this is not a universal speedup. Every trial remains available; pair ratios and per-case nearest-rank p99 values are in PAIRED_SUMMARY.json/RESULTS.json. These short microsecond measurements and three trials do not establish sustained capacity.

## M2 Max results

| History/mode | 256 entries: reference→candidate µs | Ratio | 4096 entries: reference→candidate µs | Ratio |
| --- | ---: | ---: | ---: | ---: |
| plain/append | 2.712→2.738 | 0.99× | 2.691→2.709 | 0.99× |
| plain/append-commit | 2.697→2.710 | 1.00× | 2.721→2.704 | 1.01× |
| stable/append | 11.029→2.738 | 4.03× | 86.063→2.688 | 32.01× |
| stable/append-commit | 10.952→2.721 | 4.03× | 87.358→2.697 | 32.39× |
| joint/append | 11.511→2.766 | 4.16× | 86.965→2.716 | 32.01× |
| joint/append-commit | 11.432→2.739 | 4.17× | 87.176→2.703 | 32.26× |
| checkpoint/append | 11.345→2.733 | 4.15× | 87.609→2.726 | 32.14× |
| checkpoint/append-commit | 11.404→2.714 | 4.20× | 87.368→2.723 | 32.08× |

Configured/checkpoint paths improve 4.03–4.20× at 256 entries and 32.01–32.39× at 4096; candidate total durations are about 2.69–2.77µs. Plain cells remain approximately unchanged (0.99–1.01×), so the Linux plain append-commit regression did not reproduce on this host. All three pair ratios and per-case p99 measurements remain available.

The real native codec and validation execute against a bounded memory JournalIo. Every transition checks its exact ticket, single append/simulated sync/publication and encoded/manifest byte counts. Complete final-state and fresh-session recovery/stale-ticket oracles use the full public validator outside the measured durations. Checkpoint histories include validated snapshot-reference and membership metadata; no application snapshot image is created, installed or timed by this log-only diagnostic. This is VOLATILE staging, with no actual disk durability, network, Raft quorum or application-throughput measurement; it cannot establish the requested OpenRaft floor.

The initial restricted-sandbox launch returned contradictory Node spawnSync EPERM/status0, aborting before accepting any cases. invalid-sandbox-launch/ preserves its original CSV/stdout/error; none enter statistics. The focused correction changed only execution permission, then ran the unchanged frozen driver into a fresh directory. All 96 corrected cases passed without retries. Every raw argv, exit, stdout/stderr and 256-row CSV is retained with hash manifests.
