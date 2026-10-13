# Paired slices 411/412 measurements

Both hosts completed 96/96 cases: 24 durable native and 72 volatile framework cases, with unchanged source and executable hashes. Tables report medians of three interleaved trials per variant, comparing frozen c35e1f1 (1336 inputs) with the exact 1345-input 411/412 snapshot published as 8d50b0e. Source manifests identify the actual bytes; parent-commit metadata alone does not identify a dirty snapshot.

## Durable native results

Three replicas on one host, actual synchronized FileLogIo plus manifest publication, authenticated loopback TCP/QUIC, eight-byte Counter commands, 64 warmup and 256 measured operations per case. Every original Applied receipt, final state and close/reopen retry was checked. Windows 1/32 bound simultaneous requests. These are end-to-end diagnostics with substantial storage I/O, not isolated engine throughput.

| Host | Transport/window | Reference writes/s | Candidate writes/s | Ratio | Reference→candidate p99 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| Linux | TCP/1 |9.410|43.783|4.65×|239.113→124.964|
| Linux | TCP/32 |32.084|36.655|1.14×|1900.026→1400.933|
| Linux | QUIC/1 |8.853|47.342|5.35×|283.601→100.376|
| Linux | QUIC/32 |27.718|33.603|1.21×|2230.018→1727.331|
| M2 Max | TCP/1 |11.666|11.251|0.96×|98.187→227.419|
| M2 Max | TCP/32 |56.450|55.521|0.98×|1061.555→1101.984|
| M2 Max | QUIC/1 |11.171|11.458|1.03×|102.062→96.232|
| M2 Max | QUIC/32 |48.423|50.450|1.04×|1315.932→1148.205|

The Linux singleton improvement did not reproduce on APFS/M2 Max. Candidate window 32 remains slower than singleton on Linux; Mac TCP singleton p99 increased. Physical I/O counts remain roughly two leader sync/publications per singleton write. Mean measured call durations are substantial: Mac WAL sync 8–10ms and manifest publication 23–28ms; Linux singleton candidate approximately2.4ms/5ms versus reference12ms/22ms. These averages suggest an I/O scheduling/overlap investigation; they do not prove critical-path attribution. Three replicas share one machine and filesystem. Small 256-operation runs and three trials cannot establish sustained production capacity. Post-campaign hardware/filesystem inventories are in platforms/; compiler versions are in each build log. The harness does not trace Written/send/Durable ordering; separate correctness controls cover that contract. Startup logical barrier/encoded-byte columns are unpopulated and must not be read as zero work.

## Volatile framework diagnostic

Three in-process voters, zero-byte commands, one cooperative driver, volatile exact-ticket storage, 1024 warmup writes and 16,384 measured writes per case, with retained history. Clients 1/64/256/512/1024/4096 and batches 1/4 cover 12 cells per variant/trial. It exercises the public provider change in 412; it does not exercise 411's physical-owner overlap path.

| Host | Range of cell throughput ratios | Candidate singleton writes/s | Highest candidate batch writes/s |
| --- | ---: | ---: | ---: |
| Linux |5.20–64.80×|160,433|403,798|
| M2 Max |13.17–34.13×|128,437|805,280|

These remain unmatched OpenRaft diagnostics: no pinned upstream comparison, different scheduler/store and no checkpoint purge. They neither meet nor establish the requested OpenRaft minimum. Full cell medians and p99 values are in each host's PAIRED_SUMMARY.json; every individual result remains in RESULTS.json and raw files.

## Evidence

Each host directory preserves the complete original hashed archive: 823 Linux files and 824 Mac files (Mac includes its runner log), plus EVIDENCE.json. Commands, exits, samples, journal/publication counters and exact persisted journal/manifest bytes are retained. measure.mjs is the actual driver. VERIFIED.json records pre-copy hash/profile validation.

builds/{linux,mac}/{reference,candidate} contains exact source manifests, build commands, compiler JSONL and terminal results. Three genuine fresh:false targets per build used opt3 with debug assertions and overflow checks enabled; reference/candidate artifact hashes differ. Executables, rlibs, checkout/build trees and transfer archives are omitted. Baseline ORIGINAL_EVIDENCE.json preserves the source archive's full original manifest, including references to omitted compiled artifacts; each build's EVIDENCE.json covers the published metadata. No timed 413 results are included.
