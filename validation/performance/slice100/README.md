# Slice 100 WAL and host-progress attribution

Base revision: `3e21294`, plus the two benchmark changes committed with this evidence.
Production storage, consensus, transport, timer and runtime behavior is unchanged.
Hardware/compiler/filesystem/device settings are the same as recorded in
[the slice-99 environment](../slice99/README.md): Ryzen 9800X3D, Linux 7.2.9-1-cachyos,
Btrfs on Sabrent NVMe firmware RKT4B5.1, Rust 1.98.1. No exclusive CPU/device
reservation or independent replica machines; other desktop load is possible.
Cargo.lock remains SHA-256
`532a0f956d683a1f35701ce838c35ede07b4bf98ceb5305370b34a27427b7ae8`.

Measured source SHA-256:

- examples/wal_benchmark.rs: `0c175c8dff71d7ae6d1a9e4eba1cbd02b426b6d53f97a1d8408a108bc5a9f2e1`
- examples/native_benchmark.rs: `c9ee16e6b037ffeec3c302d7c5b6d87ee664a3f9322a01cbaedf0083ae5985c6`

Release executable SHA-256:

- wal_benchmark: `fe695a91d5ad47455e90b069528689957bd663336e41732165f31dd300e897a1`
- native_benchmark: `7d92ccfa267eaa5a35030f5aea3f0575ed87cf8367efede749fdf0f756fde81e`

## Local WAL observations

Build: `cargo +stable build --locked --offline --release --example wal_benchmark`.
Run sequentially with arguments `FRESH_ROOT 64 1` and `FRESH_ROOT 64 32`.
Default native/tls features are compiled; no transport or workers are started.
Each run adds eight warm-up batches. Both recovered the exact acknowledged log,
replayed every command into a fresh Counter and verified original first/last
retry outcomes without changing the final value. Raw per-batch CSV and summaries
are retained alongside this file.

| Entries/batch | Measured records | Elapsed s | Local records/s | Barrier p99 ms | WAL sync total ms | Publication total ms | All barriers ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 64 | 1.950159 | 32.818 | 69.309 | 692.377 | 1255.062 | 1948.316 |
| 32 | 2048 | 2.354276 | 869.907 | 89.405 | 726.713 | 1612.872 | 2342.797 |

Publication accounts for about 64–69% of observed barrier time; WAL sync accounts
for about 31–36%. Encoding/append is much smaller. The existing published length
protects detection of torn/corrupt acknowledged prefixes, so these results do not
justify removing publication. Batching amortizes similar barrier cost across
more local records, but this is not quorum throughput or fixed-p99 improvement.

## Replicated host observations

Build: `cargo +stable build --locked --offline --release --all-features --example native_benchmark`.
Runs are sequential, with arguments `FRESH_ROOT tcp|quic 256 1|32`, unchanged
64-operation warm-up, heartbeat 50 ms/elections 1000–1999 ms and default Node
queue/poll budgets. Measured-phase reports sum existing progress over all three
replicas; warm-up, follower drain, verification/reopen and recovery retries remain
outside measurement. All four runs pass recovery/value/retry/join verification with recovered value 320
and zero recovery retries.

| Transport | Window | Applied ops/s | p99 ms | Elapsed s | Host polls total ms | Max poll ms | Persist batches | Worker events | Apply deliveries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| TCP | 1 | 7.106 | 615.773 | 36.024863 | 3950.153 | 5.954 | 1534 | 3068 | 766 |
| TCP | 32 | 26.125 | 2369.064 | 9.798930 | 1308.943 | 1.710 | 350 | 700 | 91 |
| QUIC | 1 | 7.498 | 513.127 | 34.141575 | 4491.359 | 7.767 | 1534 | 3068 | 766 |
| QUIC | 32 | 27.581 | 1780.343 | 9.281825 | 1259.621 | 3.040 | 353 | 705 | 93 |

Serial runs report 1534 persistence batches for 256 useful operations across
three replicas (about six per operation). Window-32 runs report 350/353 batches,
showing existing batching reduces physical barrier work under load. Host poll
wall intervals total about 11–14% of elapsed measurement, with individual maxima
under 8 ms. These observations support investigating durability batching; they
do not isolate the entire critical path or prove an optimization. All four p99
values exceed the predeclared 250 ms next-tuning target.

Independent CSV checks confirm local stage/byte/percentile arithmetic and all
four replicated unique operation IDs, results, applied indices, latency/rate
summaries and diagnostic bounds.

This is finite local diagnostic evidence. Primitive and host timings overlap
with parallel worker/network progress and are not additive critical-path costs.
Sustainable/open-loop/maintenance/multi-group performance, macOS and separate-host
validation and an attributable improvement at fixed p99 remain outstanding.
