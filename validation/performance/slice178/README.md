# Static lane composition evidence

Source base: `432a6e966b7315989e06ab6e975792c5ec5f2bd2`, plus the slice178
runtime changes identified by `release-source-sha256.txt`. Later test-only
changes enable this example in ordinary Cargo test/CI and fix two test lints;
final source hashes are in `../../baseline/slice178/source-sha256.txt`.
Build: `cargo +stable build --locked --offline --release --all-features --example native_benchmark`.
No new dependencies or durability bypass. Host metadata is retained alongside
all samples, static plans, per-lane storage traces and command summaries.

Commands, run sequentially with fresh roots under `target/benchmark-runs/slice178`:

```sh
./target/release/examples/native_benchmark --lanes target/benchmark-runs/slice178/tcp-one tcp 64 8 4 1
./target/release/examples/native_benchmark --lanes target/benchmark-runs/slice178/tcp-two tcp 64 8 4 2
./target/release/examples/native_benchmark --lanes target/benchmark-runs/slice178/quic-two quic 64 8 4 2
node validation/check-native-benchmark.mjs --storage validation/performance/slice178/tcp-one validation/performance/slice178/tcp-two validation/performance/slice178/quic-two
```

All runs use three logical replicas on one Linux machine,8-byte counter commands,
64 total warm-up operations, real synchronized native WALs and the same bounded
Node interfaces as embedding. Each successful summary follows all-replica value
and quorum-read checks, worker joins, file reopen, original retry receipts and
final joins. Source files/WALs remain at the original run roots; archived data
contains raw measurement/diagnostic outputs, not the full stores.

| Configuration | Applied ops/s | p99 ms |
| --- | ---: | ---: |
| TCP,1 lane | 15.265 | 706.891026 |
| TCP,2 lanes | 15.538 | 1075.042194 |
| QUIC,2 lanes | 12.526 | 1058.867266 |

These are finite composition checks on a busy host, with the older baseline175
suite running concurrently. There is no controlled throughput improvement,
sustainable capacity, independent-host scaling or original serial250ms gate
claim. Extra lane stores share the same physical Btrfs/NVMe device. Measurements
exclude warm-up and post-run recovery; the common start includes lane wake-up
delay, while request latency starts immediately before proposal. Worker/thread
counts are explicit in each summary. Automatic placement, live lane migration
and maintenance/offered-load multi-lane modes are not implemented by this slice.
