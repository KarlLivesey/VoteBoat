# Slice183: written-stage replication experiment — not adopted

The default source was restored. The tested candidate did not demonstrate a
performance benefit, and its first reference run failed after a leadership change.
`candidate.patch` preserves the complete source/test experiment against
`8065503efe948b6fb023197999a90adbe0997433`; `git apply --check` verifies it still
applies. The full P0–P7 goal and the original 250ms p99 requirement remain open.

| Run | Applied ops/s | p99 | Outcome |
| --- | ---: | ---: | --- |
| Preserved control | 5.964 | 873.493ms | Complete; fixed gate fails |
| Candidate first run | — | — | Invalid at operation202; leadership changed |
| Candidate diagnostic | 5.521 | 686.150ms | Complete; instrumented, not the reference gate |
| Candidate repeat | 5.223 | 1107.214ms | Complete; fixed gate fails |

All completed runs recovered320, verified original retries and joined all
workers. The failed candidate retained137 measured completions and joined all
workers; its partial data must not be treated as a complete benchmark or a
successful recovery check. The operation immediately preceding the failure took
1986.913ms. Replica3 was campaigning in term2. The later diagnostic measured a
984.101ms directory sync, but cannot prove the cause of the earlier failure.

Each run used three logical replicas on one physical Btrfs/NVMe host, TCP/TLS,
256 measured operations after64 warmup, one outstanding operation and8-byte
commands. Heartbeat50ms, election minimum1000ms plus spread1000ms; no timer or
durability settings were relaxed. The older full-suite process remained active
on tmpfs. There was no CPU/device isolation; these finite sequential runs do
not establish statistical superiority, sustainable capacity or a power-failure
certification. See `provenance.json` and `source-binary.sha256`.

The candidate allowed only ordinary stable-membership leader appends to start
replication at Written. Exact local Durable remained mandatory for self progress
and commitment. It retained queued/leased send charges across completion.
Four added core tests and two owner tests covered early replies, stale tickets,
held ownership, power loss, sync/publication failures and subsequent elections.
The candidate passed full affected ownership/worker/consensus suites,54 native
service tests,28 benchmark tests, both strict Clippy profiles and core-only tests.
`initial-focused.log` includes the initial bounded-control-queue harness failure;
`service-worker-benchmark.log` and `core-only.log` contain corrected runs.
`sandbox-socket-refusal.log` records a permissions-only failure before the
successful socket-enabled `membership.log`. These are candidate tests, not
claims that the removed API remains installed.

Measurements used:

```sh
/tmp/voteboat-slice183-control-benchmark target/bench-slice183-control tcp 256 1
target/release/examples/native_benchmark target/bench-slice183-candidate tcp 256 1
target/release/examples/native_benchmark target/bench-slice183-diagnostic tcp 256 1 --journal-timings
target/release/examples/native_benchmark target/bench-slice183-repeat tcp 256 1
node validation/check-native-benchmark.mjs validation/performance/slice183/control validation/performance/slice183/diagnostic validation/performance/slice183/repeat
node validation/check-serial-gate.mjs validation/performance/slice183/control
node validation/check-serial-gate.mjs validation/performance/slice183/repeat
node validation/check-journal-timings.mjs validation/performance/slice183/diagnostic.journal.csv
node validation/check-publication-timings.mjs validation/performance/slice183/diagnostic.publication.csv
```

The two serial-gate commands deliberately exit1 for the measured failed budget.
Raw receipt arithmetic and both diagnostic timing checkers pass. Reference
`storage.csv` is absent by design; the startup benchmark's own full reopen/retry
checks run before successful summary publication. No fabricated storage CSV or
replacement checker is used. The original run directories remain in `target/`.
