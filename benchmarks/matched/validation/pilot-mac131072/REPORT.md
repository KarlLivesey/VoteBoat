# M2 Max short primary diagnostic

All 18 interleaved cases exited zero using the previously qualified asserted
release binaries. Source and artifact hashes matched before and after. Each
unified case completed 131,072 original operations, with 131,072 singleton
requests or 32,768 four-entry requests, and all three applied frontiers were
132,097. The untouched upstream benchmark reported the same operation and
request-sample counts. Every original result was checked by the unified wrapper.

These are medians across three trials. This short diagnostic does not establish
sustained capacity or satisfaction of the required OpenRaft minimum. Both engines
use volatile stores and in-process transport, with one client runtime worker,
16 server workers, three voters and 1,024 singleton warmups.

| Case | Engine | Original operations/sec | Request p99 |
| --- | --- | ---: | ---: |
| 1 client, batch 1 | VoteBoat | 61,950.86 | 31.417 us |
| 1 client, batch 1 | Unified OpenRaft | 10,936.42 | 126.834 us |
| 1 client, batch 1 | Untouched upstream | 10,790.69 | 128.125 us |
| 4096 clients, batch 4 | VoteBoat | 1,227,168.73 | 15.370 ms |
| 4096 clients, batch 4 | Unified OpenRaft | 2,185,512.26 | 8.544 ms |
| 4096 clients, batch 4 | Untouched upstream | 2,176,841.83 | 8.135 ms |

The high-concurrency VoteBoat rate is approximately 56% of OpenRaft's rate. No
VoteBoat replica installed a snapshot in any Mac case, so the concurrency gap
persists without the snapshot churn observed in the Linux diagnostic. This does
not isolate the contribution of each remaining cost.

Every VoteBoat replica checkpointed 26 times. Final retained suffixes were at
most 3,097. Singleton retained peaks were 6,000. Batch leader retained peaks were
9,276–22,376 and client queue peaks were 3,752–3,840; follower retained peaks were
at most 6,120. VoteBoat's 64-entry capacities remain below upstream's primary
4096-entry API/append and 1024-entry replication capacities. Snapshot model and
timer differences remain documented in the harness README.

Upstream defaults were preserved: snapshot threshold 5000, retention 1000 and
purge threshold 1024. The minimal OpenRaft provider does not expose retained-log,
checkpoint or internal queue gauges. The untouched driver does not expose final
follower frontiers; the unified wrapper verifies them separately. These gauges
remain unavailable in the untouched report.

Raw commands, outputs, exits, source/artifact guards, exact validation and Bash
builtin elapsed/user/system timing are retained in `raw`. RSS is unavailable.
The runner is preserved alongside the report. The Mac was quiet during timing;
result transfer and parsing began only after all 18 cases terminated.
