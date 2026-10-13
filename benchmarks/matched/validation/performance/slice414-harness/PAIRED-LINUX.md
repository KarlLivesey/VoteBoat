# Short paired Linux diagnostic

All 18 baseline/candidate/untouched cases exited zero and passed exact validation
and frozen source/artifact guards. Workload: 131,072 measured originals, 1,024
singleton warmups, one client worker, 16 server workers, three voters, volatile
stores and in-process transport, primary default maintenance. Source is isolated
from subsequent production edits. Candidate release is exact306b26b; a separate
test-only fixture correction is not part of this measured executable.

| Case | Baseline ops/sec | Candidate ops/sec | Change | Untouched ops/sec |
| --- | ---: | ---: | ---: | ---: |
| 1 client, batch 1 | 102,516.80 | 110,348.89 | +7.64% | 22,739.66 |
| 4096 clients, batch 4 | 752,128.05 | 845,112.42 | +12.36% | 3,300,898.21 |

These are medians across three interleaved trials. Request p99 moved from
19.086 to 18.134 us for singleton writes and 26.842 to 24.087 ms for batch 4.
The candidate remains well below the high-concurrency reference. This short run
establishes neither sustained capacity nor satisfaction of the required minimum.
The requested healthy margin is at least1.5x the larger published/matched sustained
floor per cell. For4096 clients/batch4 the published-floor requirement alone is
8,422,500 writes/sec; this short candidate result is approximately10% of that
threshold. Sustained matched rates and the complete curve remain unqualified.

Every unified case verified 131,072 originals, the correct 131,072 or 32,768
request count and all three applied frontiers at 132,097. Candidate reports
4096/4096/1024 effective API/append/replication capacities; baseline reports64/64/64.
Upstream settings were untouched. The candidate's exact original-width histogram
accounts for 132,096 originals including warmup and reached4096. The measured
batch4 work used175–199 Core proposal batches, instead of counting each four-entry
client request as one original. Leader persistence units were2358–2400.

All candidate replicas checkpointed25 times, final retained suffixes were at most
5096 and the lagging follower installed16–18 snapshots in batch4 cases. Wider
batching is active; the remaining throughput cost is not established by these
counters alone. The untouched minimal provider does not expose private queues,
retention/checkpoint gauges or final follower frontiers; the unified wrappers
verify their own complete prefixes separately.

Raw evidence is under `linux-paired131072`; build/compiler/source provenance under
`artifacts-linux`. Bash builtin elapsed/user/system timing was used; RSS is
unavailable. Before/after guards use immutable exports and verified executable
hashes, not the changing repository working tree. No executable/checkouts are
included in this evidence archive.
