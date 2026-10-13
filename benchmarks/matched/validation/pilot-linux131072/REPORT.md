# Linux short primary diagnostic

All 18 corrected cases exited zero. Each unified case completed 131,072 original
operations, with 131,072 singleton requests or 32,768 four-entry requests. All
three final applied frontiers were 132,097. The untouched benchmark reported the
same operation and request-sample counts. Source and executable hashes passed
before and after the campaign.

These are medians across three interleaved trials on one Linux host. This short
diagnostic does not establish sustained capacity or satisfaction of the required
OpenRaft minimum. Both engines use volatile stores and in-process transport.

| Case | Engine | Original operations/sec | Request p99 |
| --- | --- | ---: | ---: |
| 1 client, batch 1 | VoteBoat | 115,486.85 | 16.391 us |
| 1 client, batch 1 | Unified OpenRaft | 23,046.25 | 84.809 us |
| 1 client, batch 1 | Untouched upstream | 22,818.04 | 85.641 us |
| 4096 clients, batch 4 | VoteBoat | 731,406.84 | 28.109 ms |
| 4096 clients, batch 4 | Unified OpenRaft | 2,989,002.95 | 6.640 ms |
| 4096 clients, batch 4 | Untouched upstream | 3,170,394.12 | 6.155 ms |

VoteBoat is about five times faster for the singleton case and about four times
slower at high concurrency in this diagnostic. The latter is the immediate
limiting case; the full seven-cell curve and longer qualification remain pending.

VoteBoat checkpointed every replica 25–26 times. Final retained suffix lengths
were at most 5,672. Singleton replicas had no snapshot installations and peaked
at 6,000 retained entries. For batch 4, the leader peaked at 22,376 retained
entries and 3,666–3,830 queued client requests; one follower installed 12–16
snapshots per trial, while follower retained peaks were 6,120. Those observations
identify costs worth isolating, without proving their individual contribution.

The primary upstream configuration was preserved: API and append batches 4096,
replication payload 1024, snapshot threshold 5000, retention 1000 and purge
threshold 1024. VoteBoat currently has 64-entry API, append and replication
capacities. The public application snapshot models also differ as documented in
the harness README. No competitor limits were reduced for this comparison.

The pinned minimal OpenRaft provider does not expose retained-log, checkpoint or
internal queue gauges. Its untouched driver also does not expose final follower
frontiers; those are verified separately by the unified wrapper. They remain
unavailable in the untouched report, rather than inferred.

The initial runner failed all 18 launches with exit 127 because `/usr/bin/time`
was absent; no benchmark process ran. Those errors are retained in
`linux-primary-131072`. The corrected run in
`linux-primary-131072-builtin` uses Bash builtin elapsed/user/system timing, with
RSS unavailable. Raw commands, outputs, exits, timers, guards and per-case exact
validation are retained there.
