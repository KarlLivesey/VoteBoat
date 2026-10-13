# Short paired M2 Max diagnostic

All 18 cases passed exact results and immutable source/artifact guards. Both
VoteBoat versions verified 131,072 originals, 131,072 or 32,768 requests and all
three frontiers at 132,097. Candidate histograms include 132,096 originals with
warmup. Workload, default maintenance, one client/16 server workers, three voters, volatile
stores and in-process transport remained unchanged. Untouched upstream defaults
and all original result checks were retained.

| Case | Baseline ops/sec | Candidate ops/sec | Untouched ops/sec |
| --- | ---: | ---: | ---: |
| 1 client, batch 1 | 54,897.19 | 54,231.32 | 10,233.22 |
| 4096 clients, batch 4 | 974,773.92 | 2,101,726.77 | 1,915,907.18 |

Medians over three interleaved trials show roughly −1.2% singleton throughput and
2.16x batch 4 throughput. Median request p99 moves 32.042→32.667 us and
19.211→8.457 ms. These short diagnostics leave sustained qualification pending.
The batch 4 candidate is only about 25% of the 8,422,500/s published-floor margin
requirement; the full curve and matched sustained maximum remain pending.

Actual candidate widths 2492/4096/4096 were observed. Measured original writes
formed 371/349/309 Core proposal batches. Each replica checkpointed 24–25 times;
0–3 snapshot installations occurred per trial and final retained suffixes were
at most 4,072. All original IDs/results were checked, including partial committed
chunks; nothing was counted as four originals merely because it was one request.

Raw commands, JSON, stderr, exits, elapsed/user/system timers and validation are in
mac-paired131072. RSS and untouched provider's private queue/maintenance/frontier
gauges are unavailable. Remote parsing preserved exact results while Linux native
timing was active; collection occurred only after that quiet window ended.
