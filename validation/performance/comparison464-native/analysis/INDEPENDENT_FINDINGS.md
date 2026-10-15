# Independent Linux464 native readout

All12 workloads and validators exited0. Same published464 ELF, producerOFF0 versus ON2;15,000 offers at500/s, one group/three voters, window64/ready16,64 warmups and active2s maintenance. Rates below count applied originals; p99 is offer-to-completion latency. This is a finite durable diagnostic, not sustained or framework acceptance.

| Protocol | OFF→ON median completed/s | OFF→ON median horizon completed/s | OFF→ON median p99 ms | OFF→ON median leader barriers/original | OFF→ON median commands/publication |
|---|---:|---:|---:|---:|---:|
| tcp | 160.582→164.261 | 159.500→163.500 | 1122.767→1589.014 | 0.138676→0.129250 | 7.211063→7.736940 |
| quic | 152.979→152.658 | 152.033→151.100 | 1145.813→2462.987 | 0.134969→0.131161 | 7.409091→7.624194 |

| Protocol/trial | Applied OFF/ON | completed/s OFF→ON | p99 ms OFF→ON | rate ON/OFF | p99 ON/OFF | Refused OFF/ON | ON joins transitions/replies/proposed originals | Barrier/original ON/OFF |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| tcp/1 | 4954/4943 | 163.968→164.261 | 1122.767→1589.014 | 1.001786 | 1.415266 | 10046/10057 | 98/116/361 | 0.916154 |
| tcp/2 | 4121/5239 | 136.490→173.006 | 2670.104→1080.213 | 1.267535 | 0.404559 | 10879/9761 | 106/112/385 | 0.931357 |
| tcp/3 | 4849/4147 | 160.582→137.062 | 1058.381→1974.637 | 0.853535 | 1.865714 | 10151/10853 | 88/96/337 | 0.936821 |
| quic/1 | 4625/3853 | 152.979→126.832 | 1145.813→3580.458 | 0.829078 | 3.124819 | 10375/11147 | 80/97/283 | 1.001269 |
| quic/2 | 4159/4727 | 138.104→156.699 | 1876.928→1510.747 | 1.134642 | 0.804904 | 10841/10273 | 95/109/396 | 0.918351 |
| quic/3 | 5216/4597 | 173.005→152.658 | 980.022→2462.987 | 0.882388 | 2.513197 | 9784/10403 | 93/108/470 | 0.944470 |

Throughput is mixed: TCP trial3 and QUIC trials1/3 lose. P99 loses in TCP1/3 and QUIC1/3, with QUIC1 at3.125× OFF. TCP horizon rate also loses slightly in trial1 despite its small elapsed-total rate gain. Leader barriers/original improve in five pairs; QUIC1 regresses slightly. Actual joins occur, but do not establish their causal contribution to rates or tails.

All applied/refused outcome totals equal15,000; unknown/pending/no-leader/admission-refused/not-proposed are zero. Recovery value equals64+applied, every retry/worker flag is true, and maintenance CSV counts/reclaimed-byte sums match summaries. Every disabled warmup/measurement join count is zero. Full per-case horizon/drain, all three replica storage deltas, maintenance, recovery/retry and whole-process RSS values are in READOUT.json.

Leader logical barriers, log-sync calls and manifest-publication calls happen to have equal measurement deltas in these runs. They include maintenance/drain. Manifest publication includes nested filesystem operations: these are API calls, not a full fsync syscall count. Attribution is inclusive wall time, not CPU; do not sum overlapping phases. RSS is whole-process peak including recovery/cleanup, not a continuous memory/debt plateau. No comparisons use earlier-campaign medians.
