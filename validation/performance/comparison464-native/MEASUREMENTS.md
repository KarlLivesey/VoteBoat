# Native464 OFF/ON decision: reject ready-reply joining

All twelve workload processes and validators pass, original1214 exits0 with
ALL_TIMING_END. Same published464 binary, one group/three voters, Counter8-byte,
15000 offers500/s, warm64/window64/ready16/active2smaintenance. Only ready-replies0
versus2 changes. Three rotating pairs per TCP/TLS and QUIC, fresh storage roots.
[Raw archive](linux/README.md), [independent numerical review](analysis/INDEPENDENT_FINDINGS.md)
and [machine-readable readout](analysis/INDEPENDENT_READOUT.json) preserve every
pair and loss. Root independently derives outcomes/rates/p99 from original summaries.

| Protocol | OFF → ON completed originals/s median | OFF → ON p99 ms median | OFF → ON leader barriers/completed original median |
| --- | ---: | ---: | ---: |
| TCP/TLS | 160.582 → 164.261 | 1122.767 → 1589.014 | .138676 → .129250 |
| QUIC | 152.979 → 152.658 | 1145.813 → 2462.987 | .134969 → .131161 |

TCP paired rate ratios1.001786/1.267535/.853535 and p99 ratios
1.415266/.404559/1.865714 retain one rate loss and two tail losses. QUIC ratios
.829078/1.134642/.882388 and3.124819/.804904/2.513197 retain two rate losses and
two tail losses. TCP trial1 also loses horizon completion rate despite its tiny
total-rate gain. No old-campaign medians are spliced into the comparison.

Enabled measurement joins TCP98/106/88 and QUIC80/95/93 are real. Joined
originals361/385/337 and283/396/470 count proposed positions, not completed
outcomes. All originals have exact terminal results; recovered values=64+applied,
retry verification and worker joins are true, unknown/pending0. Every refusal,
maintenance/catch-up event and whole-process resource sample remains archived.
Large window refusals preclude healthy headroom. These finite cases are neither
sustained capacity nor physical-memory/debt or fairness acceptance.

Leader barrier counts are store API calls; sync_log/publish_manifest observations
are not a complete physical fsync syscall count. Inclusive elapsed phases overlap
and are not CPU time. Modest observed barrier amortization does not establish a
consistent useful throughput/latency gain. Reject463/464 production optimization
without guard/quantum/window/maintenance tuning; restore the exact checked462
production455 baseline, preserving source/history/evidence. No Mac463/464 campaign
or full P9 performance claim follows. Original Mac455 qualification continues.
