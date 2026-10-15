# 484 paired native results: reject483

Fixed480→483 comparison, not framework acceptance. All48 processes, validators
and physical readbacks on each host pass. Original Linux84364 ends0/52f183;
Mac2749 ends0/1812fd. Both actually terminate before collection/readout.
The [schema](../SCHEMA.md) retains three adjacent rotating trials of singleton,
moderate, loaded one-group and two-group31:1 skew, over TCP/TLS and QUIC.
Each host receives391680 offers with64 original warmups per case, active2s
maintenance, original120s drain and unchanged per-original outcomes/timers.

Baseline source f7b0ef12709bb7e7aad4f0279851aa035b508e9e; candidate source
efd383dc36b4b9edf8f66021b90115824440b468. Native8-byteCounter/WitnessLogIo,
three voters sharing one physical SSD per host. Linux Ryzen7 9800X3D/Btrfs uses
stable1.98.1/protoc3.9.0; Mac M2Max/APFS uses stable1.98.0/protoc3.21.12.
Within-host compiler/profile/source/artifact/fixture guards are exact. Mac is on
AC power. No kernel/CPU/filesystem settings changed; Linux policies stay2/2/516.
No same-host build/check/copy/archive/profile competes with timing.

Median completed originals/s and intended-arrival p99 milliseconds below are
computed separately over the three trials, not ratios of pooled rates. Every
individual pair, refusal, tail and raw ledger is retained in [Linux](linux/README.md),
[Mac](mac/README.md) and [independent review](review/).

| Host | Cell | Protocol | Completed/s480→483 | p99ms480→483 |
| --- | --- | --- | ---: | ---: |
| linux | singleton | tcp | 6.339→5.750 | 426.399→712.197 |
| linux | singleton | quic | 6.375→5.844 | 480.930→618.535 |
| linux | moderate | tcp | 127.001→123.067 | 1,999.864→4,182.107 |
| linux | moderate | quic | 126.740→123.570 | 2,112.988→3,272.071 |
| linux | loaded | tcp | 807.092→629.374 | 5,763.106→8,149.859 |
| linux | loaded | quic | 765.733→764.098 | 6,125.799→5,747.768 |
| linux | skew | tcp | 359.715→404.957 | 7,475.871→6,741.002 |
| linux | skew | quic | 422.705→429.684 | 6,201.337→5,980.634 |
| mac | singleton | tcp | 7.406→7.406 | 165.053→160.915 |
| mac | singleton | quic | 7.406→7.375 | 163.256→162.872 |
| mac | moderate | tcp | 127.832→127.845 | 1,306.882→830.698 |
| mac | moderate | quic | 127.823→127.873 | 775.851→811.113 |
| mac | loaded | tcp | 1,078.570→1,342.682 | 4,709.463→3,067.395 |
| mac | loaded | quic | 1,110.755→1,499.466 | 4,625.423→2,676.933 |
| mac | skew | tcp | 797.170→738.905 | 3,109.447→3,438.682 |
| mac | skew | quic | 785.004→732.471 | 3,099.940→3,304.539 |

Linux loses17/24 completed-rate pairs and16/24 p99 pairs. Loaded whole-child
CPUµs/completed original worsens TCP439.932→480.890 and QUIC421.666→458.409;
this numerator includes startup/warmup/recovery/cleanup beyond the native timer.
Mac loaded improves all six rate and p99 pairs, with CPU535.634→459.354 TCP
and586.308→469.940 QUIC. These Mac gains remain evidence despite rejection.
Mac loses11/24 rate and11/24 p99 pairs overall; TCP skew loses all three rates.

Every skew case applies all256 cold originals with zero cold refusals. Mac cold
p99 worsens in all six pairs: TCP median383.173→854.009ms, QUIC411.649→809.077ms.
Linux cold TCP1777.798→1512.052ms improves, while QUIC1064.944→1264.328ms worsens.
No universal fairness improvement follows from aggregate throughput.
Linux263904 applied+127776 window-refused; Mac330860 applied+60820 window-refused.
Every other refusal/outcome class, unknown and pending is zero. Substantial window
refusals preclude healthy offered-load headroom. Successful cases verify exact
recovered values, original retries/joins and active maintenance. Storage elapsed
contexts remain nested/overlapping, not additive CPU or a causal tail explanation.
Whole-child RSS is not a memory plateau or physical-debt proof.

The frozen reader and independent raw recomputation agree on all96 cases, all
nearest-rank intended/service/dispatch/group quantiles, counts, native boundaries,
CPU/RSS and physical-counter deltas. Original display-only KeyError and independent
parser delimiter failure are retained alongside their focused corrections; no
workload is rerun. Recursive archive seals retain original hashes/bytes/modes and
lossless gzip representations; no binaries/build/source trees/generated WAL.

Decision: reject483 and [restore exact480 in485](../../native485-restore/SCHEMA.md).
A platform-specific loaded gain does not justify the Linux and cold-tail losses.
No allocator, compiler/code-layout, filesystem or individual slow-tail cause is
established. Do not retune the ingress scope or replace unfavorable trials.

Full P9 remains incomplete: sustained one-group framework margins on both hosts,
durable headroom/fairness/skew, bounded memory/debt, independent placement/shared
barriers and P8 composition remain. Real ext4/XFS is pending. FileLogIo/ready16
remain defaults;8,422,500 is the framework minimum, not a stopping ceiling.
