# 487 results: less loaded CPU work, unresolved tails

All96 original cases, validators and physical readbacks pass. Original Linux44616
actually terminates0/8d63f2; Mac67726 terminates0/148477, including postguards.
Root records both END before any collection/readout. No workload is replaced or
repeated. [The schema](../SCHEMA.md), immutable480 versus486 sources and fixed48-case
rotations remain unchanged. These are native8-byteCounter/WitnessLogIo, three
durable voters sharing one SSD per host; not framework floor measurements.

Linux Ryzen7 9800X3D/Btrfs uses stable1.98.1/protoc3.9.0; M2Max/APFS uses
stable1.98.0/protoc3.21.12 on AC power. Each arm within a host uses the same
compiler/profile/fixture/device identities and configured worker topology. No
threads/workers/cores are added. Actual affinity/core isolation is not imposed.
No builds/checks/copies/archive/profile compete with timing; kernel/CPU/filesystem
settings remain unchanged. Baseline f7b0ef1…; candidate dcdcafe3… .

Below are separate medians of three trials. Native completed throughput includes
the original horizon+drain and ends before latency sorting/recovery/cleanup. CPU
is whole-child user+system, including startup/warmup/recovery/cleanup, divided by
measured completed originals. Average busy cores divides that CPU by the SAME
whole-child wall time, not the shorter native timer. It is utilization, not an
assigned physical-core count. [Compact numeric data](CPU_WORK_SUMMARY.json) retains
the individual CPU/cores/whole-child-rate trials and exact boundary distinction.

| Host | Cell | Protocol | Completed/s480→486 | p99ms480→486 | CPUµs/completed480→486 | Average cores480→486 |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| linux | singleton | tcp | 5.562→5.531 | 779.728→754.867 | 19,849.747→21,573.335 | 0.0966→0.0993 |
| linux | singleton | quic | 5.937→6.000 | 705.812→638.553 | 19,490.068→18,986.572 | 0.0954→0.0952 |
| linux | moderate | tcp | 124.974→122.687 | 3,046.693→3,418.042 | 944.704→958.682 | 0.1136→0.1149 |
| linux | moderate | quic | 125.590→127.476 | 2,889.078→1,759.514 | 970.304→954.887 | 0.1191→0.1180 |
| linux | loaded | tcp | 629.570→753.827 | 7,052.436→5,949.939 | 479.211→280.340 | 0.2700→0.1778 |
| linux | loaded | quic | 702.331→810.306 | 6,297.598→5,489.118 | 483.184→258.221 | 0.2662→0.1795 |
| linux | skew | tcp | 398.857→402.549 | 6,824.550→6,567.673 | 529.754→442.116 | 0.1758→0.1474 |
| linux | skew | quic | 443.638→371.598 | 6,249.932→6,392.711 | 521.031→481.811 | 0.1757→0.1475 |
| mac | singleton | tcp | 7.406→7.375 | 161.366→167.152 | 14,653.364→14,727.823 | 0.0977→0.0984 |
| mac | singleton | quic | 7.375→7.406 | 165.098→166.158 | 21,282.589→21,546.464 | 0.1395→0.1424 |
| mac | moderate | tcp | 126.669→127.858 | 772.463→760.857 | 1,035.263→1,032.512 | 0.1289→0.1297 |
| mac | moderate | quic | 127.844→127.677 | 833.333→806.001 | 1,465.797→1,474.808 | 0.1822→0.1829 |
| mac | loaded | tcp | 1,070.849→1,297.300 | 4,854.187→3,455.130 | 586.680→282.446 | 0.5336→0.3080 |
| mac | loaded | quic | 1,021.043→1,332.900 | 4,753.135→3,357.463 | 618.483→318.240 | 0.5105→0.3427 |
| mac | skew | tcp | 773.221→776.634 | 3,127.944→3,247.519 | 487.169→328.100 | 0.3236→0.2231 |
| mac | skew | quic | 785.173→740.803 | 3,119.642→3,201.881 | 540.828→397.371 | 0.3495→0.2426 |

Decision: retain486 as a checked direct reduction in loaded Counter CPU work. All
12 loaded CPU/original pairs improve and matching-wall average busy cores fall;
11/12 native throughput and11/12 intended-p99 pairs improve. It does not pass
universal native latency/fairness/default or full P9 acceptance. Linux loses
10/24 overall rate pairs and8/24 p99 pairs, including moderateTCP and skewQUIC.
Mac singleton CPU worsens slightly in all six pairs; moderateQUIC adds15 window
refusals. Mac skewTCP p99 worsens in all three pairs and skewQUIC in two. Cold
p99 median460.326→809.464ms TCP and480.639→884.601ms QUIC each lose two of three
pairs, despite every256 cold originals applying without refusal. Linux cold
TCP1298.238→1287.555ms andQUIC1256.863→1201.789ms improve in median; individual
losses remain. Do not count rate gains accompanied by greater CPU/original or
greater average cores as efficiency wins:11 such native-rate pairs are explicitly
flagged in the independent core decomposition. No causal claim follows from
whole-child correlations or nested elapsed storage phases.

Linux253450applied+138230window-refused and Mac323608+68072 account for all391680
offers per host; every other refusal class, unknown and pending is zero. Each
valid history retains exact recovered values, original retries/joins, active
maintenance and physical storage/publication counters. Linux has624 checkpoints/
1872reclaims/0skips. Whole-child RSS is not a continuous memory/debt plateau.

The [Linux](linux/README.md) and [Mac](mac/README.md) archives preserve every raw
case/command/exit/outcome/latency/refusal/resource/storage/maintenance/readback.
Independent recomputation agrees with all96 frozen readouts, all nearest-rank
quantiles and same-child CPU identities; [review evidence](review/) retains the
full loss sets, initial analysis parser errors and focused corrections. No raw
error is hidden and no build/workload/check suite is rerun for an analysis fix.
Root verifies complete recursive seals, original SHA/length/mode and lossless
gzip roundtrips. No binaries/build/source trees/generated WAL are archived.

Paused at the user's request after committing/pushing this wrap-up. Singleton
framework CPU profiling is deferred until explicitly resumed; no next task is
started. When resumed, preserve fixed workers and require reduced CPU per useful
completed original, reporting average cores and latency/refusals alongside rates.
All seven sustained framework healthy-margin cells on both hosts, native
headroom/fairness/skew, bounded memory/debt, independent placement/shared barriers
and P8 composition remain required. Real ext4/XFS is pending.8,422,500 is the
framework minimum, not a stopping ceiling; P9 is incomplete.
