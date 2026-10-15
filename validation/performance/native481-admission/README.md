# 481: admission work falls; throughput and tail tradeoffs remain

The [premeasurement schema](SCHEMA.md) fixes immutable476 versus published480,
both WitnessLogIo, three rotating adjacent pairs over four cells and TCP/TLS+QUIC.
Both original campaigns terminate0: Linux1858 and Mac81015. All96 processes,
validators and readbacks pass. Root independently recomputes every original
ledger, rate, nearest-rank latency and storage delta; the independent reader
agrees across900 raw inputs. No workload is repeated or replaced.

Loaded one-group three-trial medians, baseline476 → candidate480:

| Host | Protocol | Completed originals/s | Intended p99 ms |
| --- | --- | ---: | ---: |
| Linux/Btrfs | TCP/TLS |614.909→532.740|7464.139→7317.488|
| Linux/Btrfs | QUIC |623.344→687.192|7404.386→6218.806|
| M2 Max/APFS | TCP/TLS |794.133→1065.505|6162.868→4870.050|
| M2 Max/APFS | QUIC |809.012→1177.929|6139.026→4376.000|

All six Mac loaded pairs improve completed rate and p99, with fewer refusals.
Linux loaded TCP rate ratios.866/1.735/.672 and QUIC1.547/.977/1.069 remain mixed;
the median alone is insufficient. During-horizon rates and drain are reported
separately in each host's raw-derived readout. These8s loaded histories still
have substantial window refusals and do not establish refusal-free headroom.

The intended work reduction is clear. Linux loaded whole-process CPU per completed
original falls TCP676.516→546.579µs and QUIC679.847→479.032µs, improving all six
pairs. That CPU boundary includes warmup/recovery/cleanup and refused-offer work;
it is not a measurement-only CPU profile. Linux loaded maximum whole-host poll
rounds fall from28.658–37.573ms to2.001–2.987ms. Mac loaded median maximum polls
fall TCP45.9→3.08ms and QUIC46.3→3.28ms. These rounds cover fixture work across
replicas, not one Shard visit. No allocator cause is inferred.

Material regressions remain. Mac default16 moderate QUIC p99 rises779.149→1664.171ms
in all three pairs (tail ratios2.541/1.753/2.136), with38 candidate trial1
window refusals versus zero baseline. Mac TCP skew falls754.503→738.781 completed/s;
cold p99 rises454.874→835.434ms and worsens in all three pairs. Linux QUIC skew
falls435.135→332.843/s, p996088.608→8230.328ms, cold p99940.798→2103.980ms.
Every cold cohort still applies all256 originals with zero cold refusals. Linux
singleton rates and several moderate tails also worsen; all values/trials remain
in the host readouts. Successful-original latency must be read with refusals.

Retain480 as a qualified algorithm improvement: it removes repeated exact work,
reduces observed polling cost and improves every Mac loaded pair. This is a
tradeoff, not universal native performance acceptance. The repeated default/cold
tail losses need existing-timeline review and current profiling before another
change. FileLogIo and ready16 remain defaults; no wider default is promoted.

Linux391680 offers=248327 applied+143353 window refusals; Mac391680=296506+95174.
Other refusal classes/unknown/pending are zero, every case recovers64+applied,
verifies retries and joins workers. Active maintenance records623/624 checkpoints
and1869/1872 reclaims on Linux/Mac, with one Linux skip. Bounds and finite success
do not establish a long-run memory/physical-debt plateau. Both groups lead on
replica1, all voters share one physical device per host, and the skew fixture
provides2048 pending slots per group. Aggregate rates are not one-group floors
or independent-placement/fairness certification.

Leader barrier elapsed fractions remain large and overlap host progress. Loaded
Linux TCP mean barrier27.054→34.552ms and QUIC26.422→27.070ms vary alongside CPU
reduction; barriers/original slightly increase. These observations do not prove
the cause of rate variation. Nested wall times are not additive CPU; commands
per append unit include metadata and do not prove literal proposal width.

See [Linux evidence](linux/README.md) and [Mac evidence](mac/README.md) for all
raw results, failed helpers, source/artifact/compiler/profile/device guards,
resources and paired quantities. Native opt3/debug0/assertions/overflow/default
LTO+codegen is separate from the framework profile. Each within-host comparison
uses the same compiler; Linux1.98.1 and Mac1.98.0 are disclosed. No filesystem,
CPU or kernel policy changes, browser, Pi work or security work are included.

[Root and timeline review](review/ROOT_DATA_CROSS_REVIEW.json) verifies all96 raw
records against the independent reader; [tail conclusions](review/tail-review/CONCLUSION.json)
separate service delay from dispatch and retain maintenance counterexamples.
Review payloads have their own exact seal and original byte/mode/gzip inventory.
The [next profile schema](../native482-current-profile/SCHEMA.md) precedes any new
capture or production edit. No unsupported cause is used to justify a rewrite.

Full P9 remains incomplete: all seven sustained framework cells/both hosts with
healthy margin, durable offered-load/headroom, bounded memory/debt, fairness/skew,
independent placement/shared barriers and P8 composition remain required. The
real ext4/XFS environment is still awaited.8,422,500 completed originals/s is the
batch4 framework minimum, not a ceiling or a native Counter target. No P10.
