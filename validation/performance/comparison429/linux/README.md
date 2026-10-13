# Linux428/429 paired diagnostic

Original70128 terminates0/DONE, all24 exact cases. Three rotating trials/cell use
1,048,576 originals plus1024 warmups, singleton1/batch1 or bulk4096/batch4:
published428 OFF, published429 OFF,429 ON and pinned OpenRaft. No case restarted.
Source/compiler/artifact/profile/DWP/driver guards pass before/after. Root holds
the quiet host window through actual terminal; Mac qualification stays separate.

| Cell | 428 OFF PUTS/s | 429 OFF PUTS/s | 429 ON PUTS/s | OpenRaft PUTS/s |
| --- | ---: | ---: | ---: | ---: |
| Bulk | 2,137,517 | 4,725,315 | 4,520,710 | 3,188,183 |
| Singleton | 97,612 | 101,802 | 84,485 | 18,467 |

Request p99 medians in that order: bulk9.251/5.095/5.499/7.805ms; singleton
0.019226/0.019717/0.020869/0.138270ms. All OFF bulk pairs improve
2.3466/2.2098/2.1959. Singleton OFF ratios1.0211/1.1447/0.9591 retain trial3's
regression. ON regresses against candidate OFF in every bulk/singleton pair and
against baseline OFF in every singleton pair. All per-trial rates/p99/ratios,
regressions/tails and resource timers remain in linux-results/PAIRED.json,
SUMMARY.json, RESULTS.json and original raw outputs.

Leader bulk protected verification is1.677/1.661/1.702ms inclusive within whole
compaction11.015/10.116/11.226ms. Reconcile0.133/0.131/0.130ms; rawEq/clone
compact calls remain zero. Host admission90.585–97.101ms and result handling
66.599–70.657ms are now larger observed elapsed scopes. These may overlap/nest
and are not CPU shares, allocator causes or an additive total. Logical equal-
retained-entry units do not count actual compared/skipped work. The remaining
CPU bottleneck must be profiled before choosing a further optimization.

CONFIG_PAIR matches publisheda46da2b428 baseline (exe85f997d4...) with
published832479d429 (exe2ddcf9ec...), within-host compiler/locks/profile and
unchanged per-original channels, timer/cleanup boundaries and3600/30 guards.
Both preserve PROFILErelease reference-oracle evidence and ordinary assertions/
overflow, fresh root/executable/upstream records, original/copied DWP hashes.
Source inventories1424/1425 differ by one added independent equality test module.
Observer29 phases/four scopes/15 methods stays unchanged. Prepublication guard
refusals/checker controls are retained separately, not relabeled429 qualification.
Actual429 three fixtures plus ten checker negatives and eight artifact negatives
pass before preparing the usable pair.

This regular-file archive keeps all raw reports/stderr/exits/timers/terminal,
source/artifact/compiler identity metadata, config/unchanged drivers and every
preparation error/refusal. No binaries/source/build trees are archived. Hashes
cover the preserved files; data is readable without browser/UI/SVG.

Bulk exceeds this matched upstream short median, but remains below5,615,000
published originals/s and8,422,500 minimum healthy floor. No short run qualifies
sustained20M/full seven-cell/both-host acceptance, real quorum-durable application
capacity, fairness/skew/headroom/placement/shared barriers or P8 compositions.
P9 remains active and incomplete; Windows/P10 stay excluded.
