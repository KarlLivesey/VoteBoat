# CPU3 measurement and final disposition

**Reject [PR27](https://github.com/KarlLivesey/VoteBoat/pull/27) as implemented; close without merge.** Exact candidate `6c7cb6d7e535a79265b41ee8e51679616bb1b85d`, baseline `0bf9271ecb5a34c4089c6bb10381303232624170`. Candidate base `c60e6ba4` is production-identical to baseline. Main receives only this record; the caller-owned application receipt APIs/Counter/matched consumer changes are not integrated.

All nine original driver pairs met their functional and per-width CPU gates. The subsequent64 canonical children/32pairs met functional/input checks, but recorded singleton CPU and rate medians worsened, and bulk1 p99 rose. These observations do not meet the unchanged whole-path keep gate. An unresolved post-campaign resource reporting assertion is a separate qualification hold. No new tolerance, replacement measurement, causal explanation, source repair or full-suite waiver follows. Rejection is an unmet acceptance decision, not a demonstrated consensus defect or proof of the cause of small timing changes.

## Recorded paired observations

Three AB/BA/AB driver pairs per width; one server worker,1024warmup rounds/default retention/original workload specs. Whole-child user+system CPU includes startup/warmup/application/validation/diagnostic exit, divided by actual measured originals.

| Driver width | CPU/original | Internal elapsed/original | CPU wins |
| --- | ---: | ---: | ---: |
| 1 | -2.090074% | -2.017518% | 3/3 |
| 231 | -3.859106% | -4.611516% | 3/3 |
| 515 | -2.752117% | -3.102398% | 2/3 |

Every width met median paired CPU B/A<1 and at least2/3wins. Actual measured originals200000/1386000/1545000; all18 children checked original IDs/replies, all3 actual core/application/published prefixes, no fencing/error, default checkpoint/retention/settings and adapter conjuncts. Expected final prefixes201025/1622545/2072361 were compared with observations, never assigned into telemetry. The synchronous diagnostic exits before ordinary owned shutdown and is not canonical qualification.

Only after that gate, the fixed canonical64 campaign ran: eight alternating pairs per cell, actual3997696 bulk and131072 singleton originals,4096clients/batch4 or1client/batch1, equal16or1workers per pair,1024warmup/default retention/upstream-default envelopes/attributionfalse. No sustained-profile long counts were used. Medians below are medians of paired B/A changes.

| Cell | CPU/original | Rate | Request p99 | CPU wins |
| --- | ---: | ---: | ---: | ---: |
| bulk16 | -1.303419% | +0.907003% | -0.705444% | 7/8 |
| single16 | +0.487895% | -0.632392% | +0.540550% | 3/8 |
| bulk1 | -1.841091% | +2.538958% | +1.146322% | 7/8 |
| single1 | +0.497490% | -1.120697% | -0.835000% | 2/8 |

Singleton CPU was higher5/8 and6/8 pairs, and singleton rate lower6/8 and7/8. Bulk1 p99 was higher5/8; its last paired p99 ratio1.163632 is retained. No cross-cell averaging offsets these costs. This table is recorded campaign evidence with the resource hold below, not a fully qualified performance claim.

All canonical original IDs awaited/matched, per-client/sample counts checked, all3 actual application prefixes equaled completed+1025, real checkpoints and normal owned shutdown completed before JSON/exit0. Attributionfalse exposes no exact live commit-counter field; the boundary is real applied-prefix plus original completion contract and owned exit, not invented commit telemetry. Whole-child CPU includes warmup/verification/aggregation/serialization/shutdown; reported request rate excludes some of those phases. No server-only instruction/allocation counter or profiler was used. [All41 pair records and identities](evidence/REPORT.md), [recomputable summary](evidence/summary.json).

## Resource qualification remains on hold

After the complete campaign, hash-verified collection and Mac cleanup, the independent reporting auditor failed its final assertion that every current-process R4 observer task record was conforming. The original transient rows were not saved before assertion, so the offending PID/thread and cause are unproven. A later conforming snapshot does not erase this failure. [Original failure receipt](evidence/audit-resource-failure.json) and [exact reporting auditor](evidence/audit-resource-failure-original.py) are preserved.

All2218 recorded campaign Linux task observations conform to absolute nice19/cores8-9 with zero recorded campaign violations; this does not supply overall resource clearance or explain the reporter failure. No dependent build/measurement launched after the reporting failure. The resource hold survives this rejection record; no unchanged timing rerun or automatic resource repair is authorized for this candidate. Future observers must persist offending rows before asserting, rather than losing the evidence.

## Source, feature and artifact boundary

Both independent exact-head Sol source reviews approve without P1/P2/materialP3: [A5623470249](https://github.com/KarlLivesey/VoteBoat/pull/27#pullrequestreview-5623470249), [B5623470369](https://github.com/KarlLivesey/VoteBoat/pull/27#pullrequestreview-5623470369). Each verified all1535 frozen inputs. Builder325 grouped focused passes/zero ignored, six compiled direct behavioral mutants caught/restored, final strict7 and actual enabled nonforce push-hook7 passed. These are separate source/focused receipts, not full5 suite totals. The committed candidate's earlier pending-publication wording is historical; actual publication succeeded. All five complete configurations/exhaustive debug relay/docs were not launched because the keep gate was unmet.

The candidate preserves every actual application and receipt, exact original ordering, Counter staged state/errors/dedup, owned provider fallback, Actor FIFO/partial delivery, bounded retained capacity and snapshots. Core/quorum/native/logical/wire/dependencies/hooks stayed unchanged; the semantic matrix remains on the rejected branch. Public Counter behavior was tested, not performance-measured by this Noop campaign. No feature was deleted for a score; no full feature certification follows.

Frozen binary SHA256 identities:

- driverA `c05c3c718ab6d9ac5a72f2e04cfce29fb35f6618a76b3e0debdda10ff7f5fa6a`;
- driverB `ec93670d1a45104fec07ce382455613619e55dc9198ccb918ccb77e3752d545d`;
- canonicalA `0c6998bc63614f43faaba36dfff64f9291b7a14336853f4d901c4068c2c69208`;
- canonicalB `228bee4627a06cdb000e28c939de60c70d5f9d7c2d3976eb3b1ac622b9ab1727`.

A artifacts reused after source/overlay/toolchain/hardware/flags verification; all runtime observations fresh. B driver uses exact qualified R3 two-file overlay; both files restored to exact6c, all804 canonical input paths/modes verified, canonicalB rebuilt in the same normal target before use. No driver/canonical substitution. Mac14,5 AppleM2Max/Ruststable1.98/Cargo1.98, original profile/flags/defaults. Full examples/include closure transferred upfront; no giant history/source duplication or missing-example rerun.

Original preparatory quoting/metadata failures and dependent preparation after a missing set-e are retained, with no measured replacement or production repair. Earlier builder/controller/fixture and source-review resource gaps remain disclosed. No whole-task/harness containment claim or scheduler/parking explanation is made.

## Cleanup and next scope

All788 collected raw files hash-verified before pruning the two marked Mac roots, normal target, scripts/binaries and empty owned parent. Final Mac root/job and current-user readable Linux cwd checks found no known prior owned workload jobs. Full unchanged stock guard with actual Mac capacity read passed. Unknown/shared infrastructure, foreign/inaccessible process visibility and deleted-cwd CUA hosts remain untouched; no global zero-process claim.

Compact immutable receipts here carry SHA256SUMS. Complete raw/source/build/overlay/binary/context/failed-reporting evidence remains in `target/claude/pillar1-20261022/cpu3-perf/`; [runner's durable comment](https://github.com/KarlLivesey/VoteBoat/pull/27#issuecomment-6356816510). Rejected candidate branch retained, no source merge.

Next work remains exclusively pillar1. Profile the unchanged baseline's actual asynchronous in-process embedding, separately from the synchronous driver, to locate a larger production CPU class before another implementation. This does not explain CPU3's timing differences, revive its PR, alter qualification workloads or authorize benchmark-collector-only optimizations. Network/storage/roadmap and parked candidates remain paused.
