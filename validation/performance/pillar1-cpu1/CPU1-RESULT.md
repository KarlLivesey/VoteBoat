# CPU1 final measurement and disposition

**Reject PR25 as implemented; close without merging.** Source head `7799b4f87770a5bff9234ef2ac27c6df9fb06da8`; measured release source `eb32cbef502dbee666b7d0487c2eca1ad1ad34db`; baseline `0bf9271ecb5a34c4089c6bb10381303232624170`. Final-head source approvals A5622212887 and B5622201025 verify the three-path followup is docs plus cfg(test), with no runtime/matched/build/hook delta. This carries release-source equivalence, not a new-build or byte-identical-artifact claim.

The candidate saves singleton CPU but does not meet the CPU/rate goal across workloads. No new numeric guard tolerance or parking explanation is adopted. All 64 canonical children passed; rejection is performance-based, not a demonstrated consensus defect. Full repository suites were not launched because the performance gate failed. No CPU1 production is integrated.

## Canonical default-retention comparison

Mac14,5 Apple M2 Max / 12 CPUs, Rust stable 1.98, pinned equal build flags, three real in-memory replicas, same equal worker counts per pair; eight alternating pairs per cell. Whole-child user+system CPU divided by completed originals; requested4000000 bulk rounds to3997696, singleton131072. All exact original replies/application prefixes and canonical owned-shutdown paths succeeded. These are medians of paired B/A changes, not ratios of marginal medians.

| Cell | CPU/original | Rate | Request p99 | CPU wins |
| --- | ---: | ---: | ---: | ---: |
| bulk16 | -0.076% | -0.070% | +1.121% | 6/8 |
| single16 | -5.933% | -1.111% | -2.047% | 8/8 |
| bulk1 | +0.605% | -0.882% | -0.253% | 3/8 |
| single1 | +0.001% | -3.522% | -5.209% | 4/8 |

Singleton rate was lower 7/8 and 8/8 pairs. The canonical results are immutable; a successful later diagnostic does not erase these costs. No server-only allocation/instruction reduction was measured or claimed.

## Fixed-schedule diagnostic

R1 failed its opaque frontier guard. Source found its synchronous adapter bypassed run_inner's publication of shared telemetry. R2 stopped before build at a false diskguard accounting failure. Parent corrected only empty-input scratch accounting, preserving every cap/shared-port exclusion/real Mac check. R3 built the identical adapter for both sources; one omitted-publication control failed telemetry ONLY while all actual reply/commit/application/fence guards passed; one positiveA passed. Its observer then raced a vanished /proc task beforeB. All these failures remain preserved, not retroactively green.

R4 reused the exact frozen R3 binaries (no source copy or rebuild): A SHA256 c05c3c718ab6d9ac5a72f2e04cfce29fb35f6618a76b3e0debdda10ff7f5fa6a, B7a219f38626fa5e0ce7bb4c63db785459c7d8ba3a010470d26501d6cbcce2c93. The observer skips only FileNotFoundError/ProcessLookupError for vanished tasks and continues rejecting live priority/affinity violations. Fresh 18 children/nine complete pairs all passed. Earlier unpairedA is excluded. Three AB/BA/AB pairs at each width, default retention,1024warmup rounds, exact original replies and all 3 core commit/application/published frontiers, unfenced state, checkpoints and settings checked.

| Driver width | Whole-child CPU/original | Internal elapsed/original | Pairs |
| --- | ---: | ---: | ---: |
| 1 | -6.890% | -6.832% | 3/3 |
| 231 | +3.229% | +3.335% | 3/3 |
| 515 | +2.260% | +2.428% | 3/3 |

Bulk driver CPU was worse in every pair. Whole-child CPU includes startup/warmup/validation; internal elapsed excludes warmup. Both divide measured originals, so their absolute values are not comparable to canonical rates. Diagnostic process::exit precedes canonical owned-shutdown qualification; no lifecycle gate is claimed from this driver. These controls show a singleton work saving with wider-batch cost; they do not prove a parking cause for canonical rate losses.

## Review, correctness and cleanup boundary

Both source reviewers approved exact7799 with no P1/P2 findings. Builder focused core 202 / matched 75 per config and supplemental notice 18, logical release 11 + persistence 2 each default/all-feature; rootfmt/four strictClippy/matchedfmt+Clippy and actual enabled push hook passed. Counts overlap and are not full-suite totals. Semantic feature matrix remains on the rejected branch. No feature was deleted for a score. Full default/all/no-default/native/matched suites and final semantic qualification remain unrun, not green.

All builder/reviewer/measurement delegated tasks are terminal; own worktrees/targets/hosts removed. R4 used only two binary-only named Mac roots, both removed with empty parent; raw contexts and current guard receipts preserved. R4 sampled 520 Linux task records with zero observed live-envelope violations; no vanished records occurred in this finite run. Mac process/thread snapshots checked; no Mac affinity claim. Early resource-envelope limitations in preceding tasks remain disclosed. Final full corrected stock guard passed (actual Mac check), Linux 0 scratch / 0 GiB, root 3 GiB / live 1, Mac 10 GiB / 2 dirs, free 2867 GiB. No known owned runner/binary/deleted-cwd jobs remain. Foreign worktree/shared port namespace preserved.

## Artifact map

Canonical source/build/artifact identities and all 64 raw reports: target/claude/pillar1-20261022/cpu1-perf/REPORT.md, raw/, summary.json.
Original failures/control/build provenance: cpu1-driver-r2/ and cpu1-driver-r3/ under the same root; preserved R3 manifest of 220 files verified unchanged.
Final driver: cpu1-driver-r4/protocol.json, SCHEMA.md, frozen-inputs.json, summary.json, AUDIT.json and raw/ (all 18 stdout/stderr/wait4/argv/environment/settings/guards/context/artifact checks), exact controllers, cleanup/fullguard receipts. No diagnostic build tree remains.
Resource helper correction: diskguard-empty-input/REPORT.md, before/after/diff/controls. Test expectation mistakes are archived; no capacity was raised.

Next work remains exclusively pillar1: choose the next large core/embedding CPU cost from this measured residual, considering fewer volatile completion/staging handoffs. No parked network/storage candidate is reopened and no new split/lifecycle roadmap is implemented.
