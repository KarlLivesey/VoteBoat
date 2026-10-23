# CPU5 result: rejected without merge

23 October 2026. [PR29](https://github.com/KarlLivesey/VoteBoat/pull/29)
is CLOSED, unmerged, at `81d2cfe6b953e9a268c6444d571fed5f21263652`.
The borrowed summarized metadata and append-construction reorder are rejected
as a CPU optimization. Production remains
`0bf9271ecb5a34c4089c6bb10381303232624170`; none of this candidate's source
or tests is integrated. This commit records the outcome and evidence only.

## Decision and actual paired results

Each original width independently requires median paired whole CPU/original B/A
below1 and at least two CPU wins from its three pairs. All18 primary children
and nine pairs completed with functional, actual identity and observed resource
guards. Widths1 and231 failed. Width515 cannot offset them.

| Width | Actual measured originals per child | Median CPU B/A | CPU change | Wins | Gate |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 | 200,000 | 1.008275503 | +0.827550% | 0/3 | FAIL |
| 231 | 1,386,000 | 1.002872001 | +0.287200% | 1/3 | FAIL |
| 515 | 1,545,000 | 0.999994667 | -0.000533% | 2/3 | PASS |

CPU is direct exact-PID wait4 user+system across all target threads divided by
actual completed measured originals. Startup/election,1024 warmup rounds,
cold ownership, validation, drop and diagnostic exit remain included; warmup
CPU is not subtracted. Original widths/specs/order AB/BA/AB, default retention,
one worker, all actual IDs/counts/three commit/application/reported frontiers,
settings, checkpoints and bounded suffixes were checked. Internal elapsed is
separate. Every valid pair is preserved in [CPU-pairs.csv](evidence/CPU-pairs.csv)
and [the runner report](evidence/REPORT.md). Parent independently recomputed
CPU/original, ratios, medians and wins from the18 raw child wait4 rows.

Canonical was conditional on every primary width passing: **0/64 ran**.
Canonical throughput/p99 and normal owned Tokio/Actor shutdown are untested.
No full default/all-feature/no-default/native/matched suite was launched for this
candidate. No speedup, cause/allocator/frequency story, new tolerance, replacement
measurement, unchanged timed retry or cross-width offset is inferred.

## Source and static machine mechanism

Fresh exact-head source-only COMMENT approvals:
[A5628623002](https://github.com/KarlLivesey/VoteBoat/pull/29#pullrequestreview-5628623002)
and [B5628638541](https://github.com/KarlLivesey/VoteBoat/pull/29#pullrequestreview-5628638541),
no P1/P2/materialP3. Both independently verified1532 inputs. These are source
verdicts, not GitHub APPROVED, performance or full qualification.

The original private view change borrowed immutable summarized metadata while
native/new/trimmed views kept independent owned snapshots. Required plans,
predecessors, stamps, charge checks, Pending and consumers stayed owned/checked.
Initial Apple codegen at a97 still materialized an equivalent304-byte temporary
across append_summary. The authorized81 follow-up changed only OwnedAppend::new
to compute the pure summary first and then copy the required owned predecessor;
the borrowing source remained exacta97. Both orders are semantically valid.

R4 compiled the old-order-only negative first, restored all1532 exact candidate
inputs, built normal B and passed five actual-Mac view tests. Actual old control
retained three mutually exclusive extra304 spill sites plus two owned-copy sites.
Normal B retained only the two required direct predecessor304 copies, after
append_summary. No equivalent zero/full-view/summary replacement was found on
the eligible arms. Required summary336/staging672/effect272/install/stamp work
remained. R5 independently checked the sealed actual instruction words and
79 normalized helper instructions. This proves the specified static elimination,
not dynamic frequency, allocation reduction or useful net CPU savings.

Builder follow-up had1036 positive test events (not unique tests), zero selected
failures/ignored; strict7 and actual enabled nonforce push hook passed. Semantic
coverage retained weighted/recursive/logical-child quorums, membership/learners/
joint/final/retirement/transfer, reads/readiness/authority, full IDs/dedup/outbox/
lineage, snapshots/recovery/default maintenance and bounded independent groups.
Focused evidence does not certify the full feature matrix; full5 stayed pending.
No implemented VoteBoat capability is removed from production.

## Observer controls and preserved failures

R1 stopped before transfer/build after a120s preflight timeout; output was lost,
cause UNKNOWN/HOLD. A bounded launcher diagnosis separately passed its fault
controls and one preflight without resolving the lost original cause. R2's a97
machine gate failed equivalent replacement materialization despite build/tests
passing. R3 used the wrong bootstrap directory and exited2 before its payload.

R4 passed machine/tests/build, then its first healthy Rust control passed all ten
functional checks but failed controller identity: truncated ps comm was treated
as executable path. R4 identity HOLD/rc1 is not retroactively qualified. Its
post-STOP prohibited bootstrap-as-payload invocation and other workflow gaps
remain recorded. No primary pair ran in R1-R4.

R5 narrowly corrected the external observer to obtain native Darwin executable
path and actual PID/parent/nice/creation identity, immediately after direct Popen.
Raw native errors/bytes preceded assertions; full realpath and frozen hashes
were checked instead of argv0, basename or truncated prefix. Two metadata helpers
passed, including rejection of the wrong actual executable behind a spoofed argv0.
All six original Rust controls then passed: three healthy and three telemetry-only
negatives. Negatives actually exited1 with only reported_frontiers failing while
real core/application/ID/outcome evidence passed. All control CPU is discarded.

The same direct exact-PID wait4/capture/status/rusage/identity path was used for
the18 primary children. No wrapper/helper CPU or new O_DSYNC writes were charged
to Rust; no Popen poll/wait reaped scored children before wait4. Preparation
faults, initial/read/bootstrap and observer visibility gaps remain in the report.
Historical CPU3/CPU4 resource HOLDs remain uncleared and unattributed. Clean new
samples do not establish whole-task/global/historical certification.

## Identities, cleanup and retained evidence

- A: `0bf9271ecb5a34c4089c6bb10381303232624170`; B:
  `81d2cfe6b953e9a268c6444d571fed5f21263652`, tree
  `fdbaf14f4cfc199b7854e6b4080f25fbccd2bba1`.
- Frozen1532-input manifest SHA256:
  `1bdd11f6236b36ed79a52522564e232b0ac29254eca1439d75c80d7f486eaf08`.
- Canonical A SHA256:
  `0c6998bc63614f43faaba36dfff64f9291b7a14336853f4d901c4068c2c69208`.
- Diagnostic A SHA256:
  `c05c3c718ab6d9ac5a72f2e04cfce29fb35f6618a76b3e0debdda10ff7f5fa6a`.
- Canonical B SHA256:
  `36ad89f682f1d21ce09cf7b68405e9dd8111064d842107e4414bb093432bac4a`.
- Diagnostic B SHA256:
  `ed521eff725d81a8ef15c6726d5c414521a9a676a2250137d1d3fa463901c1ee`.
- Mac14,5/M2Max/12 CPUs; Rust1.98.0/Cargo1.98.0, aarch64-apple-darwin,
  locked/offline/jobs4/incremental0/release debug0/thinLTO/CGU1/emptyRUSTFLAGS;
  nice19, no Mac affinity claim. R5 reused exact sealed binaries; no rebuild/test
  or source extraction ran in R5. Exact original repaired R3 overlay was retained.

R5 collected/hash-verified278 remote files before sweeping both owned binary-only
Mac roots and empty parent. Known saved process identities were audited absent;
final unchanged actual-Mac stock guard passed. Original R1/diagnosis/R2/R3/R4
evidence hashes stayed unchanged. Unknown infrastructure/foreign roots/shared
test ports were untouched. Published candidate branch remains as evidence.

[Runner comment](https://github.com/KarlLivesey/VoteBoat/pull/29#issuecomment-6368634077)
and [closed API state](evidence/PR29-CLOSED.json) record the disposition. Compact
files have exact hashes in [SHA256SUMS](evidence/SHA256SUMS). Full raw binaries,
assembly, source manifests, controls, all failures and resource/cleanup receipts
remain in `target/claude/pillar1-20261022/cpu5-perf-r5/` and preceding CPU5
evidence directories. This record is not a merge or full-suite qualification.

All five new CPU candidates are rejected without source integration. Further
Pillar1 work needs a different source-supported and dynamically significant cost;
no successor implementation or speedup claim follows from this rejection.
