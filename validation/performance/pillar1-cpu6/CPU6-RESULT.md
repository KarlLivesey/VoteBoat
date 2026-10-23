# CPU6 result: rejected without merge

23 October 2026. [PR30](https://github.com/KarlLivesey/VoteBoat/pull/30)
is CLOSED, unmerged, at `bd579edd7ace4741f80fdcf0d44abee05db976d6`.
The prepared-entry-batch candidate is rejected as a CPU optimization. Production
remains `0bf9271ecb5a34c4089c6bb10381303232624170`; none of its source or tests
is integrated. This commit records the decision and evidence only.

## Decision and actual CPU results

Each original width independently requires median paired whole CPU/original B/A
below1 and at least two CPU wins from three pairs. All18 primary children/nine
pairs passed their functional guards. Width1 failed; the two wider wins cannot
offset it.

| Width | Actual measured originals per child | Median CPU B/A | CPU change | Wins | Gate |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 | 200,000 | 1.077819244 | +7.781924% | 0/3 | FAIL |
| 231 | 1,386,000 | 0.997322745 | -0.267725% | 3/3 | PASS |
| 515 | 1,545,000 | 0.993227627 | -0.677237% | 2/3 | PASS |

The parent independently recomputed every CPU/original from the18 raw direct
wait4 user+system rows and actual completed-original counts, then all nine
ratios, medians and wins. Raw captured JSON matched each saved document; child
exit/join, exact PID/start/kernel executable identity and functional checks were
verified. [Parent recomputation](evidence/PARENT-RECOMPUTATION.json),
[compact child rows](evidence/CPU-child-rows.json) and
[all pairs](evidence/CPU-pairs.csv) preserve that calculation.

Whole target-thread CPU includes startup/election,1024 warmup ROUNDS, cold
ownership, validation, drop and diagnostic exit. Warmup/helper CPU is not
subtracted or substituted. Original schedule AB/BA/AB, widths/specs, one worker,
default retention, actual full original IDs/counts, all three commit/application/
reported frontiers, settings/checkpoints/bounded suffixes remained guarded.
Internal elapsed is a separate observation. No valid pair was discarded or
replaced. No tolerance, offset, unchanged timed retry or performance-cause story
is inferred.

Canonical was conditional on every primary width passing: **0/64 ran**. Canonical
rate/p99 and normal owned Tokio/Actor shutdown remain untested. No full default,
all-feature, no-default, native-only or matched suite was launched for CPU6.
Source/focused/static results do not qualify the full feature matrix.

## Source, mechanism and preserved features

Fresh exact-head source-only COMMENT approvals:
[A5633159800](https://github.com/KarlLivesey/VoteBoat/pull/30#pullrequestreview-5633159800)
and [B5633193885](https://github.com/KarlLivesey/VoteBoat/pull/30#pullrequestreview-5633193885),
no P1/P2/materialP3. Each reviewed all42 changed paths and independently checked
all2609 frozen actual build/consumer inputs. These are source verdicts, not
formal GitHub APPROVED or performance/full-suite acceptance.

The candidate added batch-level Inline LogEntry ownership for direct singleton
proposals/selection and fused receiving-core assignment with immutable facts in
the command-origin wide batch's same Vec. Canonical storage remained Owned/Shared
pieces; generic/mixed validation, receiving authority and required ownership
stayed checked. The explicitly authorized independent logical capsule changed to
Protocol(Box<Message>) with full heap/capture/provenance/queue/decode charges and
borrowed full-value authorization. That logical allocation/clone work is an
introduced cost. Ordinary Message/Rpc/Pending remained inline, with larger fixed
values and transfers also recorded as costs; no limit or lint was weakened.

Builder evidence reports1377 focused passing events, not unique tests, with two
existing ignored selections; strict7 and the actual enabled nonforce push hook
passed. Two semantically VALID old-representation work controls are distinct from
thirteen incorrect compiled mutant kills. The first retained-charge mutation
survived a dependent cached-charge oracle; its raw pass remains, and the later
independent backing/payload ledger killed that same error. Exact restoration
receipts are retained, without converting the first result into a kill.

The focused matrix covered actual public volatile/durable nonempty Counter and
all three normal matched replicas at B1/B231/B515 through two default5000-entry
checkpoints, real lagging snapshot catch-up and continued writes. It also covered
quorum/logical-child consumed-DURABLE, Written/stale rejection, membership/read/
lifecycle, native/crash/codec/FIFO/partial progress and independent-group paths.
No implemented VoteBoat feature is removed from production by rejecting CPU6.

Apple Stage0 passed34 focused tests. Two separately compiled old VALID work
controls and exact2609-input restoration supported the runner's normal ARM64
singleton transient-Vec and extra uniform-wide-inspection removal findings.
[Static attribution](evidence/STAGE0A.md) records complete bounded paths rather
than function disappearance alone. Required canonical growth, payload ownership,
mixed/generic passes, fixed copies, Arc/shrink and application/receipt work remain.
Static elimination did not establish useful net CPU benefit.

## Failures, observers and qualification boundaries

Builder R1 stopped at an enlarged Message enum lint; the parent authorized only
the direct logical-capsule owner amendment. Builder R2 compiler0 with missing-PID
ProcessLookupError/observer-bootstrap1 remains incomplete, cause UNKNOWN/HOLD.
R3's two synthetic metadata controls validate its corrected observer only.

Qualification R1 stopped before build: all2609 received bytes/sizes matched, but
Git archive permissions were0664/0775 rather than frozen0644/0755. Its receiver
correctly refused; no Rust/control/timing ran. The parent corrected only source
packet headers from verified manifest rows and tested both mode classes plus an
old-header negative. R2 retained strict received bytes/modes checks and matched
whole sender/receiver packet hashes before building. No global umask/ACL/Git
config or failed-tree normalization workaround was used. R1 raw failure remains.

R2 passed two native executable-identity controls and all six exact Rust driver
controls: three healthy, three telemetry-only negatives with actual rc1 and only
reported_frontiers failing while real core/application/ID evidence passed.
All control CPU is discarded. Native Darwin kernel path and PID/parent/start/
nice, frozen binary hashes, direct wait4 and original capture/scorer were retained.
Read/setup/parser faults, early observation qualifications and all historical
resource/visibility HOLDs remain in the [runner report](evidence/REPORT.md).
Clean finite samples do not establish whole-machine or historical clearance.

## Identities, cleanup and continuing scope

- B tree: `6c421547bfac8778cc27b1fcec479aaa7bf19a4d`.
- Frozen2609-input manifest SHA256:
  `588bb969a10af790e09ef9e1e174884251573bdc9378b09320db475c8d89b6d0`.
- Full candidate patch SHA256:
  `03a63974b95de869beee0ebcb3a801df6e185f17468b18afd6b008ba9f3c5793`.
- Normal canonical B SHA256:
  `206e1a5771cee811761187ea599819749e36b6cd01a4bc1f43bd071ea32a92da`.
- Diagnostic B SHA256:
  `bef1c35b2944566348465607a3f941756e419363797ec84bbc27656956fed75f`.

Actual Mac14,5/M2Max/12CPU, original Rust1.98.0/Cargo1.98.0/aarch64-apple-darwin,
locked/offline/jobs4/incremental0/release-debug0/thinLTO/CGU1/emptyRUSTFLAGS;
nice19, no Mac affinity claim. A reused original frozen binaries, never rebuilt.
The exact original CPU1 R3 two-file diagnostic overlay was retained.

Normal2609 source inputs were restored. The runner collected and hash-verified
343 files before removing both owned Mac roots, shared target and empty parent.
Known-identity audits and the final unchanged actual-Mac stock guard passed.
Foreign roots/tools/shared test ports/infrastructure were untouched. Published
candidate source and full raw binaries/assembly/controls/failures/observations
remain in target/claude/pillar1-20261022/cpu6-perf-r2/ and earlier CPU6 reports.

[Runner comment](https://github.com/KarlLivesey/VoteBoat/pull/30#issuecomment-6373616989),
[closed API state](evidence/PR30-CLOSED.json) and compact
[SHA256SUMS](evidence/SHA256SUMS) preserve the disposition. This is not a merge.
All six CPU candidates are rejected without source integration. The user's
Pillar1 CPU goal continues: select one justified structural design or identify
one precise missing measurement before another implementation. Different
singleton/wide specializations within one coherent ownership design are allowed;
the existing per-width gates and feature preservation remain unchanged.
