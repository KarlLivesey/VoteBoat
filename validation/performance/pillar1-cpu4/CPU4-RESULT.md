# CPU4 result: rejected without merge

23 October 2026. [PR28](https://github.com/KarlLivesey/VoteBoat/pull/28)
is CLOSED, unmerged, at `bb311bf16ba6212a8542a4df7b82014fc3b49883`.
The public reusable original-write port and its matched consumer are rejected
as implemented. Production remains `0bf9271ecb5a34c4089c6bb10381303232624170`;
none of this candidate's source or API is integrated.

## Decision and measured results

The unchanged primary gate requires median paired whole-process CPU/original
B/A below 1 and at least two CPU wins in each width's three pairs. All nine
original pairs completed; every width failed, with zero wins. The parent
independently recomputed these ratios from the 18 recorded child rows.

| Width | Actual measured originals per child | Median paired CPU B/A | CPU change | Wins |
| --- | ---: | ---: | ---: | ---: |
| 1 | 200,000 | 1.093521186 | +9.352119% | 0/3 |
| 231 | 1,386,000 | 2.539935634 | +153.993563% | 0/3 |
| 515 | 1,545,000 | 2.674665391 | +167.466539% | 0/3 |

All 18 children passed functional/input guards, including actual generated IDs,
all three committed/applied/published frontiers, exact original outcomes,
default retention/checkpoints and bounded suffixes. Whole direct-child wait4
user+system CPU includes all threads, startup, election, warmup, cold port/client
ownership, copies, locks, validation, drop and exit; warmup CPU is not subtracted.
Internal elapsed time is recorded separately. AB/BA/AB order, settings and every
pair are preserved in [the report](evidence/REPORT.md),
[child rows](evidence/all-measured-children.csv) and
[parent recomputation](evidence/parent-pair-recomputation.json).

Canonical runs were conditional on passing every primary width: **0/64 ran**;
canonical B was not built. Canonical throughput/p99 and normal owned Tokio
shutdown remain untested. No full default/all-feature/no-default/native/matched
suite was launched after the failed CPU gate. No tolerance, replacement run,
allocation/instruction count or cause attribution is inferred from this loss.

## Source and control boundaries

Final source-only COMMENT approvals:
[A](https://github.com/KarlLivesey/VoteBoat/pull/28#pullrequestreview-5625966059)
and [B](https://github.com/KarlLivesey/VoteBoat/pull/28#pullrequestreview-5625979373).
Both independently verified all 1,555 frozen inputs. The final independent
adapter audit returned GO with A1 resolved. These are source verdicts, not
GitHub APPROVED, CPU acceptance or full qualification.

The earlier P2 permitted prefix publication before whole-result cardinality/ID
validation; the fix prevalidates before publication. The earlier adapter A1
omitted failed-send originals from generated counts; the fix counts real cursor
creation before the send. Both objections, compiled negative controls and
restoration history remain in the candidate branch and safe raw evidence.
Final default/all-feature matched tests passed 59 each, complete overlay controls
12, strict seven checks and the actual enabled pre-push hook passed. These
builder receipts are historical; the measurement runner did not rerun Clippy.

Fresh Mac Stage 0 passed 12 tests, the actual nonempty Counter example, and all
14 final-driver JSON controls: three healthy, three telemetry negatives, three
count negatives and five genuine queue-fault cases. Actual final JSON/failure
paths were exercised. Control CPU is discarded; temporary changes were restored
before freezing the measured binary. Coverage limits, initial parser correction
on the same saved output and observer corrections are explicit in the report.

## Identities and reproducibility

- A: `0bf9271ecb5a34c4089c6bb10381303232624170`; B:
  `bb311bf16ba6212a8542a4df7b82014fc3b49883`.
- Frozen 1,555-input manifest SHA256:
  `ea7ed1c28273da125aeefc3fcb1597c4f4f680c732491ddd0c1f129c3d8394f2`.
- Diagnostic A binary SHA256:
  `c05c3c718ab6d9ac5a72f2e04cfce29fb35f6618a76b3e0debdda10ff7f5fa6a`.
- Diagnostic B binary SHA256:
  `827847cd6ffd993b69ac15ea7bd51a95821817618043f86b692d4dc42d9d29f2`.
- Mac14,5, Apple M2 Max, 12 CPUs; Rust/Cargo stable 1.98.0, aarch64-apple-darwin;
  locked/offline, four build jobs, incremental off, release debug off, thin LTO,
  one codegen unit. Absolute nice19; no Mac affinity claim.

The approved two-file diagnostic overlay and frozen driver are identified in
the report and protocol. Normal canonical behavior was not measured. The
overlay emits three retained dead-code warnings; this is not a clean diagnostic
Clippy claim. No binary identity is inferred merely from source equality.

## Resources, cleanup and disposition

The campaign recorded 105,270 Linux thread rows and 5,722 Mac resource events;
current sampled violations were zero. Peak sampled own Mac storage was
1,072,451,584 bytes under the unchanged 20 GiB own limit. Short-lived,
reparented, initial tool/bootstrap and observer visibility gaps remain disclosed.
This is not a global or whole-task resource certification. Historical CPU3 and
CPU4 resource HOLDs, including the unexplained parent priority transition, remain
uncleared and unattributed; later clean observations do not erase them.

All 336 collected raw/artifact files were hash-verified before deleting both
owned Mac roots and their empty parent. Known owned jobs joined; final unchanged
stock guard passed, including actual Mac observation. No Linux build tree was
created by the performance runner. Unknown infrastructure and shared test ports
were untouched. Published candidate branch and safe evidence remain.

[Runner comment](https://github.com/KarlLivesey/VoteBoat/pull/28#issuecomment-6362066969)
and [parent rejection](https://github.com/KarlLivesey/VoteBoat/pull/28#issuecomment-6362170292)
record the disposition. [GitHub state](evidence/PR28-CLOSED.json) confirms closed,
unmerged at the measured source. Compact immutable files are indexed in
[SHA256SUMS](evidence/SHA256SUMS). Full raw evidence, binaries, controls and
resource receipts remain at `target/claude/pillar1-20261022/cpu4-perf-bb/`.

The next bounded Pillar 1 design must justify a different removable core or
embedding cost from unchanged production, preserving every implemented feature.
No new implementation or claimed speedup follows from this rejection.
