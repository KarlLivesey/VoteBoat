# Transaction protocol model311 — terminal Linux/macOS

This is the required independent model gate for the proposed P8 two-participant
account transfer. It implements no native transaction engine. Production remains
unchanged; five independent test files add the model, oracle, search and controls.
The complete P8 functional scope and the original Mac302 baseline sweep remain
separate open work. This goal stops after qualified functional P8; P9/P10 do not
start automatically.

## Actual checks

The frozen714 compilation/test inputs match the root and each host before/after
qualification. `source.sha256`, the source logs and `checks.sh` retain that scope.
Linux used the isolated `target/divide-membership/build`; Mac used the idle
`diagnostic303-build`, separate from the live302 build. No active executable was
overwritten. The script executes these profiles sequentially: all features,
default, no default features, and native only.

| Check | Linux | macOS |
| --- | --- | --- |
| Transaction model, each of four profiles | 8 passed / 0 failed | 8 passed / 0 failed |
| Formatting | Zero diagnostics | Zero diagnostics |
| Strict all-target Clippy, each profile | Zero diagnostics | Zero diagnostics |
| Before/terminal frozen inputs | 714 match | 714 match |
| Script terminal | 0 | 0 |

The32 test executions per host repeat the same eight tests across configurations;
they are not32 distinct tests. This is affected-target qualification, not a full
714-input platform test sweep. No lint level or threshold changed.

## Model and acceptance

Exactly two immutable transfers of5/7 operate on accounts initially20/0. Both
coordinator Starts are already durable. Coordinator phases are Started, Decided,
AppliedAll and Finished; participant phases are Absent, Prepared, DecisionApplied
and Released. Prepared locks survive application and restart. Timeout cannot
decide Abort. Original prepares permit Commit; all applications precede release.
Collected observations and two lossy message slots are volatile; only committed
phase proofs and participant state survive restart. One slot per transaction
bounds message interleavings; arbitrary intra-transaction queues are not modeled.

Each profile on both hosts exhausts **126,818 states / 827,613 transitions**,
including222,264 loss and126,415 restart edges. The declared80-step/250,000-state
ceilings are not reached. Distinct reachable states include242 with both commits
finished and704 with a finished commit/abort pair. These counts are overlapping
state categories, not independent complete histories or liveness evidence.

The independent oracle checks decision stability, exact phase provenance,
retained lock ownership, application/release ordering, nonnegative balances and
conservation when both accounts are readable. Selected controls complete both
transfers, contention/insufficient-funds Abort and lost-receipt reconstruction.
Three actual weakenings produce discovered counterexamples: timeout Abort,
premature release and decision reversal. The oracle takes no weakening selector.

The initial40-step search truncated before both commits; inspection corrected
that coverage. A second ownership review replaced durable receipt collections
with volatile observations and committed phase proofs. Only the final refined
graph above is qualified. [Model review](MODEL_REVIEW.md) records those differences
and the earlier commands; [schema](MODEL_SCHEMA.md) records the design/refinement.

This finite abstraction assumes committed/applied commands and trusted receipt
origins. It does not prove native Raft durability, driver provenance, arbitrary
participants/programs, elastic ownership or general liveness. The next deliverables
implement actual replicated applications and a driver that consumes original
scoped Node completions or fresh quorum-backed phase reads. The
[provenance review](PROVENANCE_REVIEW.md) records that boundary. Safe voter
membership does not need to be frozen; group ownership is fixed for the initial
transaction scope.

Raw test/check output and frozen source guards are retained here;
`retained.sha256` hashes those files. The model's own five input digests are
`model-source.sha256`. The original design pack is preserved.
