# CPU2 measurement and final disposition

**Reject [PR26](https://github.com/KarlLivesey/VoteBoat/pull/26) as implemented; close without merging.** Exact candidate `d7b4ec739b4b9dde8d0b0d00c59608ce3fc2c6ff`; baseline `0bf9271ecb5a34c4089c6bb10381303232624170`. Candidate base `03f3ffd9` is production-identical to that baseline. Main receives only this evidence and plan update; none of CPU2's public volatile-turn API, installer-dispatch changes or matched consumer is integrated.

Both independent Sol high/priority source reviews approve exact D7 with no P1/P2 or material P3: [A5622946232](https://github.com/KarlLivesey/VoteBoat/pull/26#pullrequestreview-5622946232), [B5622970198](https://github.com/KarlLivesey/VoteBoat/pull/26#pullrequestreview-5622970198). Focused correctness and strict checks succeeded, but the predeclared CPU gate failed. This is a performance rejection, not a demonstrated consensus defect.

## Fixed-driver result

All 18 fresh children/nine complete pairs passed their original functional guards. Three pairs per width in AB/BA/AB order, one server worker, default retention and 1024 warmup rounds. Whole-child user+system CPU divided by actual measured completed originals includes startup, warmup and validation; internal elapsed excludes warmup. Medians are medians of paired B/A ratios.

| Width | Measured originals per child | CPU/original | Internal elapsed/original | CPU wins | Width gate |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 | 200,000 | -1.097238% | -1.054983% | 3/3 | Pass |
| 231 | 1,386,000 | -2.143375% | -2.579145% | 2/3 | Pass |
| 515 | 1,545,000 | +0.695367% | +0.425918% | 1/3 | Fail |

Each width had to have median CPU B/A < 1 and at least two CPU wins out of three. Width515 failed both conditions. The other widths cannot offset this gate; no new tolerance or replacement schedule was adopted. The small aggregate differences do not establish an instruction/allocation saving or a causal explanation for the width515 cost.

Every child checked actual submitted/replied original IDs/counts, zero unreplied/wrong replies, all three actual core commit/application/published frontiers, no fencing or propagated error, checkpoints and settings. Including warmup, final prefixes were 201025 / 1622545 / 2072361 at widths1/231/515, respectively; checkpoints40/319/402 on each replica. No capability was disabled. The diagnostic exits before ordinary owned shutdown, so it does not qualify canonical lifecycle.

**Canonical held: 0/64 children, 0/32 pairs.** No canonical throughput/request-p99 or final CPU acceptance is available. All five complete repository configurations were also held; focused results and source approval do not make them green. No unchanged retry, new repair or tolerance followed the failed gate.

## Inputs and controls

Mac14,5 Apple M2 Max/12 CPUs, Rust stable1.98.0 and Cargo1.98.0, matching release flags/locks; paired children use equal worker counts. Baseline diagnostic binary SHA256 `c05c3c718ab6d9ac5a72f2e04cfce29fb35f6618a76b3e0debdda10ff7f5fa6a` was reused after original compile/toolchain/overlay provenance verification; every runtime observation is new. B diagnostic SHA256 `4c82488a265a7151e5c6aba2002b7fbbc942bb48a4a1d04436c0941150d90ae8` is exact D7 plus the identical two-file measurement adapter. Canonical baseline binary was verified but unused; canonical B was not built.

The qualified R3 telemetry-only negative control was referenced without rerunning it: actual replies/core/application passed, while deliberately omitted shared publication failed exactly the telemetry conjunct. The adapter drives the real Actor and new public API, not a replacement implementation. No phase attribution reduction is presented as whole-process CPU gain.

The first small pinned build packet omitted Cargo-declared examples; Cargo manifest parsing failed before Rust compilation or measurement. The explicitly permitted single input-closure correction added 48 exact D7 example files without changing any original input. Original failure and patch identities remain preserved. No measured pair was replaced. [Original runner report](evidence/REPORT.md) records source/overlay closure, all nine ratios, counts, context and artifact map.

## Correctness and cleanup limits

Builder focused selections passed621 with one existing ignored timing report: root-default selections plus normal matched selections, not621 in every configuration. Five compiled causal controls were killed and restored. Public focused traces retain8 messages/9 events/6 checked installations/3 intervals while reducing external completion handoffs6 to0; this structural count reduction was not sufficient to meet the CPU gate. Prepublication strict7 and actual enabled nonforce push-hook7 passed. Both reviewers independently matched all1535 frozen build inputs.

The FIFO mutant changes the legacy persist consumer; ordering of the new cold finish-turn arm additionally rests on source inspection and the synthetic nonzero-position test. All implemented feature preservation remains a requirement, with the candidate's semantic matrix retained on its branch; no full feature/full-suite acceptance is claimed here.

Owned builder checkout/target/registration and both owned Mac roots were removed. The runner reports no known owned jobs and a final unchanged full stock guard pass with actual Mac capacity check. Samples:773 controlled Linux task records and105 Mac descendants, zero observed live envelope violations. Early inventory containment gaps are disclosed; sampling is not whole-task proof. Three uncertain automatic CUA hosts with deleted builder cwd were deliberately left untouched because cwd/shared ancestry is not ownership proof. No claim of zero infrastructure or full historical containment is made.

Only compact immutable receipts are committed here; their SHA256SUMS accompanies them. Complete raw children, binary, source packets/overlay hashes, original failure, controllers, resources and cleanup remain under `target/claude/pillar1-20261022/cpu2-perf/`. [Durable runner comment](https://github.com/KarlLivesey/VoteBoat/pull/26#issuecomment-6355434674).

Next work remains exclusively pillar1: choose a bounded reduction in repeated per-original core/embedding work from unchanged production. CPU1/CPU2 are rejected implementations; network/storage and parked candidates remain paused.
