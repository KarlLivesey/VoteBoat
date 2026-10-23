# CPU6 R2 finite qualification — FAIL primary CPU gate

Exact candidate `bd579edd7ace4741f80fdcf0d44abee05db976d6`, tree `6c421547bfac8778cc27b1fcec479aaa7bf19a4d`; draft [PR30](https://github.com/KarlLivesey/VoteBoat/pull/30). Parent remains sole main writer and owns disposition. MAIN `5fda857d2f827a47925c152f227bce7cbc1ced98` is clean; production A `0bf9271ecb5a34c4089c6bb10381303232624170` and pinned objects are unchanged. No tracked, memory, tools, PR-body or source push occurred. Hooks remain `.githooks`. No CPU acceptance, merge or full5 release follows.

Stage0a PASS; Stage0b PASS; Stage1 **FAIL_PRIMARY_CPU_GATE**. All **9/9 valid pairs /18 fresh children** were retained, with no replacement or unchanged Rust/workload/identity retry. Canonical **0/64**, unreleased because width1 fails. No offset, tolerance, profiling, cause story, follow-up candidate or successor was attempted.

| Width | Median of three paired B/A ratios | Paired change | B wins | Fixed gate |
| --- | ---: | ---: | ---: | --- |
| 1 | 1.077819244 | +7.781924% | 0/3 | FAIL |
| 231 | 0.997322745 | -0.267725% | 3/3 | PASS |
| 515 | 0.993227627 | -0.677237% | 2/3 | PASS |

Each width requires median<1 AND at least2/3 strict B wins. Score is **whole exact-PID `wait4` all-target-thread user+system CPU / actual completed measured originals**. Startup, warmup and all work through process exit remain charged; no helper CPU, subtraction, sampler-derived CPU or server-only timing enters it. The original R3 diagnostic process-exit path is unchanged. Normal owned TokioActor shutdown was reserved for canonical Stage2, which did not run. No canonical throughput/p99 or normal-shutdown result is claimed.

## Every actual paired result

| Width | Round | Order | A CPU ns/original | B CPU ns/original | B/A | Winner |
| --- | --- | --- | ---: | ---: | ---: | --- |
| 1 | 1 | A/B | 2620.015000000 | 2812.020000000 | 1.073283932 | A |
| 1 | 2 | B/A | 2591.000000000 | 2804.325000000 | 1.082333076 | A |
| 1 | 3 | A/B | 2597.815000000 | 2799.975000000 | 1.077819244 | A |
| 231 | 1 | A/B | 111.569985570 | 111.271284271 | 0.997322745 | B |
| 231 | 2 | B/A | 111.843434343 | 111.635642136 | 0.998142115 | B |
| 231 | 3 | A/B | 114.103896104 | 111.244588745 | 0.974941194 | B |
| 515 | 1 | A/B | 120.420711974 | 119.605177994 | 0.993227627 | B |
| 515 | 2 | B/A | 121.685436893 | 119.042071197 | 0.978277058 | B |
| 515 | 3 | A/B | 121.118446602 | 122.065372168 | 1.007818178 | A |

Original schedule: rounds AB/BA/AB, widths1/231/515 in every round; specs `200000,1,1` / `6000,231,4` / `3000,515,4`;1024 warmup ROUNDS, one worker, default retention, upstream default envelopes, attribution false,600s target timeout. Actual measured originals per child:200000/1386000/1545000; total replies including warmup201024/1622544/2072360; all three commit/applied/reported frontiers201025/1622545/2072361. All ten functional conjuncts and original independent full-ID/cardinality/count/settings/remaining/default-checkpoint/suffix/no-error/nonfenced checks passed for all18. Raw stdout JSON is compared again with each saved document; exact PID wait4/exit/kernel identity receipts are preserved.

## Stage0 evidence and boundaries

Normal canonical B was built once on actual Mac14,5/M2 Max12CPU, aarch64-apple-darwin, original Rust1.98.0(88d9e12ae)/Cargo1.98.0(797e8a9bc). Locked/offline/jobs4/incremental0/release thinLTO/CGU1/debug0/empty RUSTFLAGS; no installs, flags, caps, OS or hook changes. One B source/one shared normal+matched target, no Linux build/source extraction/copy/worktree. A was never rebuilt: frozen canonical A4,114,944 bytes SHA`0c6998bc63614f43faaba36dfff64f9291b7a14336853f4d901c4068c2c69208`; diagnostic A4,192,928 bytes SHA`c05c3c718ab6d9ac5a72f2e04cfce29fb35f6618a76b3e0debdda10ff7f5fa6a`.

Actual focused default Mac tests:25 entry,2 prepared capsule,2 release capsule,1 actual singleton producer/selector,4 compiled-size probes = **34 pass, zero failed/ignored**. No full-suite/relay/strict duplicate was added. Compiled Message272 and fixed enclosing plan/effect copies remain; capsule/command/release sizes are recorded in [STAGE0A.md](STAGE0A.md).

[STAGE0A.md](STAGE0A.md) traces complete bounded actual normal ARM64 producer, selector, pending, clone, message, canonical append/drop and command-origin wide bind/cached-summary/ordinary-follower paths against frozen A and **two separately compiled original VALID old-work controls**. Singleton transient entry Vec and extra uniform wide inspection are absent without downstream equivalent replacement. The exact original saved control patches/hashes were applied separately in the SAME B tree/target and all2609 normal bytes/sizes/modes restored after each; original frozen canonical B hash remained unchanged. Function disappearance, isolated offsets, source and cfg(test) visits alone were not used as machine proof. No dynamic allocation count, frequency, exclusive-time or speed gain is inferred from static removal.

Canonical growth/first-piece allocation, payload cloning, Arc ownership, spare-capacity shrink, genuine generic/mixed passes, fixed copies/register pressure and real application/receipt work remain charged. Only authorized logical capsule `Protocol(Box<Message>)` introduces its allocation/value-clone cost. No CPU5 scalar304 criterion/old-size requirement, unsafe/unrelated box/padding/lint/cap/driver/queue/wire/WAL/Noop/receipt/notice/volatile-turn/borrowed-summary change occurred.

After exact normal restoration/hash, the **original CPU1 R3 two-file overlay** was used, not a CPU4 adapter: original patch SHA`8e274bdce7503c4f8d75263d4a43cc503e18bc0fb610ccf7950ab8229d78f185`; driver addition SHA`c2b3bf066abcc02419b0d2c485ffddbacad4e4c612eadcd527b7b2c3b2b172f0`. Diagnostic engine SHA`2dcf17213843236b8928e00053794ef760b1c613b48636973debb45cfeb7e3ba`, Actor SHA`56778c27225a8641530e8c23b6456f1cacb09ff6b6c54faba3ca945e56a522aa`; other2607 inputs unchanged before/after build and every scored child. Original normal Actor/engine identities match A before overlay. Frozen normal canonical B was never replaced by diagnostic B.

Exactly **2 <=2s native metadata controls** passed: actual sleep with misleading argv0 accepted; actual sleep masquerading as diagnostic path rejected ONLY `actual_path_mismatch`, normal rc0/join. Exactly **6 B Rust controls** used the same original score path at widths1/231/515,3 measured rounds/chunks1/4/4 and1024 warmup rounds. Three healthy actual Rust rc0/allten PASS; three `CPU1_OMIT_PUBLICATION` actual Rust rc1 with ONLY `reported_frontiers` false while actual IDs/counts/all3core/application/error/fence checks passed. Control CPU discarded. See stage0b.gate.json and raw mac-*stdout/stderr/native/result files.

## Frozen collected artifacts

| Artifact | Bytes | SHA256 |
| --- | ---: | --- |
| canonical-B.bin | 4115216 | `206e1a5771cee811761187ea599819749e36b6cd01a4bc1f43bd071ea32a92da` |
| valid-old-singleton-vec.bin | 4115216 | `8ea4d027baba45d004afacdaf2856e7a1a1ccde0ae449197b15bec8a24c31664` |
| valid-old-separate-wide.bin | 4115200 | `0e93121a38a48749ad31c523f1db57d2f4501b8c5eec1beb192199283ad8c007` |
| mac-diagnostic-B.bin | 4193136 | `bef1c35b2944566348465607a3f941756e419363797ec84bbc27656956fed75f` |

Full linked symbols/imports and actual bounded `*-range-*` assembly are hashed in stage0a.gate.json. Initially empty symbol-selection `*-asm-*` artifacts are retained and excluded from proof. Original saved patch, build launch/settings/context, source before/control/restoration metadata and compiled artifacts are in the collected inventory.

## Transport, source and historical limits

R2 resumed the existing qualification exactly once after R1's classified PREBUILD transport failure; R1 raw/HOLD remain immutable. R1 had all2609 bytes/sizes match but2605 headers664 vs644 and four775 vs755. No Rust build/test, metadata control, timing or scored child ran there, so no failed scored child was replaced. Its missing whole remote packet hash remains absent.

Parent mode helper SHA`13af6eea4fcbe9e0ca5fbb94435bc3ad8f39a6f1d23184d97aad9350cb55eac0` and finite positive644/755/old-header-negative controls were inspected and preserved unchanged. R2 writer uses that SAME `source_member` at TarInfo emission, independently verifies the header, retains actual pinned Git bytes/path/size/exec classification; receiver still checks ALL2609 actual extracted bytes/sizes/modes BEFORE build. No umask/ACL/Git configuration or failed-tree chmod acceptance workaround. Only normal creation permissions for own new extracted files. Single R2 packet **55,971,840 bytes SHA`f72b0108ff78d3354ec455fb7c11429ae5d9434f3b9098d60433acba1a23b6a6`**, independently matching sender and whole consumed receiver; Git/SSH0/0. Writer/receiver/helper/controller diffs and hashes preserved.

Exact manifest SHA`588bb969a10af790e09ef9e1e174884251573bdc9378b09320db475c8d89b6d0` and patch SHA`03a63974b95de869beee0ebcb3a801df6e185f17468b18afd6b008ba9f3c5793`: all2609 actual Git build/include/TLS closure inputs and all42 changed paths were independently checked for membership/mode/size/bytes/hash. The63336 archived docs were not rehashed. Full design/builder/R2capsule/R3observer releases/full R3 report/terminal/matrix/source audits/policies/current memory were read; selected read/hash anchors and independent verification records remain. Source comments A5633159800/B5633193885 approve exact B without P1/P2/materialP3; they are COMMENT evidence, **not formal GitHub APPROVED, performance or full5 approval**.

Historical CPU6 builder R2 missing-PID ProcessLookupError cause stays UNKNOWN/HOLD. R3 metadata fault controls validate the observer, not that old causality. R3 builder1377 focused PASS EVENTS (not unique tests)/2ignored, seven strict checks and actual hook remain historical evidence, not newly rerun checks here. Two old VALID work controls remain distinct from13 incorrectly compiled mutant kills/restorations; first retained-charge survived a dependent cached oracle before independent raw-ledger checking killed both (all raw preserved). All CPU1–5 rejected sources stay excluded. Default/allfeature/checkpoint/snapshot/native/durable/Counter/IDs/quorum/read/lifecycle/independent-fleet/ship/department/no-ancestor-per-write contracts remain source-audit/R3 evidence. No allocator/parking/noise explanation is assigned to this CPU result.

## Resource observation and cleanup

Actual stock guard SHA`649f0e53f3cc3b3f9456feb1f600fd173131e74b75a50bb45843738a9075abd9` ran before required phases/builds/transfer/controls/every measurement/sweep/final; final actual Mac guard PASS, final child/observer/outer0/0/0. Linux owned work8–9/nice19; Mac nice19 with no affinity claim. One observer thread, DEVNULL/preexec; native Darwin PID/path/PPID/birth/nice captured immediately after Popen before helpers, fixed ABI136, no visibility guess/delay/barrier/wrapper/reap-before-wait4/retry. Original score/capture/cadence/predicate ASTs unchanged.

Before metadata/Rust controls, own failed-thread-query handling was tightened to require two native ESRCH returns or live valid native identity plus differing recorded start; raw operation/PID/start/error is saved before assertion. Earlier build/focus/control observation used the original qualified R5 PS absence logic; that early visibility qualification remains, not retroactively certified. Mac sampled process rows3300, thread-query events3300, raw thread-output rows4865, sampled nice violations0. Failed thread queries1052, typed native absence proofs539, early PS absence proofs0, start-reuse proofs0. Linux sampled thread rows31907; fatal observer errors/recorded resource violations0. See resource-observations.json, phase-outcomes.json and raw events for scope.

Finite setup/read faults are retained: inspect.py stdlib shadow before SSH; nonexistent Mac objdump before child spawn; empty symbol-name assembly selection; local resumed-reader argument/path errors; mistaken local collector argument selecting nonexistent270.py before SSH; local sweep parser initially assumed every receipt had a child despite the preserved no-spawn objdump fault; final Git-output parser initially included the preexec identity row; local final report parser initially expected negative-control JSON on stdout rather than the original stderr path. Focused corrections preserved every failure. None replaced a failed Rust/control/scored child or modified product/scorer inputs. Child/observer/outer outcomes are separate in phase-outcomes.json; standalone reader/parser faults remain in their named bootstrap receipts.

Before deletion, **343 collected files** and **4530 source/artifact/target inventory rows** were locally hash-verified, including binaries/raw/control/source/restoration/controller diffs/protocol. Source normal2609 restored. Immutable reused files/parent helpers unchanged. Before-sweep-files.json hashes closed local evidence; SHA256SUMS seals final artifacts with active sealer/self exclusions stated below.

Own combined peak734961664 bytes <20GiB; global loop peak11301515264 <40GiB, stock topdir cap3 unchanged. Sweep reverified the full inventory immediately before removing ONLY `/tmp/voteboat-claude-loop/pillar1-cpu6-bd-r2-20261023/base-0bf9271e`, `candidate-bd579edd` (including its shared target) and the empty owned parent. No A source/target existed. No foreign roots/tools/cache/ports/processes were changed. Native exact-identity closure before delete1584; final after-sweep closure1597 known Mac IDs; direct observer helpers use exact wait4 and final SSH is joined. Final Linux audit closed470 known process identities and390 thread identities (only the current final auditor was allowed live). Actual root absence and collected cleanup receipts are hashed.

No detached own workload remains. Initial/tool/T3/model, short-lived/reparented/inaccessible and unknown-process visibility gaps remain; this is not a global or whole-task historical clearance. R1 historical44Mac/73Linux closures remain separate. Linux writes were confined to this durable ignored evidence directory; shared parent target was deliberately preserved under the explicit test-agent restriction. No own Linux source/build/worktree/scratch outside this evidence directory was created. Parent owns further disposition; **full5 NOT RELEASED**.

## Navigation

- `terminal-summary.json`, `primary-result.json`: gate and all exact ratios.
- `stage0a.gate.json`, `STAGE0A.md`, `*-range-*`: static ordinary-path evidence.
- `stage0b.gate.json`, `*.observed-*`, `mac-*stdout/stderr/native/result`: actual control/scored receipts and raw documents.
- `terminal-mac-inventory.json`, `collection-verified.json`, `cleanup-before-delete.json`, `final-mac-closure.json`, `final-audit.json`: collection, restoration, deletion and known-identity closure.
- `*.from-r1.diff`, `*.from-r5.diff`, `mac-common.final.diff`, `observer-contract.json`, `driver-contract-audit.json`, `before-sweep-files.json`: controller provenance and hashes.
- `phase-outcomes.json`, `resource-observations.json`, raw logs/bootstrap receipts: actual outcomes and retained setup faults.
- `PR30-COMMENT.md`, `publication.json`, `linked-pr-inventory.json`: the single final PR comment and final linking receipt once published.

SHA256SUMS excludes itself, final-file-inventory.json and the active seal bootstrap receipt; these cannot self-hash. The final-file inventory records those exclusions. All closed source/assembly/control/binary/protocol/raw/report/comment/publication evidence is sealed.

Final publication: [https://github.com/KarlLivesey/VoteBoat/pull/30#issuecomment-6373616989](https://github.com/KarlLivesey/VoteBoat/pull/30#issuecomment-6373616989). Exactly one comment attempt; final link inventory confirms open draft PR30, watching false. The final local sealer independently closed 482 known process identities except its own single-thread nice19/core8–9 process; it spawned no child.
