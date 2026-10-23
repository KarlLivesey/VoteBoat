# PR26 exact D7 CPU qualification — stage1 CPU gate failed

All **18 fresh diagnostic children / nine complete pairs passed every functional guard**. Width515 failed the predeclared CPU condition: median whole-child B/A1.0069536734254976 (+0.695367%), one CPU win of three. Therefore **canonical held:0/64 children,0/32 pairs**, all four cells unqualified. No replacement schedule, averaging across widths, unchanged retry series, causal repair, tolerance, parking explanation or full suite was introduced. This is a finite performance disposition, not source qualification, feature/full-gate acceptance, PR rejection/merge approval or an integration decision. Parent remains sole integration owner.

A source `0bf9271ecb5a34c4089c6bb10381303232624170`; B exact `d7b4ec739b4b9dde8d0b0d00c59608ce3fc2c6ff`. Bbase03f3ffd9 is production-identical to A (independent Git diff audit). No CPU1 candidate source, production/main/memory/design-pack edit, Linux build/source checkout/worktree, agent, network/WAL benchmark or roadmap work. PR26 linked, no watcher.

## Predeclared schedule and all paired results

Saved SCHEMA.md, protocol.json and provenance-before.json precede remote operations. Three pairs per width in fixed AB/BA/AB order; each round visits widths1,231,515. Specs200000,1,1 /6000,231,4 /3000,515,4;1024 warmup rounds; default retention, upstream-default envelopes, attributionfalse, one server worker for every diagnostic child. Current raw observations are newly paired; no old timing result is reused.

- Width1: median whole-child CPU change -1.097238%; median internal elapsed change -1.054983%; CPU wins 3/3; width gate PASS.
  Pair1 (AB): A2521.090000000, B2485.750000000 whole-child CPU ns/measured original; B/A0.985982253708 (-1.401775%); internal elapsed B/A0.984689263904.
  Pair2 (BA): A2508.115000000, B2480.595000000 whole-child CPU ns/measured original; B/A0.989027616357 (-1.097238%); internal elapsed B/A0.989450171615.
  Pair3 (AB): A2506.220000000, B2489.605000000 whole-child CPU ns/measured original; B/A0.993370494210 (-0.662951%); internal elapsed B/A0.993290781568.

- Width231: median whole-child CPU change -2.143375%; median internal elapsed change -2.579145%; CPU wins 2/3; width gate PASS.
  Pair1 (AB): A111.353535354, B108.966810967 whole-child CPU ns/measured original; B/A0.978566245076 (-2.143375%); internal elapsed B/A0.974208552501.
  Pair2 (BA): A111.522366522, B108.718614719 whole-child CPU ns/measured original; B/A0.974859287054 (-2.514071%); internal elapsed B/A0.970486936793.
  Pair3 (AB): A108.694083694, B108.909812410 whole-child CPU ns/measured original; B/A1.001984732824 (+0.198473%); internal elapsed B/A1.002903452234.

- Width515: median whole-child CPU change +0.695367%; median internal elapsed change +0.425918%; CPU wins 1/3; width gate FAIL.
  Pair1 (AB): A118.033656958, B120.699029126 whole-child CPU ns/measured original; B/A1.022581458857 (+2.258146%); internal elapsed B/A1.023723602942.
  Pair2 (BA): A119.673786408, B118.397411003 whole-child CPU ns/measured original; B/A0.989334544825 (-1.066546%); internal elapsed B/A0.990603455039.
  Pair3 (AB): A117.374110032, B118.190291262 whole-child CPU ns/measured original; B/A1.006953673425 (+0.695367%); internal elapsed B/A1.004259183609.

CPU medians are medians of paired ratios. Width1 signs are −/−/−; width231 −/−/+; width515 +/−/+. Width515 fails both strict median<1 and >=2/3 wins. Other widths cannot offset this condition. All pairs are complete; unpaired measured children0. Full precision and original per-child user/system CPU appear in driver-pairs.json, rows-partial.json and driver-audit-all18.json.

Whole-child CPU is direct-binary wait4 ru_utime+ru_stime across all child threads, including startup/election, warmup, validation/serialization and diagnostic process exit. Divide by actual measured completed originals; no warmup CPU is subtracted. Internal elapsed starts after1024 warmup rounds and excludes warmup; it uses the same measured-original denominator. driver-audit-all18.json also records the internal completion rate calculated from actual measured count/elapsed. This is not canonical throughput. The synchronous adapter has no request-p99 instrument: request p99 and canonical rate/p99 guards are unavailable, not green. Canonical attributionfalse would expose applied-prefix + original completion contract + owned JSON exit, without fabricated exact commit counters; no canonical child ran.

The diagnostic constructor calls process::exit before normal owned shutdown. Whole-child CPU includes that actual early-exit boundary, not canonical drop/shutdown qualification. Normal canonical lifecycle remains unrun. No allocation/instruction/server-only CPU instrument was applied. Context-switch rusage fields are retained raw but no causal reduction is claimed. Folded Core work and persistence_units are not turned into a CPU saving because a PersistHelper phase disappears; cfg(test) local_units is absent in release.

## Actual functional evidence

Every child passes original submitted/replied counts and IDs including warmup, unreplied0, wrong_replyfalse, all-three actual core commit/application/published frontiers, unfenced state, propagated_errornull, actual checkpoints, leader original proposals, all settings, adapter conjuncts and publication enabled. All positive stderr is empty, direct rc0, no timeout or competing-process event. The expected values are guards on actual observations, not assignments to frontiers.

- Width1: measured completed originals200000; warmup1024; total original replies201024; all three core/application/published frontiers[201025, 201025, 201025]; checkpoint counts[40, 40, 40]; final retained entries[2025, 2025, 2025]. All six children at this width agree on these actual outcomes.
- Width231: measured completed originals1386000; warmup236544; total original replies1622544; all three core/application/published frontiers[1622545, 1622545, 1622545]; checkpoint counts[319, 319, 319]; final retained entries[2386, 2386, 2386]. All six children at this width agree on these actual outcomes.
- Width515: measured completed originals1545000; warmup527360; total original replies2072360; all three core/application/published frontiers[2072361, 2072361, 2072361]; checkpoint counts[402, 402, 402]; final retained entries[3060, 3060, 3060]. All six children at this width agree on these actual outcomes.

B Actor.step and Actor.requests invoke the actual new public step_volatile_into/propose_entries_volatile_into seam. The unchanged R3 addition calls these normal Actor methods and mirrors maintain -> retained-stat -> actual application.applied publication. It does not install a test-only execution route. Builder public focused histories and default-retention/snapshot catch-up evidence remain independently recorded in pinned validation/performance/pillar1-cpu2/CPU2-DESIGN.md; those are builder results, not full-suite or feature-preservation approval by this runner. No capability was switched off for this score.

## Frozen inputs and builds

Frozen diagnosticA SHA256 `c05c3c718ab6d9ac5a72f2e04cfce29fb35f6618a76b3e0debdda10ff7f5fa6a`; frozen canonicalA `0c6998bc63614f43faaba36dfff64f9291b7a14336853f4d901c4068c2c69208` was verified as a separate, unused scope. Both original artifacts remain untouched. R3 build-A.after/overlay identities and cpu1-perf canonical freeze/source/toolchain/env receipts are referenced by hashes. Before adoption, Mac14,5 Apple M2 Max/12 CPUs and Rust stable1.98.0(88d9e12ae)/Cargo1.98.0 matched the frozen environment; actual old launch hardware/flags were additionally crosschecked. No A source copy or rebuild.

Candidate diagnosticB SHA256 `4c82488a265a7151e5c6aba2002b7fbbc942bb48a4a1d04436c0941150d90ae8`, retained as raw/artifacts/candidate-diagnostic.bin. It is D7 plus exactly two matched measurement-overlay files. Addition SHA256 `c2b3bf066abcc02419b0d2c485ffddbacad4e4c612eadcd527b7b2c3b2b172f0`; constructor-hook SHA256 `15ecde45969ff79a270ddf175ae151201fc207a534c44b79b35e4d13df57c517`. Baseline transform hashes independently reproduce the already compiled R3 overlay exactly. B original/overlay per-file hashes, patch and all1869 pinned packet input identities are saved. Only this diagnosticB artifact ran; canonicalB was not built.

The small packet reads pinned D7 Git blobs directly; no Linux extraction. It includes root Cargo/locks/config/toolchain if present/src, full matched workspace, fixture include closure and declared example inputs. This is an actual build-input manifest, explicitly not a whole Git archive identity; validation history/binaries/unrelated docs are excluded. Root/matched locks match both frozen A scopes. Same own candidate target only; cargo +stable build --release --locked --offline --manifest-path benchmarks/matched/Cargo.toml, jobs4/incremental0/release debug0/RUSTFLAGS empty, original LTO/profile. No redundant root executable build, new dependency or per-revision cache.

Initial packet omitted the nine Cargo-declared example entry files. Cargo exited101 while parsing manifests, before Rust compilation or any measured child. Exact failure/first manifest/controller/raw/cleanup are immutable in packet-closure-initial/. The brief explicitly authorized one missing-compile-asset closure fix from the same pinned Git object:48 examples/ files were added, every original input unchanged. Both roots were removed after the initial failure, then recreated under the same authorized names solely for that correction; no additional source root/target was retained. PACKET-CLOSURE-FIX.md and source-and-closure-audit.json record this. No measured schedule was repeated and no second correction occurred.

## Existing negative control and immutable history

R3 control reused, never rerun: actual rc1, failed_conjuncts=[reported_frontiers], reported[0,0,0], actual core/application[201025,201025,201025], all201024 original replies/IDs correct, no fencing/error/unreplied, checkpoints40 each. Document SHA256 `fb3b965fbbde6035b56dd5dade80df412360c58efca60488e2f3863b27c46f15`; actual stderr `e2465cdfef75978f50e89b591902e0d6df229b4ba94330e50aaf979b80f08af0`; direct result `04c0032ffc7e94c47541a3bdba3ce2199bf1392b2023b1ac2d54218eb0169571`. This qualifies telemetry-only failure for that control. R1/R2 failures and R3/R4 original evidence were not edited, repaired or relabelled.

## Context, resources and terminal cleanup

Sampled Mac load1 range2.366699–3.394531; full load/power/thermal/process/native-thread observations retained. Nonzero load is context, not a failure or a causal explanation. No obsolete load<1 gate; competing benchmark/build quiet check bounded15s, no competing event observed. No Mac affinity/frequency assertion. Linux controlled descendants: 773 sampled task records, zero live nice19/cores8-9 violations; vanished observations0. Mac 105 sampled descendant process records, zero observed nice violations. Sampling is not continuous proof; initial read-only tool shells preceded explicit containment. Harness/ancestors were never retuned/signalled, and common cwd was never treated as ownership.

Peak sampled own Mac bytes462659584 (<20GiB); global loop<=40GiB/topdirs<=3/free>=500GiB unchanged. Corrected stock guard SHA256649f0e53f3cc3b3f9456feb1f600fd173131e74b75a50bb45843738a9075abd9 verified before each remote setup/overlay/build/measurement/collection/cleanup and final audit, with actual Mac SSH checks and no cap override. R4 observer records owned task-directory FileNotFoundError/ProcessLookupError disappearance and still rejects sampled live envelope violations; none occurred in these saved samples.

Collection verified every file in the remote freeze before pruning only redundant collected copies of baseline binaries; original frozen A artifacts and candidate diagnostic artifact remain. compact-artifacts.json identifies every such removed duplicate and original retained hash. All stdout JSON/stderr/argv/environment/direct rc/wait4 rusage/source/binary identities/contexts/raw stats/guards/controllers/overlay are saved.

Both exact owned Mac roots, their targets/scripts and owned empty parent are absent. The post-cleanup remote process/lsof-cwd audit reports no owned root job or deleted cwd. Linux exact-runner/deleted-cwd audit reports no known owned job; no Linux scratch/build/worktree existed. Foreign builder/reviewer roots, shared test ports, unrelated jobs and uncertain harness processes were untouched. Controller sessions have returned terminal exits; no watcher/polling/nested run remains. Only designated safe reports/controllers/compact artifacts remain locally.

Final full stock guard:

```
free_gb=2867 (min 500)
target_gb=3 (cap 150) cargo_build_dirs=0
live_worktrees=1 (cap 5) prunable=0 worktree_target_gb=0 (cap 120)
linux_tmp_voteboat_dirs=0 (cap 300) linux_tmp_gb=0 (cap 10)
mac_loop_gb=10 (cap 40) mac_voteboat_dirs=2 (cap 3)
OK: disk within budget
```

## Evidence map

- SCHEMA.md, protocol.json, provenance-before.json: pre-operation schema, fixed schedule and provenance.
- summary.json, stage1-decision.json, driver-pairs.json, driver-audit-all18.json, driver-drift.json: full paired values, signs, outcome audits and descriptive first-to-last drift.
- raw/r*-w*-{A,B}.{stdout,stderr,launch.json,result.json,before.json,after.json,checks.json,document.json,row.json}: every fresh measured child.
- raw/build-diagnostic.*, raw/overlay-B.patch, overlay-plan.json, input-B.json, packet.json, include-asset-closure.json, source-and-closure-audit.json: input/build/overlay and one authorized closure correction.
- sources-A.json, baseline-frozen-hardware-flags-crosscheck.json, negative-control-reference.json: reused artifact/control provenance.
- resource-audit.json, collected-freeze-integrity.json, cleanup.controller.*, final-mac-audit.controller.*, final-linux-audit.json, terminal-audit.json, FINAL-FULL-STOCK-guard.controller.*: resources/integrity/terminal cleanup.

No canonical rows/medians/rate/p99 or exact canonical commit counters are claimed. No source qualification/full-gate/merge approval. This finite experiment ends here.

One durable PR26 report comment posted: https://github.com/KarlLivesey/VoteBoat/pull/26#issuecomment-6355434674 . PR linked; app reports watching=false. Terminal post-comment full guard passed; receipt TASK-TERMINAL-guard.controller.stdout.
