# PR27 CPU3 exact-head finite experiment — mixed canonical result; resource qualification on hold

Exact A `0bf9271ecb5a34c4089c6bb10381303232624170` versus B `6c7cb6d7e535a79265b41ee8e51679616bb1b85d`; B base `c60e6ba4514da39026d2b51195472dffe02d0a40` is production-identical A. PR https://github.com/KarlLivesey/VoteBoat/pull/27 linked immediately, no watcher. Parent remains sole integration/main owner.

**Measurement complete:** 18/18 original fixed-driver children, nine complete pairs, all functional/input guards passed; every width passed the predeclared strict CPU gate. Proceeded to the preauthorised canonical stage: 64/64 children, 32 complete pairs, all functional/input guards passed. No measured failure, unpaired child, replacement, retry, reweighting, changed workload or additional explanatory/profiling run.

**Result is mixed:** bulk16 and bulk1 median paired CPU improve; singleton CPU and rate regress. Bulk1 median request p99 also increases. No numeric tolerance, parking explanation, allocation/instruction claim, final ACCEPT/merge/CPU/fullqualification decision is introduced. Parent decides from the predeclared gates and remaining independent source/full-suite requirements.

**Resource qualification HOLD:** after the entire campaign, verified collection and Mac cleanup, the independent reporting auditor failed its final `all(t["ok"] for t in audit(os.getpid()))` assertion. That transient record was not emitted before failure; no cause or exact offending PID/thread is established. Preserve `audit.py`, `audit-resource-failure-original.py`, `audit-resource-failure.json`. A separate read-only snapshot is conforming but does not erase the failure. All 2,218 recorded campaign Linux task observations conform to absolute nice19/cores8–9, with zero recorded live campaign violations or vanished observations. These are campaign observations, not an overall resource-clearance claim. No dependent build/measurement launched after the report-audit failure; only evidence completion/reporting/cleanup proceeded. `AUDIT.json` and `DISPOSITION.json` retain the hold.

## Stage 1: original fixed driver

Frozen SCHEMA.md, protocol.json and provenance-before.json precede remote source changes. Exactly AB/BA/AB, each round width1 then231 then515. Specs `200000,1,1` / `6000,231,4` / `3000,515,4`; 1024 warmup rounds, default retention, upstream-default envelopes, attributionfalse, one server worker, timeout600. Actual measured originals per child200000/1386000/1545000. Sustained CPU3 profiles have different long counts and were not used for qualification.

- Width1: median paired whole-child CPU B/A `0.979099258064581` (-2.090074%); internal elapsed `0.979824818038196` (-2.017518%); CPUwins3/3; declared width gatePASS.
  Pair1: A2503.350000000000, B2452.710000000000 CPU ns/actual measured original; B/A `0.979771106716999` (-2.022889%), internal B/A `0.981226706399367`.
  Pair2: A2501.860000000000, B2444.135000000000 CPU ns/actual measured original; B/A `0.976927166188356` (-2.307283%), internal B/A `0.978087767494812`.
  Pair3: A2504.934999999999, B2452.580000000000 CPU ns/actual measured original; B/A `0.979099258064581` (-2.090074%), internal B/A `0.979824818038196`.
- Width231: median paired whole-child CPU B/A `0.961408940846450` (-3.859106%); internal elapsed `0.953884839262168` (-4.611516%); CPUwins3/3; declared width gatePASS.
  Pair1: A109.054112554113, B104.845598845599 CPU ns/actual measured original; B/A `0.961408940846450` (-3.859106%), internal B/A `0.953884839262168`.
  Pair2: A112.433621933622, B104.139971139971 CPU ns/actual measured original; B/A `0.926235136331842` (-7.376486%), internal B/A `0.920620433908113`.
  Pair3: A108.683261183261, B106.661616161616 CPU ns/actual measured original; B/A `0.981398745311515` (-1.860125%), internal B/A `0.982384196935667`.
- Width515: median paired whole-child CPU B/A `0.972478834112356` (-2.752117%); internal elapsed `0.968976017876603` (-3.102398%); CPUwins2/3; declared width gatePASS.
  Pair1: A117.690614886731, B114.337216828479 CPU ns/actual measured original; B/A `0.971506665493422` (-2.849333%), internal B/A `0.968976017876603`.
  Pair2: A117.426537216828, B114.194822006472 CPU ns/actual measured original; B/A `0.972478834112356` (-2.752117%), internal B/A `0.968696557006662`.
  Pair3: A117.427184466019, B117.814886731392 CPU ns/actual measured original; B/A `1.003301639796059` (+0.330164%), internal B/A `1.006959319075180`.

Medians are medians of paired ratios. Strict median<1 and>=2/3 wins holds separately at every width; no width offsets another. Width515 pair3 is a retained loss. Every original submitted/replied ID and count, unreplied0, wrongreplyfalse, all3 actual corecommit/application/published frontiers, unfenced state, propagated-errornone, actual checkpoints, retained suffix bounds, settings and all ten named adapter conjuncts passed individually. `driver-audit-all18.json` records the observed values and every guard. Expected values are comparisons, never assigned frontiers.

- Width1: observed total warmup+measured original replies per child201024; actual all3 core/application/published final frontier[201025, 201025, 201025]; checkpoint counts per replica[40, 40, 40]; retained entries per replica[2025, 2025, 2025].
- Width231: observed total warmup+measured original replies per child1622544; actual all3 core/application/published final frontier[1622545, 1622545, 1622545]; checkpoint counts per replica[319, 319, 319]; retained entries per replica[2386, 2386, 2386].
- Width515: observed total warmup+measured original replies per child2072360; actual all3 core/application/published final frontier[2072361, 2072361, 2072361]; checkpoint counts per replica[402, 402, 402]; retained entries per replica[3060, 3060, 3060].

Whole-child CPU = direct binary wait4 ru_utime+ru_stime across all child threads, including startup/election, warmup, validation/serialization and the original diagnostic early exit, divided by ACTUAL measured completed originals. No warmup CPU subtraction. Internal elapsed begins after1024warmup rounds and is separately reported. Driver request-p99 is unavailable, not green. The original constructor process::exit is before normal owned shutdown; no lifecycle/canonical qualification follows from driver success. No PhasePersist helper change or vanished phase is credited as savings.

## Stage 2: unchanged canonical campaign

Both B overlay files restored byte-for-byte to pinned6c; all804 source/config/assets paths and modes verified; canonical rebuilt only in the same B normal target. Canonical B SHA frozen before canonical launches; no driver binary used. Exactly eight alternating AB/BA rounds, each bulk16/single16/bulk1/single1, 64 children. Bulk4096clients/batch4/requested4000000/ACTUAL3997696; singleton1client/batch1/131072; workers16or1 as named.1024warmup originals/defaultretention/upstreamdefault/attributionfalse/timeout600 and pinned compiler/profile/flags/locks/hardware per pair.

- bulk16: median paired CPU `0.986965809286923` (-1.303419%), rate `1.009070031541842` (+0.907003%), requestp99 `0.992945558494372` (-0.705444%); CPUwins7/8, ratewins7/8, lowerp994/8.
  Pair1 AB: CPU `1.001424392664736`, rate `1.007430138836467`, requestp99 `0.983426256521405` B/A.
  Pair2 BA: CPU `0.993139886558358`, rate `1.001482357826174`, requestp99 `1.006415669205658` B/A.
  Pair3 AB: CPU `0.982501030504109`, rate `1.022908547719590`, requestp99 `0.959864178323339` B/A.
  Pair4 BA: CPU `0.983890842460433`, rate `1.010709924247218`, requestp99 `1.000209565461823` B/A.
  Pair5 AB: CPU `0.988881251338379`, rate `0.999160604160671`, requestp99 `1.030730670316110` B/A.
  Pair6 BA: CPU `0.982132671686060`, rate `1.022191259317882`, requestp99 `0.965131407326542` B/A.
  Pair7 AB: CPU `0.988124100310630`, rate `1.001780922995076`, requestp99 `0.985681551526921` B/A.
  Pair8 BA: CPU `0.985807518263217`, rate `1.017036478222379`, requestp99 `1.046864231564438` B/A.
- single16: median paired CPU `1.004878948611872` (+0.487895%), rate `0.993676081621712` (-0.632392%), requestp99 `1.005405498041944` (+0.540550%); CPUwins3/8, ratewins2/8, lowerp994/8.
  Pair1 AB: CPU `0.994921747540363`, rate `1.004282133941349`, requestp99 `0.989266701013223` B/A.
  Pair2 BA: CPU `1.008840699254139`, rate `0.995166999098576`, requestp99 `0.974445989042440` B/A.
  Pair3 AB: CPU `0.992844175891364`, rate `1.004733363371393`, requestp99 `0.965490131170605` B/A.
  Pair4 BA: CPU `1.011070650860313`, rate `0.987806800661713`, requestp99 `1.021544295070665` B/A.
  Pair5 AB: CPU `1.011850716125958`, rate `0.981751491923328`, requestp99 `1.038476062602274` B/A.
  Pair6 BA: CPU `0.998431389056843`, rate `0.992185164144849`, requestp99 `1.021660024259227` B/A.
  Pair7 AB: CPU `1.000917197969604`, rate `0.998399549842930`, requestp99 `0.987811518324607` B/A.
  Pair8 BA: CPU `1.011995135125738`, rate `0.990571346461890`, requestp99 `1.038951660861439` B/A.
- bulk1: median paired CPU `0.981589089057233` (-1.841091%), rate `1.025389580314867` (+2.538958%), requestp99 `1.011463223264593` (+1.146322%); CPUwins7/8, ratewins6/8, lowerp993/8.
  Pair1 AB: CPU `0.977540344245950`, rate `1.034126216287294`, requestp99 `1.000589096415952` B/A.
  Pair2 BA: CPU `1.020185381426981`, rate `0.987161374887309`, requestp99 `1.031355297826199` B/A.
  Pair3 AB: CPU `0.982235273165983`, rate `1.026439077074808`, requestp99 `0.955543150936467` B/A.
  Pair4 BA: CPU `0.952159643378457`, rate `1.042421359547160`, requestp99 `1.059180208585981` B/A.
  Pair5 AB: CPU `0.958849378672774`, rate `1.040096863373121`, requestp99 `0.977040265620284` B/A.
  Pair6 BA: CPU `0.995586633805239`, rate `0.999786018082011`, requestp99 `1.022337350113235` B/A.
  Pair7 AB: CPU `0.980942904948483`, rate `1.024340083554926`, requestp99 `0.941547967094751` B/A.
  Pair8 BA: CPU `0.983437350927569`, rate `1.019132980404373`, requestp99 `1.163631873123774` B/A.
- single1: median paired CPU `1.004974904474225` (+0.497490%), rate `0.988793025293817` (-1.120697%), requestp99 `0.991650000000000` (-0.835000%); CPUwins2/8, ratewins1/8, lowerp996/8.
  Pair1 AB: CPU `1.023538688193956`, rate `0.981430174512479`, requestp99 `1.052509090909091` B/A.
  Pair2 BA: CPU `1.004782614382504`, rate `0.986303863992321`, requestp99 `0.990008127360520` B/A.
  Pair3 AB: CPU `1.008438265970580`, rate `0.980882519128365`, requestp99 `0.965555771472989` B/A.
  Pair4 BA: CPU `0.991488492441781`, rate `1.008165352485673`, requestp99 `0.994128337760380` B/A.
  Pair5 AB: CPU `0.998518395607971`, rate `0.999083966282942`, requestp99 `0.932658492538732` B/A.
  Pair6 BA: CPU `1.002081800905686`, rate `0.998461583476564`, requestp99 `0.991650000000000` B/A.
  Pair7 AB: CPU `1.005167194565945`, rate `0.991282186595314`, requestp99 `0.991650000000000` B/A.
  Pair8 BA: CPU `1.022316181379957`, rate `0.973887807122171`, requestp99 `1.040601723382503` B/A.

All paired ratios/signs/counts and absolute per-child values remain in summary.json, canonical-partial.json and canonical-audit-all64.json. Singleton16 rate is lower6/8 pairs; singleton1 lower7/8. CPU is higher5/8 and6/8 respectively. Bulk1 p99 is higher5/8; its final pair p99 B/A1.163631873123774. These costs are retained without tolerance or cross-cell averaging.

Whole canonical CPU includes startup/warmup/application/validation/aggregation/serialization and normal owned shutdown. Actual unchanged JSON rate covers ready-client barrier through joins, excluding warmup/flatten/sort/follower verification/shutdown; requestp99 uses actual sorted request samples. Rate/requestp99 are not whole-child CPU timing equivalents. No server-only CPU, profiler CPU, allocation or instruction counter was measured; no profiler launched. Raw rusage context-switch/pagefault/I/O fields remain descriptive, not causes.

Canonical guards retain original success/failure/count semantics: all original IDs awaited and matched by Client::write; aggregate checks per-client original and request/sample counts; all3 application prefixes exactly equal ACTUALcompleted+1025 (bulk3998721/single132097); all3 checkpoints; proposal counts ACTUALcompleted+1024; normal owned shutdown succeeds before JSON/exit0. Source references/hashes in canonical-source-boundaries.json: voteboat_engine.rs verify182/shutdown195/Client::write215, workload.rs run+aggregate, main.rs campaign/cleanup146–173. Attributionfalse JSON has no exact core commit-counter field: this is applied-prefix + original completion contract + normal owned exit, not fabricated live commit telemetry. Actual public Counter behavior is separately builder-focused evidence, not a Counter-specific speed claim from this Noop/Actor campaign.

## Provenance and build-input closure

Builder manifest1535paths SHA256 `82c51d64f431b4364c742722deda3dfde7e6a307074073b85cb74126eb1a9869` verified. All12 changed paths match pinned Git and saved builder source-final; build-input entries additionally match builder manifest (narrative docs intentionally are not build inputs). Release behavior changes only src/application.rs, matched application.rs and actor.rs. Original pack, shared source/main/memory/hooks and prior CPU1/2 failure evidence are untouched. Six builder mutants/325 focused passes/strict7+actualhook are referenced prior evidence and were not rerun or upgraded to full qualification.

Final partial packet804 files /7068775 uncompressed input bytes /4447791 transferred compressed bytes. Packet SHA256 `6eb187e8a1ed055fa1c25ba1b9924fdadddb0445b43b5dec464f3213b739ad70`; source manifest SHA256 `a2e85fdd3ce874fa6362af1e75101442aa6b302aeb07a9e71415244a77f5441e`. Full source/config/rootCargo+locks/build.rs/full matched workspace source/manifests/locks/assets plus48 complete examples paths and fixtures included upfront; declared manifest targets and literal includes checked. Matched validation history, unrelated docs/binaries and whole Git history excluded. This is partial build-input closure, never whole-archive identity. Git blobs read directly; no Linux extracted source/worktree/build, giant archive or duplicate packet. No missing-asset correction was needed; known CPU2 examples parse omission was not repeated.

Original qualified R3 addition SHA256 `c2b3bf066abcc02419b0d2c485ffddbacad4e4c612eadcd527b7b2c3b2b172f0`; constructor hook `15ecde45969ff79a270ddf175ae151201fc207a534c44b79b35e4d13df57c517`. Both baseline transforms independently reproduce original R3 before/after hashes. B overlay is ONLY that identical addition/hook on exact6c two files, with no contextual anchor adaptation or new driver logic. overlay-plan.json and raw/overlay-B.patch preserve evidence. Public new Actor seam remains actual consumer.

- driver A actual artifact SHA256 `c05c3c718ab6d9ac5a72f2e04cfce29fb35f6618a76b3e0debdda10ff7f5fa6a`.
- driver B actual artifact SHA256 `ec93670d1a45104fec07ce382455613619e55dc9198ccb918ccb77e3752d545d`.
- canonical A actual artifact SHA256 `0c6998bc63614f43faaba36dfff64f9291b7a14336853f4d901c4068c2c69208`.
- canonical B actual artifact SHA256 `228bee4627a06cdb000e28c939de60c70d5f9d7c2d3976eb3b1ac622b9ab1727`.

Original driverA and canonicalA binaries remain untouched in their original parent artifact paths. Original R3 build-after and canonical freeze/source manifests,804 selected-path A comparison (802 existed onA), root/matched locks, original release profile, flags and original build command verified; source/build equivalence is not a new binary-identity claim. Both current A/B runtime observations are fresh. Current Mac14,5/AppleM2Max/12CPU, Ruststable1.98.0 rustc88d9e12ae/Cargo1.98.0, identical fourjobs/incremental0/releasedebug0/emptyRUSTFLAGS/originalthinLTO profile and explicitPATH. Baseline R3 overlay mismatch list is exactly the qualified two files. Immutable telemetry-negativecontrol hashes retained; no negative-control rerun. That historical control failed only published-frontier telemetry while actual replies/core/application/checkpoints passed; all earlier failures stay unchanged.

## Resource context, failures and finite cleanup

Full stock guard SHA256 `649f0e53f3cc3b3f9456feb1f600fd173131e74b75a50bb45843738a9075abd9` unchanged, actual Mac SSH retained, all caps/defaults unchanged.92 experiment/preflight guarded remote operations captured in audit; further report/publication guards separate. Linux explicit controller/direct children use absolute nice19/cores8–9 with R4 race-safe vanished-task handling.2218 recorded campaign task observations,0 recorded violations/vanished observations; the later reporter assertion remains unresolved HOLD. Initial read-only inventory/reads preceded first stockguard/sampler audit despite initial explicit affinity/nice readback. Shell/tool/T3/model/harness and remote interpreter-entry startup not retroactively certified. Foreign/inaccessible processes and unknown CUA hosts2273664/2273680/2273774 remain untouched; no zero-infrastructure claim.

Mac nice19 task-created controller/children with native per-thread scheduling snapshots; no Mac affinity/frequency guarantee. Bounded15s quiet check had no competing benchmark/build; no load<1 gate. Actual observed before-launch load1 range[2.46240234375, 4.37939453125]. Peak own combined budget482398208bytes (<20GiB); max global loop11048951808bytes (<40GiB); min free934173036544bytes (>500GiB). All raw power/thermal/load contexts retained. These drift observations establish no scheduler/parking/thermal cause. Per-cell A/B first-to-last CPU ratios and min/max CPU/rate/p99 are in drift.json.

Preparatory failures retained: outer-shell quoting truncated a Python script before any Mac source change/build/measurement; corrected only report preparation. A metadata assertion used the build-input manifest for narrative docs (KeyError docs/IMPLEMENTATION.md); shell lacking set-e then ran the initial preparation, still before transfer/build/measurement. Original packet identity retained; its exact initial1868-path input metadata reconstructed from pinned blobs and matched original logged SHA8b2983ce4efaa863be59134b183ad966cf199238ff00c583bae186a5adb75276, without duplicate archive. Corrected docs comparison uses existing builder source-final and build manifest only for actual build inputs; set-e prevents dependent command continuation. Final small packet drops only noncompiled matched validation history. No production/driver/workload repair, missing-asset substitution or measured replacement. Postcampaign report-audit failure described above is neither hidden nor green.

Finite cleanup completed immediately after collecting and verifying788 raw files against collection-manifest.json, before report work. Both marker-owned Mac roots `/tmp/voteboat-claude-loop/pillar1-cpu3-6c-20261023/base-0bf9271e` and `candidate-6c7cb6d7`, normal B target, copied binaries/scripts and empty owned parent are removed. Mac ps/cwd audit has no owned-root job/reference; Linux readable current-user cwd audit has no named-root reference. All explicit controllers/children awaited; no known prior owned job remains, with stated foreign/inaccessible/unknown-harness visibility gaps. No signal/retune of foreign/shared infrastructure, conformance worktree, sharedportnamespace or other Mac roots. No retained Linux scratch/source tree; original A artifact files unchanged; only report/raw/candidate artifacts retained in parent-safe target path.

Final experiment full stockguard passed: free2867GiB/root target3GiB/cargobuilddirs0/liveworktrees1/prunable0/worktreetarget0/Linuxscratch0dirs0GiB/Macloop10GiB/topdirs2. Cleanup details cleanup.controller.stdout; rawSHA receipt collection-verified.json; final-linux-process-audit.json; final guard TASK-END-guard.controller.stdout. Additional final publication/end receipts follow.

Parent full gate remains separate: fresh source A+B approvals, ALL5 full suites, exhaustive unskipped normal-debug relay/docs, exact-head strict7 and actual enabled hook plus semantic features. None is waived or claimed complete. No new source patch, dependency, network/storage/roadmap work, subagent, watcher, CI polling or automatic follow-up was introduced.

## Artifact map

- SCHEMA.md/protocol.json/provenance-before.json: predeclared state/data/gates/ownership/source/build evidence.
- input-B.json/include-asset-closure.json/manifest-target-closure.json/changed-input-closure.json/reference-audit.json: pinned closure/provenance.
- raw/: all82 stdout/stderr/argv/env/wait4/results/settings/checks/before-after identities/contexts, builds, overlays, remote controller, actual candidate driver+canonical artifacts under raw/artifacts; raw/collection-manifest.json with verified hashes.
- driver-audit-all18.json/canonical-audit-all64.json/summary.json/drift.json: actual counts/ratios/signs/rusage/drift, no erased losses.
- AUDIT.json/DISPOSITION.json/audit-resource-failure*.json+original.py: explicit late-resource hold and finite experiment status.
- linux_runner.py/mac_controller.py/run.py/prepare.py/driver-body.txt/driver-addition.rs: exact report/controller/overlay inputs; raw controller equals sent one.
- PR-COMMENT.md/PR-COMMENT.json and returned comment receipt: single durable PR27 performance report.

Single durable PR27 result comment: https://github.com/KarlLivesey/VoteBoat/pull/27#issuecomment-6356816510 (posted once, response body verified exact).

Terminal receipt FINAL.json: fresh final Mac parent/roots absent and cwd/target-job checks passed; own Python cache removed; no transfer archives retained; git status short empty=True. FINAL-END full actual-Mac stockguard passed. Original postcampaign resource assertion remains HOLD, not superseded by cleanup checks.
