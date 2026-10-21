# PR23 fair exact-head Mac N0 gate — 21 October 2026

Pinned A `b77fd087b06c5ae76a5a1a25f2cfe3ed6891c339` (qualified production bd714771 plus disposition documentation); pinned B `1c2595364cbe405922695ce9c8728316ecf05741`. This compares the reconciled candidate with the qualified cumulative baseline. Both retain main's actual 8B Counter/benchmark protocol, V3e/V3f/EO1 composition; rejected PR14/ST2 and PR15/DR1 production remain excluded. PR24 stays kept/merged. No source causality beyond this fair composition is inferred.

Fresh source reviews A5595438315/B5595449962 were read in full and pinned to B. They are source approvals only. Exact-head full Linux five-configuration gate, normal-debug exhaustive relay and eligible doctests belong to the concurrent exclusive owner and remain independent/pending from this runner's perspective. Failed suites stay failed. Historical Linux neutrality is not exact-head qualification; no Linux measurement ran here. Keep/reject/integration/merge/close decisions remain parent-owned.

The prior b4e36472/44b95083 70-run campaign was not cleared and had unequal EO1/V3e composition: CPU .976676/1.089646/1.250908/1.052628/.879391 and p99 .919071/.964362/.932294/1.174531/.991563. It cannot qualify B or isolate N1c causality. Historical standalone accepted CPU .866/.946/.951/.939/.878, Linux .992 (7/10<=1, neutral), plus roughly +5% slow-regime p99/TCP4k caveat remain historical. No automatic new numerical acceptance threshold is invented; all measured winning/losing regimes must be judged by the parent under that qualified tradeoff.

## Method and limits

Current endpoint verified: karllivesey@192.168.192.76, Mac.localdomain, Apple M2 Max, 12 hardware CPUs/96GiB, macOS27.0/26A428, Rust stable1.98.0(88d9e12ae)/Cargo1.98.0. The existing writable APFS disk-image RAM volume vbram has 2GiB capacity, 4096-byte allocation/device blocks, owners disabled, encryption off; hdiutil RAM backing and available space captured in mac-initial.txt. No device/setup/toolchain/dependency changes.

Exact release locked/offline all-features native_benchmark builds were sequential, two jobs, incremental0, own targets, CARGO_TARGET_DIR unset. No RUSTFLAGS/encoded flags/build-target overrides. Full source-input manifest contains Git blobs and SHA256 for1866 A/1868 B src/examples/benchmarks/Cargo/build/.cargo/fixture inputs, verified before and after build and after measurement. Binary SHA256 receipts recorded. cluster.py SHA256682c189a919ed92001bac1e5e5eca2b28dd186e22708cf8ae0226593b24063fd is identical A/B. Cargo/lock, cluster/offered implementation, Counter, HostWait, native startup/log-store are byte-identical. All intentional main-relative production/test differences are retained in source-differences.patch. Native benchmark timer literals acquire N1c's default hold field; no command-line hold/readiness override or alternate durability mode.

Original existing70-run schedule only: five rounds/cell AB/BA/AB/BA/AB, including reversed small/big binary order at15k. One group/three separate processes,8B,64warmups,window1024,production HostWait/default native WAL/durability,fresh APFS RAM WAL each run. tcp1k16k@1000/s;tcp4k16k@4000/s;tcp15k/quic15k2k/16k@15000/s;idle20 600@20/s over30s. Quiet prerequisite each round load1<1/no competing native_benchmark/matched/Cargo/rustc, original15minute bound and10s snapshots. Stock unchanged guard before archives/builds/cells/rounds and post-cleanup; unreachable/not-checked Mac is a block even if stock guard exit0. No replacement rounds, controls, tuning or trimming.

CPU sums wait4 user+sys for every thread of the three processes, through startup/campaign/warmup/offerer/WAL workers/offer phase/quorum read/drain/exit. CPU per committed/APPLIED write is primary. Busy cores=sumCPU/common first-spawn-to-last-exit wall, an average rather than peak concurrency. p99 is offered-phase intended-offer-to-completion; its interval differs from whole-process CPU. GUI/services/kernel/RAM helpers/recorder/controller are outside node attribution and were not killed. Quiet/process checks constrain but do not remove host noise. Finite observations are not full proof.

Unmodified Cluster/run_once/check/schedule/report. Recorder-only root/pin substitutions, transparent Popen.stdout iterator, pre-round stockguard stdin handshake and outer UTC bounds are disclosed in recorder-diff.txt. Original stdout/stderr/result/node argv/base/rusage/relative child-exit and outer UTC timestamps retained. Actual all-applied/read=offers+64, zero unknown/pending/refusal, unchanged term, exits0, CPU cross-check and raw counters are independently audited. Any invalid run invalidates the entire same-round pair, both binaries and both15k sizes. Marginal CPU=(CPU16000-CPU2000)/14000 per binary within the same round, then B/A; ratios summarized by median of individual round ratios, never ratio of rounded medians. All attempted records and invalid reasons remain visible.

Only poll/wait-cause, peer sends/received frames and total persistence-batch counters are available from this unchanged harness. Append-specific RPC/entry counters and leader data-versus-commit-only persistence are **UNAVAILABLE**, not zero. Peer frames are not relabelled as Append counts. No production instrumentation or PR22 overlay added.

## Controller limitations

Initial setup assertion stopped before guard/archive/build/measurement because relative nice+19 did not establish absolute19. Early read/preflight helpers therefore have no absolute19 resource PASS. Corrected launcher sets and checks actual absolute19/cores4-7 for subsequent Linux controllers and inherited helpers. A pre-build source-audit helper then failed with Python SyntaxError from a NUL escape; no build/measurement had run. Corrected bytes([0]) audit passed against the same owned archives, without rearchiving/replacing any measurement. Original failure logs retained. These infrastructure/helper failures are not invalid benchmark runs and are not erased by later success.

## Terminal evidence and audit boundaries

91 quiet samples,05:59:20Z–06:14:27Z,907seconds observed. Original recorder deadline900s with10s sampling and ps overhead yielded7s observed overshoot; no claim of a strict900s wall maximum. Load1 min1.30517578125/max3.64794921875/last1.681640625, zero competing-process detections. Timeout occurs before c.schedule executes any run. No round/sample was discarded to construct a result.

Independent audit_zero.py confirms the only measurement artifact is tcp1k/quiet.jsonl and there are zero scheduled-run folders, command/result/node records. aggregate.json records70planned/0attempted/0completed/0valid/0invalid, all25measurement rounds unattempted. paired-all-records.json is empty. Standard analyze.py/verify_records.py and independent_aggregate_audit.py were NOT RUN because their required native records do not exist; no empty-record statistics or false70-run audit PASS is claimed. No applied values/retry checks/exit-rusage/counter validity can be assessed without a run. All CPU, marginal, p99, busy/rate and per-node metrics are UNAVAILABLE for all cells. Winning/losing regimes are UNAVAILABLE.

Source hashes rechecked after quiet timeout:1866A/1868B zero mismatches. Actual binaries unchanged and no owned Mac processes before cleanup. Source/recorder/manifest receipts copied and audited before deletion. Initial final-audit controller stopped on binary-hashes.json raw byte comparison: remote compactJSON/local indentedJSON. Raw remote receipt retained, its captured SHA256 matches, parsed values match both original and final actual binary hashes. Other helper/manifest file hashes match exactly. Only receipt comparison/cleanup resumed; no workload/build/source audit repeated. See receipt-layout-note.txt.

All-attempt Linux resource compliance is NOT PASS: initial unbounded read plus relative-nice helpers did not establish absolute19; first setup controller assertion failed before archive/build/workload. Initial scope records actual15 on cores4-7. Corrected launcher enforces actual19/cores4-7 for all subsequent controllers/helpers. This limitation is separate from stock disk-budget PASS and source/build success.

Cleanup removes only the two named Mac archive/build roots (including own targets/recorders), own WAL path if present and empty owned winddown-n1c-qualified parent. WAL was never created because no run launched. No reference tools, shared port leases, unrelated Mac roots, Linux target/worktree/source/docs/memory were removed or edited. All tracked Linux SSH/controllers exited; cleanup.json confirms no owned Mac jobs before/after and both roots/WAL absent. Post-cleanup stockguard actual receipt follows. No additional benchmark, rerun, control or scheduled/detached job remains.

```text
free_gb=2852 (min 500)
target_gb=64 (cap 150) cargo_build_dirs=1
live_worktrees=2 (cap 5) prunable=0 worktree_target_gb=2 (cap 120)
linux_tmp_voteboat_dirs=242 (cap 300) linux_tmp_gb=1 (cap 10)
mac_loop_gb=10 (cap 40) mac_voteboat_dirs=2 (cap 3)
OK: disk within budget
```
