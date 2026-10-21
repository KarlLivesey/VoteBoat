# PR23 fair exact-head Mac N0 gate

A=b77fd087b06c5ae76a5a1a25f2cfe3ed6891c339; B=1c2595364cbe405922695ce9c8728316ecf05741.
Linux: all corrected controllers/helpers execute `/usr/bin/python3 launch.py ...`, which sets actual absolute nice19 and affinity4-7 before exec. No Linux Cargo/build/test/worktree/checkout/target override.
Mac archive/build roots exclusively `/tmp/voteboat-claude-loop/winddown-n1c-qualified/{main-b77fd087,candidate-1c259536}`; WAL `/Volumes/vbram/pr23-fair-final-n0`.
Exact archive pipeline and argv: controller.py; full archive via git archive PIN piped to foreground ssh tar -xf - -C NAMEDROOT. Roots initially absent, no blind deletion.
Build sequential in each root, CARGO_TARGET_DIR unset, CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0: `cargo +stable build --release --locked --offline --all-features --example native_benchmark`.
PATH=$HOME/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin. Existing tools only.
`python3 .../candidate-1c259536/measure.py CELL` foreground SSH with guard handshakes before each round. Stock unchanged diskguard checked before each archive/build/cell/round, rejects unreachable Mac even though stock script alone can return0 in that case. Guard limits unmodified.
Recorder diff retained: pin/root substitutions, pre-round stockguard handshake, outer UTC stamps only. Cluster/run_once/check/schedule/report and all node args unchanged; transparent stdout iterator and original stderr/result copy. No ready-items/hold/instrumentation overlay.
Original five rounds/cell AB/BA/AB/BA/AB, offers order also reversed at15k. tcp1k16k@1000, tcp4k16k@4000, tcp15k and quic15k2k/16k@15000, idle20 600@20/s. One group/three processes,8B,64warmups,window1024,production HostWait/default native WAL/durability. Quiet load1<1/no competing benchmark/build per round, bounded15minutes with10s snapshots, no replacement attempts.
Raw command.json includes actual base/each node argv; result includes wait4 user/sys/exit/common wall/counters. record-wall.json records outer common UTC bounds; wait4/common per-child relative stamps remain authoritative.
15k marginal=(CPU16000-CPU2000)/14000 per binary within same round, then B/A; median of within-round ratios. Any invalid run invalidates whole round, both bins/sizes. No trimming.
Source manifest holds Git blob+SHA256 for all src/examples/benchmarks/.cargo/Cargo/build/toolchain/fixture inputs, checked before builds and after builds/measurements; binary SHA256 checked after measurements. parity.json declares baseline/candidate benchmark/build/source identity and intentional differences retained in source-differences.patch.
Cleanup only named roots/owned WAL, after raw/source/binary audits and before final stockguard; no unrelated scratch, reference tools, port leases, Linux targets or jobs touched.
