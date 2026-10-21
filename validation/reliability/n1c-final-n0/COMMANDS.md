# PR23 final N0 commands and scope

Host: karllivesey@192.168.192.76 (Mac.localdomain), Apple M2 Max, macOS 27.0 / 26A428.
Pinned A: b4e3647250d6b3ae6c770bae6ae98e88f0b3c04c.
Pinned B: 44b95083e96f7b09ab4e900ed4e1c25042896c2a.

Sources created by `git archive SHA | ssh HOST 'tar -xf - -C ROOT'` into only:
- /tmp/voteboat-claude-loop/winddown-n1c/main-b4e36472
- /tmp/voteboat-claude-loop/winddown-n1c/candidate-44b95083

PATH=$HOME/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
CARGO_TARGET_DIR unset. In each root, sequential:
`cargo +stable build --release --locked --offline --all-features --example native_benchmark`
Each build writes only its own target/. Build stdout/stderr retained locally.
Exact source-input Git-blob verification covers src/, examples/, Cargo/lock/build files,
.cargo/, tests/fixtures/ if any, and cluster.py; source-manifest.json and source-verification.txt.

For each cell, sequential foreground SSH, no detached jobs:
`python3 /tmp/voteboat-claude-loop/winddown-n1c/candidate-44b95083/measure.py CELL`
CELL = tcp1k, tcp4k, tcp15k, quic15k, idle20.
See measure.py for recording-only wrapper; it imports pinned candidate cluster.py,
uses unmodified Cluster/run_once/check/schedule/report, and retains exact node stdout
lines and stderr plus result.json. All node settings and measurement logic unchanged.
No additional node instrumentation, hold override, diagnostic mode or ready-items override.

Five historical N0 rounds (`--pairs 5` equivalent), AB/BA/AB/BA/AB. At 15k,
small/big order reverses with binary order. Each round requires Mac load1<1 and
no competing native_benchmark/matched/build/rustc process, checked with live ps;
wait at most 15 minutes, then refuse timing. Per-round ps and load logs retained.
TCP1k: 16,000 offers, rate 1,000/s. TCP4k: 16,000 offers, rate 4,000/s.
TCP15k and QUIC15k: 2,000 and 16,000 offers, rate 15,000/s.
Idle20: 600 offers, rate20/s, 30s horizon. Window1024, payload8bytes,
64 warmups, one group, three distinct processes, production HostWait, native
startup/default WAL and durability, fresh APFS RAM-volume WAL directories.
No external client CPU; all node CPU summed including startup/campaign/warmup,
offerer/WAL workers, measured phase, quorum read, drain and exit, per harness.

Actual per-node argv recorded in each command.json, selected base in result.json.
WAL root exclusively owned: /Volumes/vbram/pr23-final-n0. Per-run WAL removed only
after output/result copy, including invalid runs. No unrelated scratch removed.
Invalid run reasons retained, and any invalid run invalidates its complete round.
15k marginal CPU uses only same-round big-small and same-round B/A.
Per-cell medians and ratios in DETAIL.md/aggregate.json are recomputed from raw
result.json/runs.json, never rounded console figures.

No Linux build, test, checkout, worktree, main source/docs/memory change; no new
research/slices. Main target/claude/research-r4/pr23-final-n0 contains evidence only.

Accounting limits: wait4 covers only the three node processes and their threads.
As documented by cluster.py, busy cores averages their CPU over the shared whole-run
wall interval; it is not peak concurrency or total machine CPU. macOS GUI/services,
RAM-volume diskimages-helper and kernel work are outside this attribution. They
were not killed or silently charged to a node. The low-load prerequisite and live
process snapshots constrain, but do not remove, platform noise. p99 is offered-phase
intended-time-to-completion; it does not include startup, whereas CPU does.

Raw node stdout is captured in memory by a transparent iterator around Popen.stdout
and written after run_once returns. No extra child, node command or on-node timing
instrument is introduced. Stderr is the harness's original file copied verbatim.
Fresh WAL is removed only after these records and result.json are written. Recorder
and parent ps CPU are outside the three-node sum, identical for A/B.

Final steps: python3 analyze.py; python3 verify_records.py; SSH python3 - < verify_sources_mac.py; after successful audit, SSH python3 - < cleanup_mac.py; sh target/claude/tools/diskguard.sh. Outputs preserved in aggregate.stdout, record-audit.txt, source-verification-after.txt/.stderr, cleanup.json/.stderr, diskguard-after-cleanup.txt. Final hostname was Karls-MacBook-Pro.local (initial Mac.localdomain), at the same SSH endpoint and with unchanged pinned source and binary hashes.
