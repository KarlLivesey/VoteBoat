# H1 builder measurements — 17 October 2026

Stacks on PR 1 (`622bc411`); harness-only changes. These are three interleaved
pairs per engine and cell on a shared, loaded Linux 9800X3D host, not a sustained
capacity qualification. Every run and uptime observation is retained.

- `buffer-pairs.txt`: `297aec15` flag-only versus fixed original-ID buffer
  (source subsequently committed in `b58f57bf`).
- `current-pairs.txt`: fixed-buffer baseline versus current-thread client.
- `environment.txt`: compiler, OS, CPU, flags and common settings.
- `final-cli.log`: final binary worker-count and campaign guard controls.

Both measured candidates were kept; no measured candidate was discarded. The
implementation record contains median CPU ns/original, decisions and limits.
Actor-only changes were excluded before implementation because the pinned
OpenRaft provider owns its matching path. No library change, dependency, unsafe
code, observer-count change or per-original responder removal is part of H1.

## Method

Build each source variant from this worktree with its matched comparison lock:

```sh
CARGO_TARGET_DIR="$PWD/target/h1-release" CARGO_BUILD_JOBS=4 cargo +stable build --release --manifest-path benchmarks/matched/Cargo.toml --locked --offline
```

Copy each resulting `voteboat-matched-framework` executable aside before the
next build; the logs identify their SHA-256 hashes. The measured source changes
are separate commits. Both baseline and candidate binaries are untracked local
artifacts under `target/h1-evidence`. No builds/checks ran during measurements;
other workloads were left running.

Use the supplied measurement tool at
`/home/karllivesey/Documents/ChatGPT/VoteBoat/target/claude/tools/run.py`:

```sh
uptime
python3 -I /home/karllivesey/Documents/ChatGPT/VoteBoat/target/claude/tools/run.py BASELINE 4096 4 4000000 --engine voteboat --server-workers 16
python3 -I /home/karllivesey/Documents/ChatGPT/VoteBoat/target/claude/tools/run.py CANDIDATE 4096 4 4000000 --engine voteboat --server-workers 16
```

Repeat for `--engine openraft` and for cell `1 1 131072`. For each cell the
engine order is VoteBoat/OpenRaft; pair orders are AB, BA, AB. Record uptime
before each engine's pair, as shown in the logs. The tool initially passes
`--engine voteboat`; its trailing explicit engine option selects OpenRaft via
the parser's last-value-wins behavior. Each invocation launches just one engine
run, so its child CPU is that process's whole CPU (including setup, warmup,
follower drain and teardown), divided by actual rounded measured originals.
The bulk requested count 4,000,000 rounds down to 3,997,696. Both variants use
1024 warmups, default retention, upstream envelopes, attribution off, a
600-second campaign guard, 16 server workers and one client executor.

Medians are calculated from all three integer `cpu_ns/write` samples per
variant/engine/cell. Rates and busy cores are in the raw logs. This supplied
tool searches a nested latency object, whereas this harness emits
`request_p99_ns` at top level, so it prints `p99=None`. No p99 result or quiet
capacity claim is made. No outlier was removed. Different experiment windows
have different background load; compare each candidate only to its own paired
baseline, not across windows. No worker scaling curve or macOS run was taken.
