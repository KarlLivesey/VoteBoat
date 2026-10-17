# H1 builder measurements — 17 October 2026

Stacks on PR 1 (`622bc411`); harness-only changes. These are three interleaved
pairs per engine and cell on a shared, loaded Linux 9800X3D host, not a sustained
capacity qualification. Every run and uptime observation is retained.

- `buffer-pairs.txt`: `297aec15` flag-only versus fixed original-ID buffer
  (source subsequently committed in `b58f57bf`).
- `current-pairs.txt`: fixed-buffer baseline versus **rejected** current-thread
  client candidate (`80cab34e`), preserved unchanged as historical raw data.
- `environment.txt`: compiler, OS, CPU, flags and common settings.
- `final-cli.log`: historical `80cab34e` binary worker-count and campaign guard
  controls, not controls for the restored runtime.
- `round2/REPORT.md`: restored-runtime validation, full attribution outputs and
  the explicitly unverified numeric match on the heavily loaded host.

The fixed buffer is kept on code reasoning; its allocation saving is unmeasurable
at this noise level. All sample medians, including singleton deltas, are
historical diagnostics rather than measured wins or attributed savings.
Quiet-Mac confirmation remains recommended. The current-thread client was rejected
and reverted in round 2. GPT-6.1-Sol review found smaller merge widths and higher
persists per original in all three bulk pairs, including with attribution off:
attribution-on median mean width 687.36→512.33 and persists/original
0.005866→0.006617; attribution-off 715.28→554.08 and 0.005997→0.006287.
The candidate changes the benchmark instrument despite favorable CPU medians.
The original one-worker multithread client runtime is restored without an
alternative flag. Any future runtime experiment needs separate quiet-Mac runner
qualification of widths, persists/sends per original, latency and CPU for both
engines. The implementation record contains decisions and limits.
Claude Opus round 1 independently found narrower merges and more persists across
19 bulk pairs and accepts dropping `80cab34e` (B1). Its N1 timing concern is
removed by the same revert; the runtime mode that would require N4's additional
topology test is not retained. Both reviews require cautious CPU interpretation.
Actor-only changes were excluded before implementation because the pinned
OpenRaft provider owns its matching path. No library change, dependency, unsafe
code, observer-count change or per-original responder removal is part of H1.

## Method

Check out the exact source in a clean detached worktree: `297aec15` for flag-only,
`b58f57bf` for the retained buffer, or `80cab34e` only to reproduce the rejected
candidate. Build from that worktree with its matched comparison lock:

```sh
CARGO_TARGET_DIR="$PWD/target/h1-release" CARGO_BUILD_JOBS=4 cargo +stable build --release --manifest-path benchmarks/matched/Cargo.toml --locked --offline
```

Copy each resulting `voteboat-matched-framework` executable aside before the
next build; the logs identify their SHA-256 hashes. The measured source changes
are separate commits. Both baseline and candidate binaries are untracked local
artifacts under `target/h1-evidence`. No builds/checks ran during measurements;
other workloads were left running.

The historical pairs used the supplied measurement tool at
`/home/karllivesey/Documents/ChatGPT/VoteBoat/target/claude/tools/run.py`:

```sh
uptime
python3 -I /home/karllivesey/Documents/ChatGPT/VoteBoat/target/claude/tools/run.py BASELINE 4096 4 4000000 --engine voteboat --server-workers 16
python3 -I /home/karllivesey/Documents/ChatGPT/VoteBoat/target/claude/tools/run.py CANDIDATE 4096 4 4000000 --engine voteboat --server-workers 16
```

Those absolute paths describe historical provenance; they are not prerequisites
for reproduction. The tool starts a fresh interpreter for exactly one child and
computes `cpu_seconds = getrusage(RUSAGE_CHILDREN).ru_utime + ru_stime` after it
exits. It reports `cpu_ns/write = cpu_seconds * 1e9 / completed_originals`
(`settings.operations` is equal on success), `cores = cpu_seconds / wall_seconds`,
and the harness's `operations_per_second`. Wall time surrounds process launch
through exit; CPU includes setup, warmup, campaign, follower drain and cleanup.
Medians use the three printed integer-rounded CPU samples. Do not reuse
cumulative child rusage across multiple invocations in one interpreter.

This self-contained replacement requires no untracked tool and corrects the
p99 lookup to `report["request_p99_ns"]` (Claude Opus N3). Substitute either
locally built binary for `BINARY`; use `--engine openraft` for the other engine:

```sh
python3 -I - BINARY --engine voteboat --clients 4096 --batch 4 --operations 4000000 --server-workers 16 --warmup 1024 --maintenance default-retention --envelopes upstream-default --attribution false --campaign-timeout-secs 600 <<'PY'
import json, resource, subprocess, sys, time
start = time.monotonic()
child = subprocess.run(sys.argv[1:], capture_output=True, text=True, check=True)
wall = time.monotonic() - start
usage = resource.getrusage(resource.RUSAGE_CHILDREN)
cpu = usage.ru_utime + usage.ru_stime
report = json.loads(child.stdout)
originals = report["completed_originals"]
assert originals == report["settings"]["operations"]
print(json.dumps({"rate": report["operations_per_second"], "wall_s": wall,
                  "cpu_s": cpu, "cpu_ns/write": round(cpu * 1e9 / originals),
                  "cores": cpu / wall, "request_p99_ns": report["request_p99_ns"]}))
PY
```

The corrected recipe was smoke-checked on the restored release executable with
2 clients / batch4 / 128 originals / 4 warmups: successful completion and a
numeric top-level p99. This validates the recipe, not performance or latency
equivalence. Historical logs and their missing p99 values were not rewritten.

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
