#478: amortize the remaining native barriers with existing ready batches

Before runner changes or measurements.477's96 original cases and independent
raw readouts show a bulk gain from two-sync WitnessLogIo on Btrfs and APFS.
Its leader barrier still occupies roughly83–91% of bulk/overload elapsed time;
whole-process CPU is about0.2–0.35 core equivalent over a different boundary.
Do not add these scopes or infer an allocator cause. Reduce barrier frequency
before another persistence redesign or CPU micro-change. This advances
P9.0/P9.1/P9.2/P9.3; the complete P9 objective remains unchanged.

## Existing API, ownership and bounds

Compare existing `--ready-items 16` and `--ready-items 64`, explicitly selecting
WitnessLogIo and the common4096 offered-capacity profile. Reuse the exact fresh
published476 native artifacts on each host: no source/build/profile/dependency
change or new default. Public NativeStartupConfig already exposes this quantum;
startup and shared benchmark assembly install it into ShardLimits.visit_items.
`ready_batch::window` limits an already-ready ordinary prefix by the remaining
visit budget and Core's64-entry proposal ceiling. Bytes,2ms visit time, control
reserves, queue bounds, admission checks and protected-control precedence stay.
There is no new gathering timer, delayed sync, speculative persistence chain,
commit/proposal fusion or outcome removal.

The same owner retains admitted originals and builds one bounded owned proposal
batch; the existing NativeLogWorker/NativeLogStore/provider own persistence.
Nothing escapes before the original exact quorum-durable application outcome.
Partial ready-prefix refusal, failed persistence/fencing, restart, snapshots,
retry and cleanup retain their existing paths. Larger visit budget is the sole
deliberate runtime variable. Retained source/qualification hashes prove unchanged
code scope; do not rerun broad suites or rebuild just to change this supported
configuration. Check actual selected CLI histories/readbacks before timing.

This is new measured context, not a replay of rejected small-window experiments:
475/477 now actually reach thousands of pending originals, whereas window32
cannot demonstrate64-wide demand and the earlier window64 results were mixed.
477's histogram records append units per call, not command-bearing proposal
width: every one-group leader bin is1. Commands/append-unit includes metadata
transitions. Do not claim a literal width from that ratio. Compare actual physical
barriers/original, completed rates, tails and observed demand without adding a
new observer prerequisite.

## Fixed paired matrix

| Purpose | Offers | Offered originals/s | Window | Groups/weights |
| --- | ---: | ---: | ---: | --- |
| Singleton regression guard |256|8|1|1|
| Moderate-load tail/refusal guard |8192|128|256|1|
| Loaded one-group amortization |8192|1024|4096|1|
| Hot/cold fairness and shared physical work |8192|1024|4096|2,31:1|

Three rotating adjacent16/64 pairs per TCP/TLS and QUIC produce48 cases/host,
1344 seconds (22.4 minutes) of scheduled horizons plus actual warmup, drain,
recovery and cleanup. Use the same64 warmups,8B Counter operations,2s active
maintenance, native opt3/debug0/assertions/overflow/defaultLTO+codegen, existing
120s drain guard and original native timer boundary. All voters and both skew
groups share one device per host; this is not independent placement.

For skew, use the existing `--group-weights 31,1` selector and actual per-group
offer/history results. The fixture also enforces ceil(window/groups), giving
each group a2048 pending ceiling here; do not describe hot demand as4096 or
attribute this fixture protection to an untested engine policy. The cold group
has256 measured offers at32/s over8s. Report hot and cold completed/during rates, latency
quantiles/maxima, each refusal class and backlog. Cold success latency without
cold refusal counts is insufficient. Shared append-unit histogram/group counters
may demonstrate physical grouping; aggregate rates cannot replace a one-group
floor. These finite offered histories are not closed-loop maxima, sustained20M
acceptance or a long-run physical-memory/debt plateau.

## Evidence, transitions and acceptance

Reuse477's bounded controller/resource/validator pattern with the smallest
case-plan adaptation. Freeze source/artifact/compiler/device/profile identities,
actual commands/order and the full imported validator closure. Validate requested
and effective quantum in summary and the actual native command receipt; existing
run.txt does not expose quantum, so preserve it without inventing fields. Retain
the unchanged constructor/source check of installed visit_items. Check six capacity and six provider
Create/Recover readbacks. Keep original job handles, durable terminal/exit markers
and all failed/partial files. No restart or replacement on polling timeout.
Prepared -> Guarded -> Timing -> Terminal -> Collected still requires a separate
quiet-window start; no same-host builds/checks/copies/archive/profile competition.
Run independent validators only after all workload timing ends. Archive original
outputs losslessly with exact hashes/lengths and independently recompute ledgers.

Rank all three paired completed/during-rate and tail results, refusals/backlog,
maintenance/recovery/retry/joins, per-replica barrier counts/durations, ordinary
publication primitives and whole-process resources. Preserve477's Mac moderate
QUIC regression (p99 median897.705→1891.831ms; witness refusals43/0/3 versus zero)
and Linux during-rate/singleton-tail losses; do not explain them away or remove
those cells. Larger batches need useful rate/barrier amortization without hidden
cold starvation or singleton/moderate harm before any default is considered.

FileLogIo and ready16 remain defaults. Witness acceptance still needs the user's
real ext4/XFS scratch alongside paired Btrfs/APFS results; the detected spare disk
is not implicitly disposable. No filesystem or kernel setting changes. Preserve
all seven framework cells/both hosts/healthy sustained margin, bounded debt,
fairness/skew, independent placement/shared barriers and P8 composition.8,422,500
completed originals/s is the batch4 framework minimum, not a ceiling or a native
Counter comparison. No full P9 milestone closes here.
