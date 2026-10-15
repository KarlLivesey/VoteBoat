#475: explicit bounded capacity profile for offered-load measurements

Before editing. Purpose: measure real durable headroom/batching at windows32,
256 and4096; current CLI/validator cap64, client per-group64 and shard ordinary
queue240 prevent that experiment.474's same-device filesystem policy results are
mixed; keep the existing default filesystem and sync/manifest protocol. This
fixture change advances P9.2/P9.3, not an engine speedup or default promotion.

## Data/API and bounds

Add a private explicit `--offered-capacity 4096` mode. Only that checked value is
initially supported; absence preserves all current inputs/defaults and window64.
Reject duplicate/missing/unsupported values, closed-loop/startup/lanes use and
window>selectedcapacity before opening any root/socket/worker. An explicit mode
allows offered windows1..4096 and installs one COMMON profile for every sweep
cell: ClientRouter requests4096/group_requests4096; shard group_items4112 with
existing16 group control slots. Keep global8192 items/1024 control slots, bytes,
control/background reservations, ready16, Core proposal64, serial worker/Core,
transport, timers, application accounting and durability unchanged. Larger
window is backlog capacity, never an assertion of4096-wide batches.

Thread the immutable profile through shared assembly create and every recovery
closure. Startup/ordinary shared and static-lane callers use legacy settings.
Validate/read back actual public router/shard limits before spawning workers;
report selected/effective profile and all changed item bounds in offered output.
No production src/public provider/API/format/contract/default changes, new cache,
hidden worker, runtime singleton or persistent state. Refusals remain visible.
The current 4MiB group bytes and32MiB client bytes remain real independent caps;
report actual occupancy/refusals instead of assuming a full4096 window is reached.

Extend the independent offered validator only for output carrying this explicit
profile plus exact consistent limits. Legacy reports stay limited to64; never
reinterpret archived workloads. Keep original per-operation ID/status/order,
intended/dispatch/completion clocks, horizon/drain/p99, capacity/refusal, retry,
recovery value and worker-join conservation. Qualify parser refusal-before-root,
actual applied profile in create/reopen, ordinary/legacy bounds and isolation,
full-window admission/refusal/control reserve where feasible, and negative CSV
mutations; do not weaken existing checks. Constructors/partial startup failure
use existing cleanup; profile is immutable volatile host configuration reused on
restart, not authoritative log or quorum evidence.

## Acceptance and measurements

Focused Rust example/JS controls, actual small TCP/TLS+QUIC offered runs including
wide selection/recovery/readbacks, metadata and full enabled strict hook atzero.
Commit/push checked fixture, then fresh native artifacts on each host from the
published source with root+example/compiler/profile/locks/source/modes guards.
Re-use exact unchanged production/protocol/recovery/P8 qualification. No broad
unchanged re-run, framework rebuild or synthetic code speed claim is needed.

Use one common selected4096 profile/executable per host for all cells: onegroup/
3voters/8B Counter/64warmups/8192 offers, windows32/256/4096 and offered32/128/512/s,
ready16/maintenance2s/noPause. Horizons256/64/16s fit<=300s and count+warmup+64 fits
unchanged16384 log limit. Three rotating window trials, TCP/QUIC (54cases/host),
quiet serial timings with no builds/copies/checks/archive competition; preserve
every failure and original handle. Capture actual completed/during/drain rates,
intended/service/dispatch latency, all refusal classes, pending/backlog/observed
width, maintenance/catch-up/recovery/retries/cleanup, per-replica physical barrier/
sync/publication deltas, wholeprocess resources with honest memory/debt scope.
Do not extend120s drain guard or replace a slow/failing case. If this reveals
unmet capacity or lack of grouping, use measured stage/counter evidence for the
next algorithm/order/publication decision, not more unsupported knob tuning.

The subsequent publication format needs its own explicit crash/corruption and
migration plan. Naive highest-valid-slot or last-valid-WAL-marker fallback cannot
hide final acknowledged damage. Full P9 seven-cell/both-host healthy margin,
8,422,500/s batch4 minimum (not target), durable/headroom/fairness/memory/debt,
placement/shared barriers and P8 performance remain open. Windows/P10/Pi excluded.

User refinement: native optimization acceptance requires a measured gain on all
three filesystem environments: current Linux Btrfs, a real ext4/XFS partition,
and Mac APFS. Pair baseline/candidate on the SAME volume/device/compiler/profile
within each environment; a different drive is coverage, not isolation of CoW
versus hardware flush latency. No loopback-on-Btrfs substitute or automatic disk
formatting. No ext4/XFS scratch volume is currently identified; user will provide
one and explicitly directs continuation on Btrfs/APFS meanwhile. Keep that cell
pending. The common capacity sweep applies to each available environment; no
future engine change is accepted universally before all three pass. The normal
path target is portable reduction of publication work, with an explicit proof
for final acknowledged-authority damage before any WAL-marker implementation.
