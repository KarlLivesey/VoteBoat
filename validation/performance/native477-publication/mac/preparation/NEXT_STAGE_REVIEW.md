# Independent477 review and next usable step

Both original campaigns actually ended0 before this review. Independent parsing
of all96 raw offer ledgers, summaries, storage counters/histograms and resource
receipts passes. `INDEPENDENT_READOUT.json` retains every trial and input hash;
`INDEPENDENT_COLD_WORK.json` independently records maintenance interval unions.
No engine edit, build, check suite or benchmark was launched for this review.

## Observed useful gain and limitations

Bulk completed originals/s and successful-original intended p99:

| Host/protocol | Atomic originals/s | Witness originals/s | Atomic p99 ms | Witness p99 ms |
|---|---:|---:|---:|---:|
| Linux TCP |198.032|266.437|19704.453|12856.661|
| Linux QUIC |163.538|303.161|23693.973|10756.221|
| Mac TCP |357.263|481.317|6927.865|1275.512|
| Mac QUIC |353.521|470.403|7178.674|1472.977|

These are three-trial medians. Every corresponding bulk paired rate and p99
improves; all paired values remain in the JSON, not just the ratio of medians.
Singleton completed medians are Linux TCP5.094→5.969, QUIC4.818→6.062;
Mac TCP7.250→7.406, QUIC7.406→7.406 originals/s. They are offered8/s/window1
diagnostics with refusals, not maximum singleton throughput.

Do not erase regressions: Linux singleton p99 is worse in TCPtrial2 by2.8% and
QUICtrial1 by8.7%. Mac moderate QUIC p99 is897.705→1891.831ms; the paired ratios
are2.19699/.78991/2.15140, and witness window refusals43/0/3 versus atomic0/0/0.
Linux moderate TCPtrial3 p99 also regresses2.8%. Late offer decisions are retained
as that specific observation; no missed-timer counter exists in this fixture.
Third-filesystem acceptance, sustained memory/debt and full P9 remain open.

## Remaining limiting stage

The witness bulk/overload leader's barrier elapsed is83–91% of the native elapsed
boundary. Bulk means are24.963–28.152ms per barrier on Linux and14.007–14.292ms on
Mac. The remaining WAL sync and independent witness publication each account
for roughly half. Their nested elapsed must not be added to host-poll elapsed
or described as CPU. Whole-child CPU averages approximately0.20–0.35 core
equivalents, including warmup/recovery/cleanup, on a different timing boundary.
That supports attacking synchronization quantity before CPU micro-optimizations;
it does not identify a CPU function hotspot.

The recorded `batch_histogram` is **append units per append call**. Every measured
leader histogram has only bin1. There is no recorded histogram of command-bearing
proposal widths or zero-command updates. Witness bulk has about8.665/8.761 commands
per append unit on Linux TCP/QUIC and7.714/7.692 on Mac. Those averages include
metadata transitions and are not literal proposal widths.

Source independently supplies a concrete avoidable batching ceiling:
`src/runtime/ready_batch.rs::window` limits a ready command prefix to remaining
`ShardLimits.visit_items`, currently16, and then to the Core limit,64.
It consumes only already queued ordinary originals and cannot wait for future
work. `src/runtime/ready_batch.rs::collect` records separate admissions/positions
and uses one `Raft::propose_batch` transition.477 now demonstrates thousands of
pending originals, so ready64 can actually be exercised; the old window32
quantum tests could not demonstrate that demand envelope.

Cold work remains real but is not a supported sole explanation for the tails.
Witness bulk maintenance interval unions across all replicas have median spans
of3.230/1.895s Linux and1.332/1.075s Mac TCP/QUIC, with overlaps merged. Mac's
moderate-QUIC bad witness trials have maximum checkpoints350/603ms and maximum
reclaims281/282ms. The union alone cannot attribute the approximately1.9-second
end-to-end p99 or establish a filesystem cause. Preserve this regression guard.

## Recommendation: fewer original-command barriers using the existing ready64 profile

Run the same frozen476 witness executable with ready16 versus ready64, using
the root's compact478 matrix, both protocols and three rotating adjacent pairs:
singleton256@8/s/window1; moderate8192@128/window256; one-group bulk8192@1024/
window4096; two-group31:1 skew8192@1024/window4096. This yields48 cases and22.4
scheduled horizon minutes per host, excluding startup/drain/recovery/cleanup.
This directly tests an already usable embedding setting with the new4096 backlog
and two-sync provider. No new code, provider format, batching timer, queue capacity,
maintenance interval or preparation dependency is needed. It is a new measured
context, not a replay of the old narrow-window quantum scores.

Use **actual barriers per completed original** and completed/during-horizon rate
as the useful-work decision, with latency/refusals and original outcomes. Do not
promise a4x gain or infer proposal widths from the aggregate histogram. A larger
ready prefix may increase admission work and host-poll tails; that is a required
failure criterion, not something to hide by averaging. Source currently rebuilds
Counter's reserved operation set for every candidate, so admission is the next
CPU consideration if larger batching exposes it;477 does not justify prioritizing
that over the dominant barrier stage today.

Do not revive461 commit/proposal fusion or470/472 persistence pipelining here.
The rejected-slice ledger already covers those ownership/order changes. Neither
needs to become a prerequisite to measuring this existing bounded setting.
FileLogIo and ready16 defaults remain unchanged. A new general adaptive policy
would need a separate schema and hot/cold fairness evidence; it is not implied
by a one-group profile win.

## Small schema and acceptance boundary

- **Data/API:** existing `--ready-items16|64` / `ShardLimits.visit_items`; same
  source/artifact/native profile, witness provider, admission4096, Core64,
  byte/effect/control budgets, active2s maintenance and original operation count.
  Only this existing startup setting differs between paired arms. Skew uses
  the existing weighted workload and groups2 on both sides. Its hot group has
  a2048 pending ceiling because `Ledger::full` divides the global4096 window
  equally across groups; disclose this instead of claiming4096 hot pending.
- **Ownership/transition:** immutable selected ready budget is captured by the
  same Create/Recover closure in `run_selected`; capacity readback verifies the
  actual full Shard limits before workers exist. Ready extraction owns only the
  bounded queued prefix, retains original IDs/admissions/receipts, and preserves
  protected control/background boundaries. No wait-to-fill or extra thread.
- **Partial progress/restart:** no change to accepted/Written/Durable ordering,
  one pending ordered Core transition, exact tickets, quorum commit, snapshots,
  fencing or retry. The same setting reconstructs each reopened assembly; a
  selected capacity is never a claim of observed occupancy or proposal width.
  Existing failed/uncertain cases remain terminal evidence, never replaced.
- **Checks:** reuse qualified476 source/provider tests by exact identity; use
  the same frozen validator closure and guard/readback scheme. Fresh absent
  case roots, same volume/device and quiet host. No candidate rebuild is needed.
- **Performance decision:** complete all48 finite cases per host; compare paired
  completed and during rates, actual barriers/original, drain/backlog, every
  refusal, nearest-rank p50/p95/p99/max, max host-poll and physical/cold work.
  Preserve singleton and moderate-QUIC regressions. No default promotion or
  universal acceptance without the pending real ext4/XFS and fairness/sustained
  requirements. If ready64 fails, retain the failure and do not retune the same
  matrix through repeated variants without another measured structural cause.
  The skew cold group has256 measured offers at32/s across8s; that finite sample
  and possible two-group shared barriers are diagnostics, not sustained fairness
  or a substitute for the separate one-group floor.
