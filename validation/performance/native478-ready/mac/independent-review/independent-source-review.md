# Independent478 readout and next stage

All96 original cases and their post-timing readback/validator exits are0.
`independent-readout.py` independently reconstructs the frozen matrix/order,
native/resource argv, known476 artifact identities, script/validator closure,
offer scheduling, exact weighted routing, per-group/global pending limits,
outcome conservation, exact nearest-rank latency, recovery/retry/joins, physical
counter/histogram deltas and whole-process resources. Its JSON retains every
case and input hash. The first analysis attempt was a parser error (`else64`);
the original stderr is retained separately, and the corrected attempt exited0.
No workload, source edit, remote command or profiling capture was launched here.

## What changed in the measured workload

Median completed originals/s, ready16 to64:

| Host/protocol | One-group bulk | During scheduled horizon |
|---|---:|---:|
| Linux TCP |260.799 ->591.381|217.500 ->360.125|
| Linux QUIC |310.957 ->667.779|275.375 ->445.250|
| Mac TCP |491.279 ->1015.083|463.625 ->1010.625|
| Mac QUIC |492.916 ->1011.578|482.250 ->1005.625|

All three paired bulk trials improve completed rate and p99 on both hosts and
protocols. Linux leader barriers/original fall about.110/.111 to.037/.036;
their inclusive elapsed fraction falls about.917/.905 to.654/.651. Mac remains
offered-capped near1024/s and retains.875/.855 leader-barrier elapsed fraction.
These scopes overlap and are not CPU attribution. Native elapsed excludes some
whole-process recovery/cleanup measured by resource CPU/RSS.

The larger quantum does not establish a universal gain or justify a default.
Linux TCP singleton median completed rate falls6.250 to5.867/s; trial1 p99
448.419 to561.566ms and window refusals25 to60. Linux QUIC singleton trial2
p99 rises644.059 to844.014ms. Linux moderate trial TCP1/TCP3 and QUIC1/QUIC2
retain worse tails/refusals; all per-trial values remain in JSON.

Mac moderate QUIC p99 is681.901/776.264/803.851ms at16 versus
1283.472/1230.672/773.438ms at64, with zero refusals in both arms. Mac moderate
TCP trials2/3 also have tail losses. Skew cold group always applies all256
originals with zero refusals, but Mac TCP trial1 cold p99 rises441.054 to
1695.135ms (max472.301 to1757.631ms); QUIC trial3 rises879.274 to1155.945ms.
Linux QUIC skew trial2 cold p99 rises1050.577 to1598.424ms. Cold throughput
without these tails/refusal counts would conceal regressions.

## Concrete source cost and ordering

`src/runtime/ready_batch.rs::collect` executes the preselected owned prefix
without refreshing time. `src/runtime.rs::Shard::poll` installs a deadline from
the caller's `MonoTime`; `Group::next_queued` checks the supplied value before
the operation. `EffectOwner::drive_ready_checked` also reuses its supplied time.
Thus the2ms setting is cooperative scheduling between operations, not an
interrupt inside a prepared proposal batch. The benchmark's maximum host poll
is an entire round over three replicas and progress processing, not one visit;
it cannot by itself prove that a particular visit exceeded2ms.

Nevertheless the source does repeat substantial unchanged work inside that
atomic prefix. For each candidate, `ClientRouter::check_ready_execution` walks
the unapplied log and every router pending entry (filtering by group), checks
selected-prefix representation, then walks pending again for receipt bounds.
`Counter::validate_proposal` allocates/rebuilds a `BTreeSet` of distinct pending
operation IDs and checks applied dedup membership for each entry. WithN retained
pending operations andB selected candidates, this is roughlyO(B*N*log(N)) set
work plus router scans/lookups, although duplicate identities reduce set size.
This is a concrete algorithmic opportunity; its share is not yet sampled.

The new regime makes profiling it useful: Linux bulk ready64 maximum host
poll medians28.468/28.859ms versus5.698/5.396ms at16; Mac skew ready64
20.214/20.837ms versus3.489/3.318ms. Whole-process CPU rises in Linux bulk
from.208/.224 to.307/.315 core equivalents, and Mac skew from.259/.289 to
.407/.424. Mac bulk CPU actually falls because64 avoids deep queued work and
finishes near the offered horizon. Do not infer identical bottlenecks everywhere.

## Small next step and ownership schema

First capture a separate headless CPU profile of the unchanged476 native
Counter artifact under loaded ready64, preserving all original results and
raw samples. Compare ready16 if attribution needs a control. This is diagnosis,
never acceptance throughput. Do not change kernel settings or use a browser.
The existing Linux451 pipeline used the extracted perf binary with
`record -e cycles:u -F1000 --call-graph dwarf,16384 -m128`, then plain-text
`perf script` and machine-readable conversion. Existing Mac scripts use
`samply record --save-only --rate1000 --unstable-presymbolicate` and direct
JSON processing. Reuse their provenance/terminal/raw-error conventions, not
their old framework executable or assumptions. Recheck actual tool identity
and permissions as part of setup; current availability is not proved by an old
receipt. Native debug0 means source/inline attribution may be incomplete;
report unresolved coverage rather than pretending every inline frame exists.

If samples confirm admission rebuilding, prepare one **ephemeral admission
context per unchanged ready prefix**, not a long-lived revision cache:

- API/data: a bounded application-specific prepared validation context plus a
  router view/index of the same pending originals and exact receipt bounds.
  Keep a generic fallback that executes current per-original validation for
  interchangeable applications. Counter stores the exact reserved identity set
  once; preserving all pending reservations includes later queued originals,
  even when a candidate earlier in the batch fails.
- Ownership/state: serialized owner borrows immutable application/Core/router
  state while preparing and validating. Initial context includes the identical
  unapplied log and pending stream. Process candidates in existing order with
  per-original deployment/context, payload, identity and receipt-bound checks;
  add only the same otherwise-unrepresented successful selected predecessors.
  Do not persist or carry the context across application/Core/router mutation.
- Failure/partial progress: build failure uses exact existing error/fallback;
  first-candidate failure retains existing singleton error behavior; later
  failure emits only the successful prefix and leaves the failed suffix queued.
  Drop context before persistence/application/router transitions, fencing,
  snapshot/restart or cleanup. Existing admission tickets/outcomes remain owned
  by current router and effect paths; no new durable state or recovery format.
- Checks: compare against current sequential reference for duplicate/conflicting
  IDs, same-ID pending/log overlap, capacity boundary, receipt-budget minima,
  invalid later candidate, unregistered selected predecessor and partial prefix.
  Include application mutation/restart between batches and generic fallback.
  Then repeat focused native paired CPU/throughput/tail/refusal/skew checks;
  preserve all known regressions. No default promotion from aggregate rate.

This removes repeated work rather than rearranging quorum/persistence authority.
It is not the rejected470/472 pipeline or commit/proposal fusion. Disk barriers
still dominate Mac bulk and most skew elapsed time, so do not promise that this
CPU change alone reaches any framework floor or native capacity target.
