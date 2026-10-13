# Slice 421: explicit native ready quantum

Status: implementation authorized and implemented; focused qualification is in
progress. This schema states contracts and planned acceptance, while README.md
records actual checked scope. No throughput improvement is claimed here.
Purpose: allow native startup to choose a bounded 32/64-item ready visit instead
of the current hardcoded 16, increasing useful originals per proposal/barrier.
The measured 16-wide visits/32 barriers for 256 writes motivate this experiment;
the selected quantum does not guarantee a batch width or physical call reduction.

## Reviewed configuration joint

Parent decision: add `NativeStartup.ready_visit_items: usize`, with all existing
literal constructors selecting 16. It sets the whole visit's original item quantum,
including consumed control/background turns; it is not a promised Data batch width.
NodeLimits remains separate: it configures replica-driver/peer/configuration routing, not the
Shard's queues, original visit quantum or class cursor. NativeMemberStartup and
NativeMultiStartup already contain NativeStartup and inherit this choice. Existing
NativeStartup literal constructors explicitly initialize the unchanged default16.

Native startup validates this scalar in `1..=64`, using the unchanged Core default
max_proposal_entries=64, before binding listeners, opening files or starting worker
threads. Manual Shard/Core embedding retains its existing broader public configuration
capabilities. This selection does not change existing static/member/multi inventory bounds. Runtime construction
still sizes the actual registered shard inventory.
No Core batch limit, outgoing wire cap, application capability or ready policy is
changed here. The same selection applies on Create, Recover and discovery prepare.

The scalar leaves all existing derived log/byte/owner floors untouched. Full
ShardLimits exposure would risk implying hard ceilings that native startup currently
auto-raises for selected WAL limits; it is excluded. Hosts wanting full replacement
retain the existing Node::from_parts/manual Shard construction joint. No parallel
limits wrapper or NodeLimits overload is needed.

## Propagation, ownership and cleanup

A validated scalar is copied into the existing ShardLimits.visit_items at
prepare_runtime, after exact recovery and before Shard/TimedShard construction.
Invalid input is rejected before resource creation in legacy `open`, ordinary
TCP/QUIC/member/discovery/rotation `prepare_with_options`, and multi `prepare_as`.
Validation must also run for enrollment before files if the selected startup field
is included in its configuration validation. Do not merely detect it in Shard::new
later, after sockets/files/workers have been opened.

Existing rejection returns the same owned application and cleanup handle. Startup
selection is volatile host configuration: it grants no membership/durability rights,
needs no WAL field and can be reselected during recovery before any Core step.
All uncertain-write fencing, thread joins and recovery ownership remain unchanged.

## Bounds and ordering

`visit_items` counts all original events consumed during one visit, including
control/background, not just proposals. Ready collection remains bounded by its
remaining quota, caller max_items, Core max64, remaining log entries, original
queued/input/staging bytes, owner output/continuation/free-byte reservations and
visit deadline. Count quantum changes neither visit_bytes nor visit_ms (default2ms).
Owner adaptive reservation still shrinks a ready prefix when singleton fits but a
large prefix does not. ReplicaPollBudget.steps defaults128 and continues bounding
expanded per-original outputs; raising quantum never overrides poll budgets.

Class cursor stays Control,Control,Data,Background and survives visits. Control
inputs remain queued and validated in their normal turns. Routine current-term ACKs
and heartbeats use the existing coalescing rules; protected controls and Background
still veto ready batching. Raw/singleton owner APIs and apps without the explicit
supports_ready_batch capability remain singleton. Group scheduler fairness, timer
work and exact individual admissions/positions/receipts remain unchanged.

With benchmark window32, even quantum64 can select at most32 queued originals.
64 can supply visit-quota headroom after earlier control work; it does not create a
64-wide batch or same-group persistence pipeline. Actual histograms/call deltas must
show the effect. Barrier amortization still requires proposal and commit durability,
and follower/maintenance work remains real and present.

## Explicit benchmark opt-in

Implemented additive `--ready-items 1|16|32|64` option, default16, parsed/validated before
fresh directory creation or endpoint reservations. Preserve the existing TCP/QUIC,
window1..32, warmup64, offered/maintenance and journal-timing modes. Propagate the
choice both through real NativeStartup and the existing shared manual Shard assembly,
including reopen/retry; never silently ignore it in a branch. Reject duplicates,
missing/invalid values and unsupported lane-mode combinations before resources.

Add settings output for requested/effective ready visit items, Core proposal cap,
visit byte/ms bounds, app ready capability and existing window/payload/durability
fields. Keep all original samples, original IDs, recovery retries, maintenance,
worker joins, actual physical-call observations and weighted visit histograms.
Queue refusal reasons remain unobserved; do not attribute changes to private causes.

## Acceptance before paired timing

- Startup default16 plus explicit1/16/32/64, static/member/multi and discovery/rotation
  branch propagation; invalid0/65/usize::MAX/inconsistent budgets reject before files,
  listeners or worker launch. Existing startup failure cleanup stays intact.
- Opt-in app with >=64 queued originals produces bounded widths and exact individual
  first positions/results; quantum1 remains original singleton. No result at Written.
- Protected controls, Background and routine controls under continuing arrivals:
  no starvation, preserved cursor/turn ordering, timer deadlines and cross-group
  service. Maintain mixed receipts and original duplicate/conflicting retry behavior.
- Byte/time/output/pending-admission bounds shrink/refuse correctly for all selections;
  no dropped queued originals, no false admitted success, no extra error suppression.
- Unknown outcomes after worker failure and actual ModelIO power-loss publication
  cuts; reopen with the same/different valid volatile quantum and retry original IDs.
- Actual native TCP and QUIC receipts/recovery/join controls at32/64, plus default16
  behavior and existing maintenance/checkpoint/reclaim work. CLI reports actual settings
  on startup/shared/offered/recovery branches and preserves raw histogram identities.

Only then freeze source/artifacts and compare both hosts with identical protocol,
window, workload and durability. No timing claim follows from this schema alone.
