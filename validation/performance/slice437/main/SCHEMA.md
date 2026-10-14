# Slice437: reject436 and restore qualified435 client ownership

Decision before editing: reject combined receiver-only/inline wait candidate436.
Linux bulk rates improve but one p99 regresses; Mac bulk rates are mixed.
Linux singleton loses9.7% in one pair; Mac singleton has two rate losses and
all three p99 regressions. Larger dev future336 versus old-code replica304 is
an observed risk, not an established cause. No further representation tuning.

Data/API: restore Client::write and every non-test matched host/core/library
input to published4355c2390cb's exact implementation. Remove private production
Waits enum/module. Keep the independent client outcome controls as a cfg(test)
module; remove implementation-specific Waits layout/report helper and old-code
replica, whose historical output remains archived436 evidence. No public/provider,
wire/storage/dependency/capacity/timer/maintenance/result-channel change.

Transitions/ownership: original independent Vec<(OperationId, Receiver)> again
owns every channel wait. IDs validated before channel creation, complete request
sent before original-order receipt waits, including empty send. Standard drop on
invalid ID/send failure/cancellation/wrong or closed receipt remains unchanged.
Accepted work stays actor-owned. No new watermark/generation/restart/cleanup state.

Acceptance: exact435 Client/production source proof with only documented cfg(test)
loader/new outcome module differences; preserve raw436 bulk/singleton regressions.
Run retained client controls plus standalone whole suite once after structural
module changes, formatting and strict all-target checks. Full enabled publication
hook remains mandatory. Unchanged root functional inputs permit exact scope reuse;
no extra broad root suite or old benchmark/profiler rerun. Immutable435 artifacts
and saved artifact-bound profiles remain the measured baseline. Test-only source
changes do not establish a new throughput claim. Review macro/current-plus-next-two
plan; full P9 floors/durable/headroom/fairness/placement/P8 remain active and unmet.
