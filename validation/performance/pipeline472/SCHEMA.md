#472 bounded driver pump, before implementation

Purpose: remove a source-demonstrated ordering gate in the current470 path after
its real native comparison showed rare sharing and inconsistent useful benefit.
This is one bounded hypothesis, not a proven cause or permission to retune ready
quantum, client/window capacity, checkpoints, storage format or sync durability.
Full P9 remains active; default depth1 stays the ordinary serialized path.

## Existing gate and data/API shape

ReplicaDriver::poll_inner (src/runtime/replica.rs) drains workers, advances ready
work once, collects/retries effects and submits persistence. Successful submission
only then parks A's visit and enqueues its group (runtime/append_window.rs and
runtime.rs). The ready advance has ended, so it cannot create B in that poll.
Native worker ready_window snapshots currently queued requests before a fixed
append/barrier; a later B cannot join A and needs C already queued for the next
barrier. This proves an ordering restriction, not its share of measured cost.

Use a private per-poll remaining-budget record initialized ONCE from the existing
ReplicaPollBudget. Share original step, effect-lease, retry, worker-event, snapshot
event and submit-attempt allowances across all passes. Prefer existing progress
counters; derive exact residuals, never grant a new full allowance per pass.
The public budget/provider/Core authority/tickets need no change. A private
submission result may expose whether a successful selected append actually parked
and re-enqueued a visit, rather than treating arbitrary progress as permission.
Only split a helper if needed for existing zero formatter/Clippy thresholds.

## Ownership and transitions

Retain existing initial reclaim/snapshot/worker processing. Depth1 executes the
exact existing single-pass cadence and callback count. Explicit depth>1 may
repeat ready-advance -> observe-reads -> collect/retry effects -> control/data
submission only after an accepted selected append parks its visit and residual
budgets permit. Invoke the existing scheduler, not a direct hot-group loop: queued
cold groups retain their FIFO place. Reuse the supplied now/deadline. No input
lookahead, new future offers, waiting, sleeps or changed ready quantum.

Before each additional ready pass, nonblockingly process already available worker
events within the ORIGINAL remaining worker-event allowance. Exhaustion stops the
pump. Any observed first Durable/Failed/terminal seals/stops this pump after its
ordinary continuation; do not open a replacement window in that pump. Existing
control/background/timer admission seals remain authoritative. Preserve original
control submission allowance/precedence; pending control work cannot be bypassed
by another data pass. Accepted-but-unsynchronized units never authorize canonical
state, quorum evidence, application completion or a successful original result.

The original request, receipt, callback, lease and credit owner survives every
pass. Never re-execute consumed application/configuration callbacks; preserve
CheckReplay's existing checked-prefix/error fallback. Collect/retry counts and
submission ATTEMPTS are bounded as well as successes. Stop on no eligible input,
no progress, closed/sealed window or any exhausted budget. Reconcile once at the
ordinary end with the original limits. No new long-lived cache/debt or restart
state; the existing accepted-window/ticket/recovery protocol remains unchanged.

## Partial progress, failure and cleanup

On B overload restore its exact lease to the original front and stop; do not pop
or recheck it repeatedly. Accepted A retains its exact authority/owed output.
Fatal provider/owner failures keep existing fencing and accepted-descendant
cleanup. If a worker terminal arrives before another pass, process it in original
order and stop pumping. Return queued originals, parked owners and reservations
intact on ordinary limit exhaustion. No barrier is delayed to wait for more work,
and no control, snapshot or peer completion policy is weakened.

## Acceptance before promotion

1. Explicit depth4,64 already queued originals, ready16, held first barrier:
   four ordered distinct unit IDs can submit within one driver poll, all64
   original mappings/receipts and whole-window escrow/credits conserved.
2. Default depth1 retains exact one-submit/callback cadence. Residual step/effect/
   event/retry/attempt bounds cannot multiply; zero/no-progress stops.
3. Cold group/control/background priority and same supplied deadline hold; a
   control between A/B seals before B. Available Written then Durable/Failed
   precedes additional ready advancement; exhausted event allowance stops.
4. B overload restores exact FIFO lease once; provider failure, late/stale IDs,
   partial callbacks, unwind/fence cleanup and original outcome conservation hold.
5. Relevant existing driver/owner/window/native-provider histories, actual TCP/
   QUIC retry/replacement/recovery and required strict profiles stayzero. Reuse
   exact unchanged source scopes; no broad unchanged rerun solely for bookkeeping.
6. Publish checked slice through enabled hook; fresh asserted artifacts then the
   same native campaign, with every raw failure/tail. Demonstrate actual physical
   sharing PLUS improved completed originals/s/refusals/tails/publications per
   useful completion. Four queued units need not mean four per barrier.
7. Qualify framework bulk/singleton and Mac behavior before performance retention.
   If the finite gate cannot be fixed within the existing budgets/contracts, or
   useful gain remains inconsistent, reject without another knob-tuning family.

No production implementation or measured472 speedup exists at this schema stage.
The both-host seven-cell healthy minimum, sustained physical memory/debt, durable
application/headroom/fairness/placement/shared-barrier/P8 requirements remain open.
