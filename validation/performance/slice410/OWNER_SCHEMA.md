# Ready request batching ownership

Purpose: amortize one ordered Core persistence unit over already-ready original
client invocations; advances P9.1 without batching delay or outcome aggregation.

API/data: preserve public singleton Shard/EffectOwner operations and original
OwnerStep shape. Add an internal opt-in collector with per-original admission,
operation and proposed position metadata. One shared Visit owns one Effect batch;
the owner emits one original OwnerStep for every selected request. ClientRouter's
existing per-admission/position/result mappings remain authoritative.

Application contract: ProposalAdmission gains default-false supports_ready_batch;
Counter opts in. Other applications preserve their existing serial Core-context
checks. A true implementation accepts the unchanged Core context plus earlier
selected original commands represented in pending reservation. Every command is
revalidated; no registration/position implies commitment or application success.

Selection/reservation: only already queued, tracked Data Propose commands with
no timers and no waiting Control/Background work; stop at a different data event.
Cap64, remaining visit items/bytes, Core staging/retention limits and remaining
original per-poll step budget. Reservation previews exactly the same collector
prefix, charging owned proposals and per-original metadata before dequeue. A
default-false application uses max1 at preview, preserving singleton budgets.
Every original queue charge remains owned until the shared Visit finishes.

Owner reservation also considers current free bulk/global space after held
visits. Bounded prefix bisection selects the largest fitting prefix; a fitting
legacy singleton cannot be fenced merely because a larger batch would overflow.
Expanded observations count against the original per-poll step limit, and their
owned allocation is reserved before effects install. Execution may shorten the
reserved prefix but never enlarge it.

Client validation indexes admissions directly and visits only the unapplied log
tail after checked applied/base range validation. Previously registered commands
are not added twice to the pending reservation seen by an opted-in application.

Failure/cleanup: a later application rejection stays queued for ordinary later
execution; first rejection retains the original singleton error. No additional
eligible member uses the original singleton Core path and its original limits.
One uncertain persistence failure fences the visit and all original requests
retain unknown/retry behavior. Cancellation ends observation, not committed work.
Terms/configurations/control traffic and prior dependencies remain original gates.

Acceptance: one persistence unit for multiple original requests and exact result
mapping; byte/count/step caps and queue fairness; default-false context-sensitive
application; immediate singleton/custom small limits; duplicate/conflict/retry;
capacity and later rejection; cancellation/term/configuration/worker failure;
real TCP/QUIC service and both platforms; formatting/four strict Clippy zero.
