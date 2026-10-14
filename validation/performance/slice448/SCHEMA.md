# Slice448: grouped allocation of independent reply channels

Selected from current446 profiles before editing. Linux client-write inclusive
21.640%, receiver polling7.825%, channel construction6.774% and sender self8.075%
overlap. Mac client-write inclusive9.948%; its unresolved allocator-library
contexts do not establish allocation/free or causation. The source constructor
currently allocates one Tokio oneshot per original. Group allocation removes
three such allocation/deallocation pairs per four-original request; speedup is
unproven. Empty command bytes already clone without payload allocation/copy, so
shared-payload work is held rather than used to claim removal of entry traversal.

Scope/data: private matched-host reply module only; no production/provider/wire/
WAL/public API change or new dependency/unsafe code. One Arc-owned mutex-protected
group contains four inline slot states and a Vec overflow for widths above4.
Width0 creates no group. Width1 still has one allocation but a larger block.
Factory indices are issued exactly once. Every original retains its own sender,
receiver Future, optional value, sender/receiver liveness and waker. Full128-bit
operation IDs and the existing request/wait buffers and result comparisons stay.
No operation-ID packing, channel/outcome elision, reordered/deferred notification,
reply dispatcher or extra runtime worker. Per-original completion channels remain
the disclosed comparison contract on both engines.

Transitions/ownership: consumed sender publishes exactly one slot value or
returns that value on a closed receiver. Dropping an unused sender closes only
its slot and wakes that receiver. Receiver completion consumes its value once;
receiver cancellation closes only its slot and drops any sent value. Other slots
remain independent. Pending lookup, ordered remove/send, duplicate replacement,
delivered prefixes before late errors and all original admission gates stay.
Private receiver-error implementation may change; existing closed-error text and
original-operation outcomes remain. Dropping the factory leaves no issued handle
for unused slots; the group is freed after its last endpoint/factory reference.

Concurrency: state/value/waker registration is synchronized by the group mutex.
Clone, drop and wake arbitrary wakers, and drop values, only after releasing it.
Any unlocked waker-cloning path re-locks and rechecks value/closed state before
registration, preventing a lost wake if send/drop races in that gap. No guard is
held across await or any user callback. Sender/receiver cleanup must work under
interleaving, cancellation and reentrant wakers without affecting neighbors.

Fairness: Receiver::poll calls pinned Tokio1.53.2's public cooperative
poll_proceed before inspecting state. Ready value or closed result commits one
unit; Pending restores the guard. Budget exhaustion yields without consuming the
slot. Plain try_recv cannot replace this path because it bypasses the original
cooperative accounting. Existing runtime1/server16, envelopes, checkpoint cadence,
client joins/timer boundaries and provider observation29/four/15 stay unchanged.

Bounds/tradeoffs: logical pendingP staysclients*4+1; shared groups retain completed
slot storage until the last remaining endpoint is dropped. Account for inline
four-slot space, overflow, mutex, Arc handles and unchanged request/wait buffers;
report actual sender/receiver/optional-sender/block/future layouts, not predicted
sizes or RSS. Shared refcount/cache-line and mutex contention, larger singleton
allocation and >4 overflow allocation may outweigh removed calls. No allocator
cause or performance win is inferred from sampled names.

Acceptance: keep all existing69 outcome/admission/maintenance/snapshot controls;
add independent per-slot delivery/wakers, send-close/drop ownership, wrong/closed
receipt order, registration-gap/reentrant/concurrent races, arbitrary widths and
exact cooperative-budget/cancellation controls. Format and all strict checks
zero, source/mode guards and independent review; publish the complete checked
slice, then fresh asserted Linux/Mac artifacts and repeated paired20M bulk/1M
singleton against immutable446. Keep all raw failures/tails/regressions and decide
retention from actual completed rates/latency/memory costs. No native-service gain,
full-floor or macro closure. Pi stays stopped and the complete P9 goal stays active.
