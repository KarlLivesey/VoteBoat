# Ordered original results: audit and proposed host slice

Plan only. No implementation or performance claim. Advance P9 ordered-group
capacity through hosting contracts while preserving Core, operation IDs,
durability, peer priority and default maintenance.

## Pinned upstream evidence

At e88bb40707f2e511d8ecc44d31edcb1d814a11a8, batch four does **not** use one shared
completion channel:

- [app.rs lines 123–145](https://github.com/databendlabs/openraft/blob/e88bb40707f2e511d8ecc44d31edcb1d814a11a8/openraft/src/raft/api/app.rs#L123): creates a responder/receiver for each payload, submits one batch message, then unfolds those receivers in submission order.
- [progress_responder.rs lines 88–98](https://github.com/databendlabs/openraft/blob/e88bb40707f2e511d8ecc44d31edcb1d814a11a8/openraft/src/raft/responder/impls/progress_responder.rs#L88): `complete_only` allocates one oneshot, with commit notification disabled.
- [store.rs lines 308–326](https://github.com/databendlabs/openraft/blob/e88bb40707f2e511d8ecc44d31edcb1d814a11a8/benchmarks/minimal/src/store.rs#L308): advances the applied log and sends each original response.
- [bench.rs lines 387–390](https://github.com/databendlabs/openraft/blob/e88bb40707f2e511d8ecc44d31edcb1d814a11a8/benchmarks/minimal/src/bin/bench.rs#L387): drains every stream result before recording batch latency.
- [client_responder_queue.rs lines 18–87](https://github.com/databendlabs/openraft/blob/e88bb40707f2e511d8ecc44d31edcb1d814a11a8/openraft/src/core/client_responder_queue.rs#L18): indexes a VecDeque by ordered log position; push is amortized O(1), and committed prefix extraction is O(k).

VoteBoat's current matched host allocates the same per-original oneshots but uses
BTreeMap insertion/removal by OperationId. The real difference to investigate is
lookup/storage overhead, not an upstream single-future advantage.

## First candidate: bounded FIFO correlation

Data: `VecDeque<ExpectedOriginal { operation, reply }>` in the exact order passed
to Core, plus a bounded standard HashSet of currently pending operation IDs.
The set retains duplicate-admission detection; do not remove that work or use an
unchecked custom hasher. Reserve queue/set ownership before admission, with the
existing `clients * 4 + 1` original bound and explicit capacity accounting.

Transitions: validate each request's proposal/reply shape and duplicate IDs before
mutation; stage admission order; submit the unchanged Core proposal; after
application, preflight the **entire** result vector against both committed command
IDs and the expected queue prefix before popping/sending any reply. Check exact
count, original ID and command order; blank/control entries produce no receipts.
Then remove each ID from the live set and send its individual receipt. Keep all
four client waits and the all-replica applied-frontier verification.

Failure: wrong/reordered/duplicate/missing/extra results invalidate the whole
result batch before any new client success. Admission failure leaves Core and
correlation state unchanged, or performs the existing failed-cluster cleanup if
atomic staging cannot be maintained. No acceptance from log write alone. A
canceled benchmark waiter invalidates the run without retracting its accepted
operation; native cancellation must preserve reservation until exact terminal
evidence, as `ClientRouter::cancel_wait` already requires. Shutdown drops owned
waiters and joins actors, with no hidden pending records or credit leaks.

Scope: the matched fixed-leader Noop returns exactly one receipt per command in
committed order. Generic `StateMachine` alone promises no such order; the existing
public `BoundedStateMachine` contract does promise exact per-command ordered
receipts. This makes the optimization transferable, but native `ClientRouter`
also handles multiple groups, replay/unmatched receipts, immediate cached
results, stale terms, canceled observations and position-indexed cleanup. It
cannot become one global FIFO. A later native path needs per-group ordered log
positions and existing ticket/context/receipt preflight, with fallback for other
paths. No new generic result-order guarantee is imposed.

Acceptance: exact singleton/four-original receipts and every follower frontier;
malformed admission and duplicate live IDs reject atomically; reordered, missing,
duplicate, wrong and extra result vectors emit no partial success; chunked commit
and checkpoints preserve remaining prefix; canceled waits do not withdraw writes;
queue/set bytes and original counts stay bounded; all current maintenance,
snapshot recovery, fmt and strict checks pass. Compare this FIFO-only candidate
before another change so its contribution is visible.

## Later candidate: owned request result aggregation

One request may own its original ID list, checked receipt slots/bitmap, remaining
count and one final result sender. Each Core/application result still carries its
original ID and is matched to exactly one slot. The caller verifies all original
results and completes only when every slot is valid. Cross-request Core batches
do not combine request completion ownership. Duplicate/unknown IDs, missing slots
or malformed order never fabricate counts or complete early.

This is an independent host optimization, not parity with upstream's response
channels. Start with one bounded request object and existing precise correlation;
do not introduce an arena or routing trait solely for a benchmark. Native wire
clients currently issue independent singleton requests, so aggregating unrelated
client responses changes their latency and is outside this slice. Transfer first
through existing Rust embedding batches; any later native multi-command API must
retain individual admission, outcome, cancellation and budget contracts.
