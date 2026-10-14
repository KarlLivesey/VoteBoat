# Slice452: receiver-only heap waits, with expected-ID lookup after await

Before editing: current450 profiles show Linux client-write union21.296%,
receiver polling7.042%, channel construction6.478% and Mac client-write
inclusive9.368% (overlapping whole-launch contexts, not allocator causality).
Client::write stores each validated ID in its request and again beside the
receiver, although the immutable original input remains borrowed for the call.

One candidate/data shape: keep the existing heap Vec and unchanged per-original
Tokio channel. Store only Receiver<OperationId> in waits. After the unchanged
queue send, create an iterator over immutable input IDs. Await each receiver
first, then advance/read its expected ID and compare the full128-bit value.
This avoids duplicate wait-buffer IDs and holding the current expected ID over
a suspension. No inline storage, custom channel, grouping, dispatcher or new
dependency. Only the private matched Client and a diagnostic test change.

Prior decisions:436 combined receiver-only waits with inline storage and read
the expected ID before await; its larger future and measured regressions were
rejected. This candidate keeps heap storage and moves the ID read after await.
Actual future/layout evidence is required before artifact builds.448 channel
grouping remains rejected. The empty-byte clone-call hypothesis was rejected
from exact450 instructions before any edit; no manual clone guard is added.

Ownership/transitions: inputs are borrowed immutable u128 values throughout the
async call. Initial ID validation/channel/request construction order is unchanged.
Each successful construction pushes exactly one receiver. Width0 still submits
the empty request and observes send errors. Invalid IDs stop before queue send;
send failure/cancellation drops all unsubmitted originals and receivers. Accepted
requests stay actor-owned, while canceled waits close their receivers. Closed or
wrong first/middle receipts return immediately and drop unvisited receivers.
Await order, immediate notifications and per-original cooperative budget stay.
No accepted write is undone; there is no new persisted/restart state or API.

Bounds/error order: one receiver per validated input and existing Vec capacity
remain; the request buffer, operation-ID validation, admission/envelopes and
full-value receipt/error text stay. Expected iterator advancement happens only
after a successful receive, never before yield. It does not truncate/hash IDs.
No timing, warmup, batching, maintenance, observer or application change.

Acceptance: preserve existing69 matched controls (arbitrary/nonmonotonic full128
IDs, widths0/1/4/5/17, reversed notifications, early/late zeros, full/closed queue,
accepted/blocked cancellation and first/middle wrong/closed receipts). Add actual
current versus literal450 future-layout diagnostics, wait element/header/capacity
measurements without portable size assertions or inferred physical RSS. Format
and strict zero; exact source/mode scope and root450 functional reuse. Publish
through the complete enabled hook, then fresh asserted artifacts/Mac qualification
and three paired20M bulk/1M singleton rotations against immutable450. Larger
future/memory access or iterator costs can outweigh the saved metadata; no speedup
or retention claim before measurement. Preserve every failure/tail. Full P9 stays
active, Linux/macOS only; Pi stays stopped.
