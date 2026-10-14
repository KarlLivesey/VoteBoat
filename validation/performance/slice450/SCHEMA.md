# Slice450: select the replication prefix before allocating its owned copy

Before editing: current446 profiles identify entry cloning and movement. Mac
append_for contributes41.37% of sampled LogEntry-clone leaf CPU; Vec spec_extend
contributes16.69% of memmove-leaf CPU. Linux entry-clone/to_vec contexts remain.
These are overlapping sampled contexts, not a claimed cause or speedup. Source
append_for currently starts an empty Vec and pushes cloned entries, repeatedly
growing and moving its allocation. Rejected448 channels remain restored to446.

Data/API: after the existing outstanding-request and snapshot branches, borrow
the durable suffix, bounded by the unchanged configured replication count.
Scan the existing37-byte entry/256-byte envelope rule to select the exact
contiguous prefix, then clone only that slice with to_vec. Keep out-of-range
offset behavior equivalent to old iterator skip (empty rather than new panic).
No public/provider/wire/WAL/format/default or dependency change; no new owner,
resource pool, observer phase, queue, lookahead or synchronization policy.

Transitions/order: first entry exceeding the existing byte budget returns the
same error before allocating a context or inserting a request. Empty heartbeat
still allocates no entry storage. Partial byte prefix and count clipping select
the same indices/terms/payloads. Request end/context/configuration and leader
commit are installed at the same point. Outstanding retries remain untouched:
they already clone the original range with to_vec and preserve its obligations.

Ownership/failure/restart: the exclusive core owner holds the immutable source
through both scan and clone; no callbacks or mutable transition intervene.
The effect owns independent entries and payloads as before. Standard allocation
failure behavior remains; existing durable gates, cancellation, partial progress,
stale contexts, configuration, snapshot recovery and cleanup are unchanged.
Exact vector capacity does not exceed the old growth capacity; actual retained
byte charging still observes capacity and existing reservations stay conservative.

Tradeoff: at most1024 entries are scanned twice instead of once. This does not
remove entry/payload cloning or retained-prefix shifting. No immutable payload,
VecDeque/public log representation or another rejected-channel variant is added.

Acceptance: differential old-loop versus new effects for empty, first oversize,
partial byte prefix, exact37/256 boundary, arbitrary/nonempty/configuration
payloads, counts1/64/1024 and non-power-of-two lengths; ownership/capacity,
unchanged retry contexts and acknowledgement/snapshot windows, resource budgets.
Relevant core/durable-service tests and full enabled formatting/four strict root
profiles/standalone strict at zero. Publish each checked slice, then fresh
asserted artifacts and both-host repeated paired bulk/singleton versus446 before
retention. Keep every raw failure/regression; no benchmark-setting change or
full P9 acceptance claim. Linux/Mac P9 stays active and Pi stays stopped.
