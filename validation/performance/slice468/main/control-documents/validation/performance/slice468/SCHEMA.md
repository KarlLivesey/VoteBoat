# Slice468: reuse an exclusive client's empty heap wait buffer

Before-edit experiment plan.467 restored exact465/production455 after every
selected466 Linux rate/p99 pair regressed. Saved455 CPU sampling identifies
client wrapper physical self1317/8395 (15.688%) and overlapping channel/wait/
construction contexts. The wait-vector context alone is small; these samples
do not establish allocator causation or a predicted speedup. Source inspection
identifies one avoidable allocation/free pair per request: the heap Vec of
per-original receivers is rebuilt despite sequential writes by each client.
Two independent ownership audits find no material blocker to bounded reuse.

Scope/data/API: private matched VoteBoat Client owns sender plus a receiver-cache
Vec and retention limit from arbitrary configured Settings.batch. write requires
&mut self. Each clone copies sender/limit and starts with an EMPTY unallocated
cache; the warmed parent does not prewarm measured worker buffers. First use
still allocates inside timing. Request originals Vec, every independent pinned
Tokio oneshot, original-ID validation and ordered full128-bit outcome checks
remain. No channel pool, inline receiver representation, mutex, new dependency,
unsafe code, production/provider/WAL/wire change or result elision.

Ownership/transitions: a private RAII lease borrows the cache for exactly one
write. Reserve exact current width only when needed, avoiding geometric growth
that would defeat arbitrary-width or singleton retention. Create all original
channels before sending the same complete request, then drain/await receivers
in original order. On successful completion, invalid ID, send error, wrong or
closed receipt, panic or canceled send/await, drop the current and every
unvisited receiver before releasing the lease. Return only EMPTY storage to the
client. No new await/cleanup task, queue or fairness budget is introduced.
Independent clones retain concurrency; the same handle now requires exclusive
borrowing, matching the actual shared driver's sequential worker use.

Bounds/cleanup/restart: retain storage only if actual Vec.capacity≤Settings.batch;
larger requests remain supported with the same results, using temporary storage
that is freed afterward. This is a retention bound, never a new request gate.
Each live client retains at most its configured number of receiver slots plus
Vec/limit metadata, with no channel endpoints between writes. The parent warmup
handle is charged separately. Standard allocation failure behavior is unchanged.
Client/process drop frees the buffer; no persisted state or recovery authority
is added. All original runtime/actor admission and pending limits stay.

Comparison/risk: shared workload Client::write and local bindings become mutable;
the OpenRaft implementation remains untouched. Added engine-local state can
enlarge the shared enum/client-loop future and affect either engine's generated
code. Record actual current Client/enum/write/client-loop layouts with compiler
and dev/release labels; distinguish historical source replicas from compiled
artifacts. Retained storage, larger tasks/futures, Drain bookkeeping and first-use
cost can outweigh the removed allocation. No performance gain is claimed before
fresh asserted artifacts and repeated same-host OFF/ON/upstream measurements.
Original settings, envelopes, per-original completion, maintenance, runtime1/16,
warmups and throughput/latency boundaries remain unchanged. Disclose this private
host optimization in the comparison contract. Native-service speed is unmeasured.

Checks: retain all70 matched controls including arbitrary widths0/1/4/5/17,
full128 IDs, out-of-order delivery, first/middle receipt errors, empty/closed
send, blocked queue and cooperative fairness. Add actual cache reuse after
success/errors/cancellation, clone isolation/cold start, exact retention bound
and oversized fallback controls. No live receiver may remain between writes.
Capture exact scoped source/modes, run matched tests/format/strict plus the full
enabled root four-profile/standalone hook at zero. Root production is unchanged;
reuse exact467/465 production/network/recovery scopes rather than repeat them.
Commit/push the checked slice, then build genuinely fresh asserted candidate
and qualify Mac, preserving the original455 job and all failed attempts.

Decision: retain only useful measured throughput/latency/memory improvement;
otherwise restore checked467 and preserve the complete experiment. This differs
from rejected436 inline receivers and448 shared channel allocations, neither of
which is reopened. It is not the whole15.688% wrapper cost. Hold the unmeasured
Counter capacity shortcut and the smaller append-summary fusion (166/8395=
1.977% physical self, retained computation still required). Next optimization
must follow actual remaining work or a full bounded-persistence design. The
seven-cell/both-host sustained healthy-margin, durable/headroom/fairness/skew,
placement/shared barriers and P8 goal remains active and incomplete.
