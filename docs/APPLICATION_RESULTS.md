# Bounded application results

Application receipt contract 1 adds optional `BoundedStateMachine` and
`ApplicationReceipt` capabilities to the existing `StateMachine` seam. The native
Counter and downstream host types use the same contract. Wire, WAL, checkpoint
and TLS formats are unchanged. No new dependency or hidden executor is introduced.

## Application capability

A bounded application declares `receipt_bytes_bound(entries)` without mutation or
blocking I/O. This is an upper bound on the returned vector's full capacity bytes,
including every receipt's nested retained allocation. Each Command produces
exactly one receipt, in input order, with its original log index and operation ID.
No-ops and other non-command entries produce none. Receipt `nested_bytes(limit)`
excludes inline storage and must account for spare nested capacity too.

Successful apply advances exactly through the last contiguous input index.
Failure must leave application state unchanged. Host implementations are trusted
in-process code: validation can detect misreported results after return, but
cannot sandbox allocations, partial mutations, blocking, panics or false reports.
The Counter uses exact vector preallocation and its existing atomic batch/dedup
semantics. Its receipt bound covers output storage; Counter state, dedup maps,
transactional working copies and checkpoint buffers need separate budgets.

## Fixed owner-side routing

Construct `runtime::ApplicationRouter<R>` for one exact `RuntimeOwner` and a fresh
host-reserved `ApplicationRouterGeneration`. The host selects the correct
group's application and keeps it serialized with the corresponding effect owner.
The router owns no application or store and cannot establish that a host supplied
the right application merely from its applied index.

`submit(owner, lease, application, now)` accepts only a live, nonempty Committed
lease. It checks the runtime, effect lifetime, exact entry contents against the
core's durable log/commit boundary, contiguity and the application's previous
applied boundary. It then checks count, byte and class capacity before invoking
the application. This call is synchronous on the serialized owner; no concurrent
intake can spend its reserved output capacity. Wrong effects, old owners,
overload and excessive declared output return the whole lease before application.
The caller retains that lease under the effect owner's existing reservation and
can retry transient overload; an incompatible maximum bound requires explicit
configuration/application resolution.

After apply, actual count, index/operation identity, applied boundary, vector
capacity and nested capacity must match the contract. The router releases the
Committed lease through the existing effect owner only after these checks.
Neither proposal admission, Written completion nor a local send can create an
application result. Existing exact Durable dependencies authorize Committed
effects; no new durability token or quorum evidence is introduced here.

Application failure, malformed output or effect-release failure after application
fences both router intake and the owner. Invalid/new uncertain results are never
published. The original lease returns for `discard_failed`; the application may
already have advanced, so recover its checkpoint and committed replay under a
fresh runtime instead of retrying that lease. No generic rollback is claimed.
Previously valid queued results remain observable after fencing.

## Output lifetime and ceilings

The returned `ApplicationResultTicket` is a checked local allocation sequence
inside its router binding, never a replicated/durable prefix or a client delivery
receipt. Stored output also identifies its original EffectTicket (including group)
and exact applied-through boundary. That boundary is contiguous application
progress for the selected group, not a node-wide watermark.
The envelope also retains an immutable verified index/term per receipt, captured
from the validated committed entries before releasing the effect. These bounded
records remain charged alongside receipt buffers and permit exact client
correlation after logical compaction removes the live entry. Per-batch receipt
byte limits cover application output; global byte accounting additionally includes
these position records and pending metadata.

`poll()` hands out one opaque `ApplicationResults<R>`. Receipt access is borrowed;
polling retains every slot/count/byte charge. `complete(output)` consumes the
original envelope once and returns its receipts to host-owned storage. Hosts must
budget any further retention separately. An envelope from another router/runtime
returns intact as StaleResult. Envelopes are not Clone and their identity fields
are private. Dropping an exported envelope abandons observation, keeps its charge
until router teardown, and does not undo application. Poll/complete do not prove
client delivery or remote application durability.

Defaults: 64 outstanding batches, 65536 receipts, 16 MiB, with 4 batch slots and
64 KiB reserved for command-free committed control work. One batch allows 16384
receipts and 1 MiB of output. Hard ceilings are 4096 batches, 1048576 receipts,
1 GiB total and 65536 receipts in a batch. Full declared output plus pending
metadata remains charged until completion, even if actual output is smaller.
Control work uses the global ceiling; command work cannot consume the reserves.
The router's map and ready queue are bounded by batch count. Accounting is
conservative payload retention, not an exact allocator/RSS measurement.

Default per-batch count covers a Counter commit spanning the default native log's
maximum retained entry count. Assemblies with larger logs or receipts must choose
compatible per-batch output bounds before service; a ResultTooLarge committed
batch needs explicit budget/application resolution rather than overload retries.

Close stops new submissions; queued/exported results still poll/complete.
Drain application work before closing its router, then drain outstanding outputs.
Close/drop never shuts down an application, store, scheduler or another router.
Dropping a router discards its queued results and observations; committed
application changes remain. Ingress and worker shutdown retain their own contracts.

Router generations are fresh within an owner; runtime replacement changes the
runtime generation, and storage restart changes its persisted store session.
Outputs are volatile. A lost output is recovered through operation-ID retries
and the application's checkpointed dedup state, not by persisting router tickets.

## Evidence and remaining work

Six host-only tests exercise Written vs Durable, wrong effects/runtime/modified
entries, output retention after poll, bulk overload before application, control
reserve and unrelated-group progress, cross-router rejection, retries/content
conflicts, custom nested host receipts, malformed identity/oversized allocation,
partial application failure, byte limits, close/drain and shared application reuse.
The native three-node/100-group histories now apply every live Committed effect
through this router, including TLS partitions/reconnects, snapshots/checkpoints,
application retries/reads and actual-file recovery. Startup replay remains the
separate checkpoint/recovery contract.

Committed application results now feed the separate [client router](CLIENTS.md),
which supplies proposal correlation, pre-proposal dedup/capacity reservation and
unknown outcomes. [ReadRouter](READ_RESULTS.md) supplies bounded read-result
execution and consumer ownership. Pre-quorum read invocation ownership,
reactor/facade assembly, physical WAL
cleanup, membership changes, responsibilities and split/merge remain unfinished.
Finite Linux checks do not establish macOS execution, a full liveness proof,
device power-cut behavior or performance.
