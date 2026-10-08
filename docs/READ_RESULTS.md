# Bounded read results

`application::BoundedReadableStateMachine` contract 1 extends the optional
`ReadableStateMachine` capability. Counter and host implementations use the same
public interface. Validation reports retained query allocation capacity, a result
bound including inline storage, and actual nested result allocation capacity.
Validation and immutable read callbacks perform no I/O and do not mutate applied
state. As with application receipts, a trusted provider must honor its declared
bounds; a provider that lies can allocate transiently before detection and is
fenced rather than allowed to publish a successful result.

`runtime::ReadRouter<R>` owns results from original `ReadReady` effect leases.
The host supplies the selected application and the query for that exact original
request. This router does not create or queue read invocations before quorum
establishment. The embedding host still budgets those pending queries, allocates
monotonic per-core `ReadRequestId`s, correlates original queries, handles read-step
errors, and cancels abandoned reads through the core. A production facade that
owns that full invocation lifecycle remains pending.

Construction selects one `RuntimeOwner`, a fresh host-reserved
`ReadRouterGeneration`, and node/per-group result and byte ceilings. Defaults are
4,096 results, 64 per group, 16 MiB retained bytes, 64 KiB nested query capacity,
and 1 MiB per result. Hard ceilings are 65,536 results, 1 GiB retained bytes,
1 MiB nested query capacity and 16 MiB per result. Inline query storage, nested
query capacity, declared result bound and held-record metadata are conservatively
reserved before execution. Router maps/ready slots have fixed count ceilings;
these byte charges are payload/record bounds rather than process RSS guarantees.
Results have their own budget, so a polled reply does not retain a group effect
lease or prevent unrelated WAL/control work. Per-group limits prevent one group
from occupying every configured node result slot when configured below the total.

`submit` validates the exact live owner/effect/barrier and capacities before
calling the application. `EffectOwner::complete_effect` consumes the original
one-use authority immediately before the immutable read callback, on the serialized
group owner. It checks current leadership, term, configuration, original request
context, committed boundary and application catch-up. This is the existing
quorum-backed protocol, with no clock lease, cached authority or new quorum rule.
The barrier index is a contiguous committed prefix captured for this invocation;
it is not a global timestamp. A returned diagnostic barrier is consumed evidence,
never authorization for a subsequent read.

When the callback has not run, `ReadSubmitError::Rejected` returns the original
query and lease intact, including overload, wrong binding/effect and insufficient
application catch-up. Overload and catch-up preserve retryable authority for
that same invocation. A runtime/provider failure can invalidate authority even
while the original query remains untouched; a fenced owner must recover rather
than retry its old barrier. A callback query
error is an owned `Err(ApplicationError)` result: authority is already consumed
and it cannot be reused. An invalid actual result bound fences the owner and
returns `Failed`, with no successful output. The defensive path where owner
completion fails after executing the callback also fences and discards the failed
lease; it never returns the consumed query as retryable.

`poll` transfers an opaque, non-cloneable `ReadResults` envelope. It exposes the
exact scoped ticket, effect, original barrier and borrowed result. All conservative
credits remain held until `complete` consumes that original envelope once. A
foreign/stale completion returns the envelope intact. Once complete returns the
result, the receiving host owns its memory under a separate budget. Consumer
completion establishes no new persistence, quorum or delivery evidence.

`close` rejects new submissions; accepted results still poll and complete. Drain
requires all exported envelopes to return. Dropping an exported envelope cannot
silently release credits. Runtime/router replacement requires fresh identities;
restart uses the new persisted store session and new runtime generation. These
volatile result sequences are allocation identities, not persisted watermarks or
contiguous protocol progress. A read completed before an owner failure remains a
valid result of that original invocation and can still be consumed. Pending
unexecuted invocations belong to the host/future facade, not this result queue.

Six downstream host conformance tests exercise one-use authority, original
allocation ownership, application catch-up, node byte/per-group overload,
independent group progress, query failures, invalid provider fencing,
wrong effects/runtimes, foreign outputs, consumer-held credits and close/drain.
Native three-node/100-group WAL/TLS and native-only network histories now execute
and consume reads through this same guard, including quorum loss, replacement,
reconnection, snapshots/checkpoints and actual-file recovery. These finite Linux
histories do not prove arbitrary schedules or macOS behavior.
