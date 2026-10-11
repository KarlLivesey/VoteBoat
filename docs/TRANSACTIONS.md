# Transactions

`TransferApplication` and `TransferDriver` compose a bounded transfer between two
account groups through the existing public Node interfaces. The driver advances
actual proposals and quorum reads; host-selected providers own storage and I/O.
[Implementation status](IMPLEMENTATION.md) records platform qualification and the
remaining P8 scope.

Select `TransferApplication` in the existing public Node application map. A
`TransferProfile` binds one coordinator and two distinct participant group
incarnations in canonical order. A `TransferSpec` binds its transaction identity,
debit/credit accounts, positive amount and ten original phase operation IDs.
Account ownership is fixed. Existing safe voter membership changes remain
available; membership and account ownership are different contracts.

The application implements the ordinary state-machine, bounded receipt/read,
proposal-admission and checkpoint traits. Public intake encodes an immutable
Start or queries transaction/account state. Subsequent phase commands are internal
trusted protocol assertions. Their encoded historical positions do not certify
another group's state: the driver must consume exact original Node results or
fresh quorum-backed status before constructing them. Local `status`/`account`
methods are diagnostics; distributed decisions require Node quorum reads.

Prepare retains an exclusive lock and planned balance without changing the
account. Commit applies once and keeps that lock until the coordinator records
that both participants applied. Abort changes no balance; an unprepared
participant retains an abort tombstone. Release removes only the original
transaction's lock. A locked account returns `Locked(transaction)` rather than
an intermediate balance. Timeout or cancellation never decides or undoes a phase.
No cross-group scan or arbitrary transaction program is promised by this profile.

`TransferLimits` bounds account count and lifetime transaction count. Provision
each participant for at least the coordinator's full transaction capacity and
preflight immutable account existence before Start. Each record retains all its
phase outcomes and IDs; records are never evicted. New Start stops at capacity,
while retries and terminal phases retain their reserved space. The coordinator
also reserves participant phase IDs across pending and retained transactions.
Operation IDs may coincide in different groups; conflicting reuse within a
destination group refuses.

Checkpoints retain the exact profile, limits, opening accounts, current balances,
locks, specifications and first phase outcomes/positions. Restore checks the
configured binding and reconstructs the local retained history before publishing
state. The current invocation receipt index and original first phase index are
separate: retries must reconstruct identical dependent command bytes. Application
checkpoint/replay uses the existing selected log and snapshot contracts.

## Rust embedding

Attach `TransferDriver` to an exclusively borrowed slice of 1–64 compatible Nodes.
Every profile group must be hosted, but each Node may host a different subset.
Construction checks profile, role, immutable opening accounts and capacity before
intake. The driver consumes those Nodes' client/read streams, so they must be
drained at attachment. Allocate a fresh `TransferDriverGeneration` for every
attachment under the same `RuntimeOwner`.

Use `submit(spec)` for immutable business intake or `resume(transaction_id)` to
recover the original spec through a fresh coordinator quorum read. Call `advance`
with explicit time and budgets, then `poll_completion` and `complete` the original
output. `Finished` reports the durable decision and original finish index.
`Unknown` requires retry/resume; it never authorizes compensation. An absent
coordinator record yields `Unknown(NoRetainedStart)`.

`close` stops intake while accepted work continues. `stop_waiting` cancels local
observations without changing the durable transaction. Continue progress and
consume outputs until `is_drained`, then shut down and join the host's Nodes and
providers. Dropping the driver performs no I/O or unlocking. `progress` is a local
diagnostic, not a committed phase certificate.

The standalone example uses one physical voter hosting all three durable groups:

```sh
cargo +stable run --locked --offline --example embedded_transfer -- \
  create /tmp/voteboat-transfer tests/fixtures/tls submit 1 17 forward
cargo +stable run --locked --offline --example embedded_transfer -- \
  recover /tmp/voteboat-transfer tests/fixtures/tls resume 1
```

It retains authoritative files for recovery and prints fresh account reads and
worker joins. Native acceptance separately exercises three physical replicas over
TCP/TLS and QUIC. This example is not a remote transaction command service;
broader transaction scope and logical voters remain P8 work.
