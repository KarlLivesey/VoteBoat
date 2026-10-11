# Transaction applications

The first P8 transaction slice adds role-aware replicated applications for a
bounded transfer between two account groups. The native protocol driver is the
following deliverable; these applications alone do not provide a usable cross-group
transaction service. [Implementation status](IMPLEMENTATION.md) records actual
qualification separately from planned work.

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
