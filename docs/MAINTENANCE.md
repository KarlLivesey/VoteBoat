# Durable leadership maintenance in Rust

`Maintenance<A>` wraps a host application and stores leadership intents and
outcomes in the same group's replicated log and checkpoints. Select it as the
outer application with a distinct schema and a pristine inner application;
existing unwrapped data files require an explicit migration. This does not add
a second persistence owner or change default service formats.

Use matching wire8 peer assemblies. Submit `LeadershipCommand::Begin` through
`Node::propose`, retaining its original operation ID and exact source/target
node, store and configuration identities. Admission alone is not durability;
wait for the normal applied client outcome.

On each relevant advancement, `Maintenance::leadership_action(core)` returns:

- `Transfer(request)`: submit `NodeControl::TransferLeadership(request)`.
- `Complete(command)`: submit the command through the same client router.
- `Wait`: no action is currently justified.

The host owns polling, request tickets, duplicate-action suppression, deadlines
and shutdown. The pure decision method starts no workers and performs no I/O.
Before discarding a waiting client/read, use the existing Node cancellation and
completion contracts. On a transfer deadline, cancel the exact core request
context to release local quiescence; this cannot undo a delivered signal.
The durable intent remains pending for observation or resumption.

Completion proposals are checked at both admission and execution. Only the
exact target, leading in the recorded current term and configuration after a
current-term commit, can create a new completion. A configuration change can
leave an operation pending; an operator may durably cancel it using the original
intent and prepared index. Cancellation stops that intent's retries and does
not assert the target never became leader.

`MaintenanceQuery::Leadership(operation)` through `Node::read` provides a
quorum-backed status result. `record()` and `pending()` expose only local applied
state for the serialized host driver. A completed record is historical: it
proves that the target led and its completion committed, not that it still leads.
Missing local state or a disconnected wait never proves non-execution.

Use `Maintenance::data` to frame normal inner commands. Data and administrative
operation IDs have separate namespaces. The wrapper forwards inner admission,
context checks, receipt limits, queries and checkpoint state. Every log index
reaches the inner application; maintenance entries appear as Noops. A bounded
permanent record table has one pending operation at a time, rejects conflicting
identities, and reserves space for terminal updates. Completed/cancelled records
are retained, including across checkpoint recovery; there is no eviction policy.

Native TCP and QUIC Rust histories cover pending and completed restart, exact
retry, target data writes, version refusal and quorum status. Application tests
cover malformed/checkpoint input, full capacity and torn native journal records.
See [evidence](../validation/baseline/slice196b1/README.md).

The authenticated executable `move-leader`, status/resume commands and their
disconnect tests remain slice196b2. The full maintenance milestone196 is not
complete until that operator path is implemented and tested.
