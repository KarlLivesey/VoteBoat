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

## Authenticated counter executable

On a new three-node cluster, append these options to **every** serve command:

```text
--service-access ACCESS_FILE --leadership-maintenance enabled
```

Use the same options on recovery. This explicitly selects counter application
schema2, wire8 and 64 permanent maintenance records. The existing plain counter
profile stays schema1. Do not enable the new profile on existing plain data or
mix the profiles between replicas. The executable currently accepts this profile
without a membership administration plan; the generic Rust wrapper supports
other host assemblies. There is no automatic migration or history eviction.

Use the ordinary authenticated client flags, selecting a current leader node:

```text
client BASE NODE move-leader OP CONFIG TARGET STORE INC --service-tls TLS_DIR --principal ADMIN
client BASE NODE leadership-status OP --service-tls TLS_DIR --principal READER
client BASE NODE resume-leadership OP --service-tls TLS_DIR --principal ADMIN
client BASE NODE cancel-leadership OP --service-tls TLS_DIR --principal ADMIN
```

`OP` is a nonzero u128. `CONFIG` is the expected stable configuration; `TARGET`,
`STORE` and `INC` are the exact target identity, not an endpoint. For the initial
demo configuration, CONFIG and INC are 1 and STORE equals TARGET. New intent
binds the current source; retries preserve its original recorded source.
The automatic `client ... auto` route still accepts only data read/add commands.

Start returns the applied Pending record, not handoff success. All replicas can
resume that durable intent after restart. Each local leader gets one attempt per
intent/term, bounded to five seconds, including an internal completion wait.
Timeout releases local quiescence and request ownership, preserving Pending.
Resume authorizes another local attempt and returns a fresh status read. Status
requires Inspect permission; the other three commands require Admin.

Cancel stops the local attempt before proposing the durable cancellation. If
completion wins the race, cancellation returns the original Completed record.
Retrying an already terminal cancellation uses a fresh read and leaves other
operations alone. Disconnecting a command releases its wait; it cannot undo an
already committed intent. Preserve the operation ID and exact fields on unknown
outcomes, then query a current leader. A Completed record is historical evidence,
not a promise that its target is still leader.

Slice196b2 tests use actual authenticated TCP/QUIC service processes, pending and
terminal restart, a killed target, lost replies, denied mutations, disconnected
and expired status waits, and incompatible plain data. See
[executable evidence](../validation/baseline/slice196b2/README.md).
