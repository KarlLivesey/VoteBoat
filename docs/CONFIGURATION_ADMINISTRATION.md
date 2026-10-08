# Owned configuration requests

`Node::configure(ConfigurationRequest)` admits a typed administrative proposal
and returns a `ConfigurationTicket`. It preserves the original request on
rejection. Admission is volatile; it does not certify persistence or commitment.
The existing configuration journal remains authoritative for learners, joint
consensus and finalization, including expected configuration and operation IDs.

Drive accepted requests through `Node::poll_with_configuration_authorization`.
Its host callback receives the current core and proposal immediately before
execution. It must be bounded and nonblocking, and authorize service scope,
placement/failure domains and the
selected providers' command, snapshot, wire and transport capacities. A callback
cannot waive journal grammar, quorum validation, exact readiness or durability.
The Node additionally rejects remote expansion without peer providers and static
wire format 1, and rechecks every promotion
proof against its live authenticated format-4 peer store/session. Queued work
retains no cached authorization. Ordinary `Node::poll` denies configuration
execution. Application commands retain their application admission checks.

Poll `Node::poll_configuration` for one of these outcomes:

| Outcome | Meaning |
| --- | --- |
| `Committed(position)` | The exact configuration record and term/index are in the selected core's locally durable committed prefix. A joint receipt does not certify a later final record. |
| `NotProposed(error)` | This exact invocation failed before producing configuration persistence. |
| `Unknown(reason)` | Observation ended; accepted work can still commit. Query/recover durable history before deciding what to retry. |

Finalization uses the joint operation ID and expected joint configuration and
requires joint commitment. The ticket identifies an invocation separately from
that durable operation. Runtime steps now expose configuration operation and
proposed position, alongside application proposal steps; those fields alone are
not committed receipts.

`Node::cancel_configuration` cancels the wait, preserving queued or already
submitted work. `begin_shutdown` closes administration intake, preserves observed
committed outcomes and marks unresolved waits unknown. Consume retained results
to let graceful shutdown finish. Abort/failure retains unknown results in
`NodeRecovery::configuration` with the original worker/provider ownership.

The public `ConfigurationRequests` observer provides the same contract to hosts
assembling their own owner loop. Submit to the selected owner, pass its original
steps and current durable state to `observe` after each advancement, and poll
results. Default limits are 1,024 retained requests/outcomes and 16 MiB of retained
record/observer accounting, with one slot per group. The owner separately charges
queued proposals and readiness allocations. A result occupies its slot until
polled; exported results are the host's responsibility. Admission tickets retain
the existing store session/runtime owner and monotonic owner sequence. Replacing
an observer does not reset that sequence; reconstructing an owner requires the
existing fresh runtime identity contract. No new durability token or generation.

Host tests cover durability-dependent receipts, joint/final sequencing, admission
limits, authorization revocation, cancellation without rollback, queued election
work, storage failure and retained results during shutdown/recovery. A native
startup test commits a same-electorate single-voter joint/final transition through
the selected worker, reopens the actual WAL and reconstructs membership through
explicit member recovery. This does not enable dynamic recovery through the
static startup convenience constructor.

Remote configuration-bearing Append and membership Snapshot remain gated. A
native placement authorizer, complete codec/transport capability admission,
durable operation-status/resumption, enrollment/service endpoints and faulted
multi-node add/promote/remove remain required. The supplied authorization callback
is a host composition seam, not an implemented native placement policy. Existing
TCP/QUIC readiness and static-service tests do not establish this complete path.
