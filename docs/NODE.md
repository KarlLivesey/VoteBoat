# Owning node facade

`runtime::Node` owns explicitly selected `NodeParts`: local EffectOwner,
PersistenceWorker, applications, ApplicationRouter, ClientRouter, ReadRequests,
OutboundQueue, optional SnapshotRouter/worker, and optional PeerParts. It composes
ReplicaDriver and PeerDriver over those same instances. It constructs no clock,
thread, executor, file, socket, TLS configuration or fallback backend.

The TLS feature exposes `native::node::{NativeNode, NativeNodeParts,
NativeLocalParts}` aliases for the native scheduler, timers, jitter, WAL worker,
outbound queue, snapshot worker, connector, transport factory and codec. Hosts
explicitly create or recover stores, bootstrap/register groups, restore and
replay applications, select provider limits and security identities, and start
workers before calling `Node::from_parts`. The separate `native::startup::NativeStartup` convenience API now performs
explicit single-group native filesystem/listener/worker setup and verified
recovery, generic over the application. The counter service and Rust embedding
example use it; see [startup and service](COUNTER_SERVICE.md). The facade itself
remains independent of filesystem configuration and executables. Native recovery continues to use the
existing public checkpoint/WAL recovery contracts; no second authoritative log
is introduced.

`NativeNode<A,C,F>` and `NativeNodeParts<A,C,F>` accept an optional selected
transport factory. The existing connector and NativeTransportFactory defaults
remain, so earlier one/two-parameter uses keep their behavior. Rust hosts can
select `NativeSharedTransportFactory<NativeWireCodec,NativeBufferPool>` produced
by `NativeTransportFactory::with_buffers` without expanding every underlying
Node type argument. Providers are supplied before construction and stay owned
through accepted work; this is not live replacement or automatic startup policy.
See [buffer quotas](BUFFERS.md) for explicit finite budgets and reconnect identity.

Slice145's actual native TCP/TLS100-group assembly selects one shared owner-quota
pool per node. A full host-held peer quota survives disconnect and an authenticated
reconnect while the other peer commits writes and quorum-backed reads in eight
groups. The pressured follower remains at the original applied value until
release, then catches up. Joined shutdown returns all frames and leaves a surviving
host view usable; durable reopen preserves original operation retries and accepts
a fresh write. This is one finite Linux pressure schedule, not per-group fairness,
arbitrary overload/fault proof, selected QUIC pressure or a performance result.

Construction requires quiescent components and checks the local driver contracts,
peer driver scopes, routes and configured voter store identities. Rejection
returns all original selected components without closing them. Networking is
required for groups with remote voters. Networked nodes require snapshot
components for follower catch-up; a recovered compacted group also requires
them. A fresh standalone group can omit snapshots, in which case Checkpoint
returns MissingSnapshots before admission. Extra authorized peers do not gain
voting rights. Selected components cannot be replaced while this facade owns
accepted work.

## Driving and service ownership

The serialized host calls `poll(now, NodePollBudget)`. Both local and network
budgets and monotonic time are validated before either driver does work. Peer
connection, transport and ingress progress precede local worker completions,
owner steps, application execution and original service results. Existing
bounded queues, control reserves and exact ownership credits remain in force.
Supply time, provider wakeups/readiness and fair polling from the host. Read-only
`local()` and `peers()` expose existing timer/deadline, limits and usage views;
no background polling is implied.

Hosts can record the returned result through the public `Observer` contract
after polling. The native service selects `NativeCounterObserver`; the Node
does not own or invoke it, and refusal never changes the original result.
See [bounded observations](OBSERVABILITY.md) for lifetime, limits and export.

`propose` and `read` preserve the original request/query on rejection. Client
proposal admission still validates syntax/dedup capacity twice; applied results
still require the exact committed entry/application receipt. Reads still require
fresh quorum evidence and the exact application barrier. Poll and complete each
opaque client/read output to return its original credits. Cancellation ends a
wait, not the replicated operation. `control` admits explicit Campaign,
Heartbeat or Checkpoint events; it exposes no raw Propose bypass.

This facade adds no durability token, log watermark, protocol generation or
consensus effect. Written remains distinct from Durable. Snapshot publication,
WAL durability, application installation and cleanup preserve their existing
ordered dependencies. Fresh store sessions on restart and host-reserved secure
generation ranges remain authoritative for stale-completion rejection.

`reclaim(max_bytes)` now submits explicit physical cleanup to the selected
PersistenceWorker while Running. The existing WAL thread serializes it between
complete barriers. `poll_reclaim` returns the exact scoped result; pending work
and unconsumed results keep shutdown from draining. Fatal maintenance errors
fence the node and retain the result for explicit recovery. See
[live worker maintenance](WORKER_MAINTENANCE.md).

## Shutdown and recovery

`begin_shutdown` changes Running to Quiescing and closes client/read intake.
Continue polling and consuming outputs. Quiescing waits for all service and
application results, including consumer-held replies. It then closes peer intake
and owner admission/timers and enters Draining. Accepted local work continues;
closed peer coordination resolves remaining outbound work through exact local
terminal outcomes. Drained requires local owner, both drivers, selected WAL and
snapshot work, outbound work and original output credits to drain. Only then are
the selected storage/output handles closed and `into_parts` permitted.

Drained proves handle-level accepted work has drained; it does not prove native
threads have joined. After reclamation, explicitly reclaim native WAL/snapshot
stores and finish the connector's dial worker through their public join APIs.
Scoped provider close must leave unrelated host users/executors intact.

A driver/provider error or explicit `abort` enters RecoveryRequired and fences
the owner. Earlier completed outputs remain available; pending client operations
become Unknown, never a claim of rollback. Polling cannot resume the failed node.
`into_recovery` returns original providers and driver ownership, including retained
leases and accepted worker requests. Drain network ownership independently of the
failed core, discard failed local coordination through ReplicaDriver's public
cleanup, drain accepted worker work and recover authoritative storage explicitly.
Failure never fabricates a healthy drain or releases unseen provider ownership.

## Validation scope

Nine host-provider tests cover constructor rejection/return, missing or mismatched
components, time/budget prevalidation, independent instances, exact held replies,
shutdown, Written-before-Durable abort, rejected persistence, provider failure
and explicit failed cleanup. A native three-node/100-group history selects the
public native aliases with actual WAL files and loopback TCP/TLS, exercises
snapshot catch-up, writes, quorum reads, checkpoint/restart, fresh store sessions,
dedup retries and further writes, and explicitly drains/joins native workers.
It now also physically cleans reclaimed WAL handles before reopening; see
[WAL reclamation](WAL_RECLAMATION.md).
These finite Linux histories do not establish arbitrary schedules, macOS
execution, performance or online membership transitions.

NativeStartup uses `tls.wire_version()` for its message codec, PeerRoster and
TCP/TLS or QUIC connector. The default remains 1. Call
`NativeTlsConfig::with_wire_version` supports versions 1–5; call it before opening to select an exact
matching version on every peer; mismatches fail the encrypted identity hello
before a session becomes Ready. Unsupported values reject at configuration
selection. This is independent of persistent formats and does not enable online
configuration ingress. The counter executable retains its default version 1.

Version 5 also selects the core's multi-batch learner repair during native
assembly. A host using `Raft::with_batched_joint_repair` with the generic Node
must supply a version-5 peer roster; construction refuses missing/older peer
assemblies before service and returns all parts. Repair carries no commitment
claim and never substitutes for a normal election. See WIRE_FORMAT.md.
