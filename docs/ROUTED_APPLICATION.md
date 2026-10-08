# Routed application execution

`routed::RoutedApplication<A, P>` wraps a host-selected checkpointable application
and deterministic partition policy through the existing public application
contracts. It binds one responsibility namespace to one concrete local group.
The grant can name Single, Partitioned or mixed Delegated execution; this wrapper
serves only the grant's directly assigned local buckets. Ordering remains per
Raft group. It creates no threads, sockets, storage or parent transactions.

## Bootstrap and requests

Obtain the grant from a trusted authorized source, such as a quorum-backed
Directory read. A cache entry or redirect alone is not new ownership authority.
Construct the wrapper with the exact local group, grant, fresh application,
partition policy and explicit limits. Constructor rejection returns the original
grant, application and policy. The inner application's group constraint must also
accept the selected group.

Commit and apply `bootstrap_command(max_bytes)` before accepting data requests.
The `VBROWN01` initialization binds the full canonical manifest, group, limits,
inner schema and initial checkpoint. WAL replay and checkpoint restore reject
changed bindings. This establishes a trusted fixed initial assignment; it cannot
activate a transferred owner or override an existing unavailable owner.

Use `encode_routed(hint, key, payload, max_bytes)` and the existing `Node::propose`
API. `VBRCMD01` carries responsibility identity/incarnation, group, adapter and
partition scheme versions, scope, bucket, ownership epoch and route generation.
The operation ID remains the ordinary log command's ID. Admission checks the
context and reserves capacity against pending commands. Ordered apply checks it
again against the committed local grant before invoking the inner application.
Wrong responsibility, incarnation, epoch, scope, bucket, adapter or group cannot
produce an Applied outcome. Route generation is a location hint; a changed hint
does not change the semantic request.

The wrapper retains each successful dispatch's original key and payload, binding
both to its operation ID. An exact retry reaches the inner application's original
result; changed content returns OperationConflict at apply and is refused at
admission. The selected application still owns its result/deduplication state.
Control and rejected commands advance the same inner ordered prefix as Noop,
without invoking an application command. Failed batches publish no partial state.

Use `RoutedQuery` through `Node::read` for quorum-authorized reads. The wrapper
checks ownership before delegating the query. The host application must keep its
query/payload semantics consistent with the routing key; routing cannot make an
indivisible global counter safely splittable. `application()` is read-only local
diagnostic access, not a distributed read authorization.

## Group binding, fencing and recovery

`StateMachine::validate_group` defaults to accepting any group for generic
applications. Directory restricts it to its metadata authority; RoutedApplication
restricts it to its local group and delegates the check to its inner application.
Native startup checks before files/listeners/workers. Node assembly, application
execution, read serving and snapshot/recovery paths check the same contract.
Direct embedding hosts must also validate the binding before using lower-level
application methods, which receive entries without group identity.

`encode_fence(epoch)` is a privileged, one-way local command. The first applied
fence records its operation/index and rejects subsequent data and service reads,
including work admitted before the fence. Retrying that fence returns its original
record. Checkpoint/replay cannot thaw it. Hosts must authorize control proposals
separately; this module does not supply a public network control endpoint.
OwnershipFence records local applied state, not a complete P6 transfer certificate.
Source configuration evidence, target imports, publication, activation and merge
remain separate lifecycle work. There is no unfreeze or reassignment API.

`VBROUT01` checkpoints contain the exact bootstrap binding, applied prefix,
initialization/fence records, inner checkpoint and semantic retry history.
Restore checks schemas, identities, indices, bounded lengths, unique operation
positions, ownership scopes and trailing bytes on a clone before publication.
Capacity is finite: at most 4096 retained data operations, 64 MiB key/payload
capacity, 1 MiB per inner payload, 64 MiB inner checkpoint and 8192 inspected
pending commands. Selected limits can be smaller. No eviction forgets retries;
control records retain space when data history is full. Receipt/read accounting
charges outer inline storage once and delegates nested allocations to the host.
`readiness_requirements()` declares the complete command/checkpoint envelope.

## Evidence and limits

`tests/routed.rs` covers admission/apply ownership, semantic retries, pending
capacity, malformed commands, atomic failure, source fencing, all checkpoint
truncations and a downstream application with allocated receipts/queries/results.
Native tests publish a three-level services/X/orders and services/Y/jobs forest,
read committed grants and run child groups on distinct replica sets {1,2}/{2,3}.
All directory replicas then stop and root/service cache entries are removed.
Children commit/retry/read, reopen from WAL-only and compacted checkpoints, and
return original results. Changed grants/limits refuse recovery. Parent WAL state
is exactly unchanged. Both TCP/TLS and QUIC use the same NativeStartup/Node path;
the host continues polling live roles while others drain.

These are Linux loopback embedding histories, not separate-host/macOS evidence,
a general authorization service, a directory client wire endpoint, dynamic group
creation, shared-group application multiplexing or a split/merge implementation.
