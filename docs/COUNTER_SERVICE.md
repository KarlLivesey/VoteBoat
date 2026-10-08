# Native counter service and Rust embedding

`voteboat-counter` runs one independent node per process, using the library's
`native::node::NativeNode<Counter, NativeServiceConnector>`. Three nodes communicate
over mutually authenticated TCP/TLS or optional QUIC, elect leaders using native
timers, and keep separate durable
WALs and snapshots. This is an initial usable local service, with static
three-voter membership and one counter group. Peer addresses are configurable;
the trusted local command endpoint always binds to 127.0.0.1.

## Start three processes

From the repository root, install the pinned Rust toolchain and fetch dependencies
once, then build:

```sh
cargo fetch --locked
cargo build --locked --offline --bin voteboat-counter
mkdir -p /tmp/voteboat-service-demo
```

Run each of these in its own terminal:

```sh
target/debug/voteboat-counter serve create /tmp/voteboat-service-demo/1 1 43000 tests/fixtures/tls
target/debug/voteboat-counter serve create /tmp/voteboat-service-demo/2 2 43000 tests/fixtures/tls
target/debug/voteboat-counter serve create /tmp/voteboat-service-demo/3 3 43000 tests/fixtures/tls
```

The fixture keys are deliberately public and intended for this loopback demo.
You can supply your own directory containing `ca.der`, `node1.der` through
`node3.der`, and the local node's `nodeN-key.der`. Certificates need matching
`nodeN.voteboat.test` DNS names and client/server authentication usage. Keys are
PKCS#8 DER. Each file is limited to 64 KiB. Only the local private key is loaded.
TLS authenticates peer node/store identities; it does not authorize group changes.

The base port must be 1..65432, with peer ports BASE+1..3 and command ports
BASE+101..103 available. Use a different base and data directory for another
local demo cluster. An optional peer-address file overrides BASE+1..3.
Data directories encode fixed node/store IDs 1..3, group 1,
configuration 1 and their initial incarnations. Never copy one live store over
another or reuse these demo identities in an existing deployment.

## Select QUIC

Build with the optional feature:

```sh
cargo build --locked --offline --features quic --bin voteboat-counter
```

Append `--transport quic` to each of the three serve commands above:

```sh
target/debug/voteboat-counter serve create /tmp/voteboat-service-demo/1 1 43000 tests/fixtures/tls --transport quic
```

Use the flag for nodes 2/3 and subsequent recover commands too. Each node binds
one UDP peer socket at BASE+NODE; local command ports remain TCP and client
commands are unchanged. Put any PEERS_FILE before the flag. The same roots,
certificate pins, exact identities, codec and native storage/recovery path apply.
QUIC has no dial worker. It uses a fixed 1200-byte UDP payload, bounded per-peer
queues and reliable stream chunks. Established sessions have a five-second idle
timeout, and the host must keep polling. All nodes must select compatible peer
protocols. Omit the flag or use `--transport tcp` for TCP/TLS. A build without
QUIC rejects its flag before creating a store. See
[QUIC ownership and limits](QUIC_TRANSPORT.md).

## Configure peers on different hosts

Pass a final PEERS_FILE argument to serve. The file is limited to 4 KiB and must
contain exactly three unique node IDs, each with a numeric socket address and its
TLS server name:

```text
1 10.0.0.11:43001 node1.voteboat.test
2 10.0.0.12:43002 node2.voteboat.test
3 10.0.0.13:43003 node3.voteboat.test
```

For node 1, on the host owning 10.0.0.11:

```sh
target/debug/voteboat-counter serve create /your/data/node1 1 43000 /your/tls peers.txt
```

Use node 2/3 and their own data directories/credentials on the other hosts. The
local entry selects the peer listener; other entries select authenticated routing
hints. Server names must match the supplied certificates. The smaller node ID
dials the larger, preventing duplicate connection direction choices. Recovery
can use new addresses/names with the same authorized bootstrap/store identities;
addresses cannot change membership or grant voting authority. Run client commands
locally on the relevant host; the command port is not a remote service API.
Actual acceptance tests use explicitly configured non-default loopback endpoints.
Separate-host deployment has not been exercised here.

## Write, retry and read

Inspect local roles if needed:

```sh
target/debug/voteboat-counter client 43000 1 status
target/debug/voteboat-counter client 43000 2 status
target/debug/voteboat-counter client 43000 3 status
```

For the local three-process quickstart, use `auto` to find a willing leader
without a manual status lookup:

```sh
target/debug/voteboat-counter client 43000 auto add 1 7
# OK outcome=Value(7) duplicate=false
target/debug/voteboat-counter client 43000 1 add 1 7
# OK outcome=Value(7) duplicate=true
target/debug/voteboat-counter client 43000 auto read
# OK value=7
```

An operation ID is a nonzero u128 and the delta is an i64. Keep the same ID and
payload when retrying. A timeout, disconnection or Unknown response can follow an
accepted write that later commits. A changed payload under the same ID yields the
application's conflict outcome. Read success requires a fresh quorum barrier and
application completion; a live process or local applied value is insufficient.
Followers reject service writes/reads. Automatic routing retries the exact original
command only after a failed connection attempt or the explicit `ERR NOT_LEADER`
reply (including an invocation rejected before proposal execution). It stops on
Unknown, incomplete/invalid replies, connected I/O failures, or other errors. It
never automatically resends an uncertain write to another node. A lost write
reply prints Unknown; retry manually using the same operation ID and delta.

Automatic mode scans the three local command ports with one active socket, at
most 100 rounds, a ten-second absolute observation deadline and bounded reply
storage. Partial reply progress cannot reset that deadline. Each successful read
still obtains a fresh quorum barrier. Automatic mode accepts only add/read;
status, checkpoint and quit require an explicit node ID. Explicit IDs remain
available for writes/reads too. Routing is limited to the local quickstart; it
does not discover or forward to command endpoints on other hosts.

`status` shows local role, term and commit index; it is a diagnostic, not a
linearizable application read. `checkpoint` reports admission, not durable
checkpoint completion:

```sh
target/debug/voteboat-counter client 43000 1 checkpoint
target/debug/voteboat-counter client 43000 1 quit
target/debug/voteboat-counter client 43000 2 quit
target/debug/voteboat-counter client 43000 3 quit
```

A successful `quit` response acknowledges shutdown intake. Wait for each process's
`stopped ... workers_joined=true` line and successful exit to establish completion.
Shutdown drains admitted work and explicitly joins WAL/snapshot workers and the
TCP dial worker when selected.
An unsuccessful exit requires recovery of authoritative files; it does not claim
healthy drain. `recover` never creates a missing store, and `create` refuses an
existing store. Restart using the same directories and node IDs:

```sh
target/debug/voteboat-counter serve recover /tmp/voteboat-service-demo/1 1 43000 tests/fixtures/tls
target/debug/voteboat-counter serve recover /tmp/voteboat-service-demo/2 2 43000 tests/fixtures/tls
target/debug/voteboat-counter serve recover /tmp/voteboat-service-demo/3 3 43000 tests/fixtures/tls
```

Retry operation 1 and read from the newly elected leader: the value remains 7.
You can also terminate a leader abruptly, wait for the other two to elect a
replacement, then recover the terminated node from its original directory.

## Embedding in Rust

The executable uses public library contracts throughout. Its setup helper now
selects `native::startup::NativeStartup`: typed node/store/bootstrap identities,
create/recover mode, data directory, listener, peer addresses/pins/names, TLS
configuration, election seed and node-driver limits. Call `open` with a fresh
application, an explicit host WorkerWake and the initial MonoTime for that
host clock domain. Startup verifies bootstrap identity,
restores the pinned checkpoint, replays committed state and constructs the selected
native Node providers. It supports any application implementing the existing
public proposal, bounded-read, checkpoint and receipt contracts. Original `open`
selects TCP/TLS. Use `open_with_protocol(NativePeerProtocol::Quic, app, wake, now)`
for explicit QUIC selection, or TcpTls through the same method. It returns the
same facade with NativeServiceConnector; reclaim its optional TCP dial worker
after close/drain. QUIC has no dial worker to join.

This convenience path selects one group and native providers with their default
storage/queue limits. Use `runtime::Node::from_parts` for shared multi-group stores,
different providers or other provider limits. Startup creates no global runtime.
A failed open returns the application and an explicit cleanup handle: poll
`try_cleanup` until true to close/join any workers and release the listener/WAL
lock. Files might already have been initialized or recovered; cleanup does not
roll them back, and recovery remains an explicit choice.

A complete standalone Rust embedding is
[examples/embedded_counter.rs](../examples/embedded_counter.rs). It uses the same
public startup API, then submits and consumes original proposal/read tickets and
explicitly joins workers:

```sh
cargo run --locked --offline --example embedded_counter -- create /tmp/voteboat-embedded tests/fixtures/tls 1 7
cargo run --locked --offline --example embedded_counter -- recover /tmp/voteboat-embedded tests/fixtures/tls 1 7
# Retry: duplicate=true and linearizable_value=7
cargo run --locked --offline --example embedded_counter -- recover /tmp/voteboat-embedded tests/fixtures/tls 2 3
# New operation: linearizable_value=10
```

The embedding example is a single-voter group with its own directory. The service
quickstart above demonstrates three-process replication using the same startup
API. A recovered applied counter is not current leadership: the example waits
for the new election's durable leader state before submitting service work.

The service loop in [src/bin/counter_service.rs](../src/bin/counter_service.rs)
shows the complete ownership flow for a Rust host:

1. Call `propose(ClientRequest)` or `read(group, query)` and keep its exact ticket.
2. Fairly call `poll` with local monotonic time and bounded budgets.
3. Match each `poll_client`/`poll_read` result to its original ticket, then consume
   it with `complete_client`/`complete_read` to return its reserved capacity.
4. Cancel observation explicitly on timeout, while still consuming late outputs.
5. Call `begin_shutdown`, continue polling/consuming until Drained, reclaim parts
   and explicitly join the selected native workers.

The counter is a sample application, not a restriction on the library. See
[Node](NODE.md), [client ownership](CLIENTS.md), [read ownership](READ_REQUESTS.md)
and [application results](APPLICATION_RESULTS.md) for embedding contracts.

## Bounds and current limits

The command endpoint is a trusted local-user interface without client TLS or
application authentication. It accepts one active connection, one command of at
most 256 bytes per connection, a five-second observation deadline and bounded
responses. Parsing, partial I/O and response backpressure do not block Raft polling.
A timed-out pending request cancels its wait; exact tickets prevent a late output
from becoming another connection's reply. All consensus/provider queues retain
their own existing bounded credits and control reserves.

Peer addresses and TLS names are configurable. Rust startup is generic over the
application, while the CLI still selects a fixed three-voter counter bootstrap.
Remote client routing, a generic multi-group configuration loader, richer
application protocols and operational packaging remain work. Counter deduplication and WAL capacity are
bounded; manual checkpoints do not automatically reclaim physical WAL bytes.
Online membership, recursive responsibilities and split/merge remain unfinished.
Linux process tests cover leader loss, quorum loss, retry identity, checkpoints,
restart, command limits and worker joins. Routing fault tests cover exact command
preservation, unavailable/non-leader candidates, uncertain/malformed replies,
non-retryable errors and an incomplete trickled reply that cannot extend the
absolute deadline. macOS is a target but was not run here.
This is not a production consensus release or a performance claim.

Run the service acceptance tests with:

```sh
cargo test --locked --offline --test counter_service --test startup
```

## Explicit native member restart for Rust hosts

NativeMemberStartup provides the native assembly for an already durably assigned
learner or changed member. Wrap a NativeStartup in Recover mode with a bounded
provisioned_stores map of exact node/store identities, then call
open_with_protocol with TCP/TLS or QUIC, a fresh application and the explicit host
wake/clock. Select membership wire format 2–6; witness controls require at least 3,
multi-batch repair selects 5 and snapshot repair selects 6. NativeStartup's
original static open methods retain their existing rejection of dynamic journals.

Keep the original bootstrap; it is immutable history, not the current voter map.
The provisioned map includes the local store and exactly the peer entries whose
addresses/pins/names you supply, with at most 1,024 total stores and 1 MiB of retained
peer certificate/name allocation. It may include future/retired peer credentials.
Only verified durable membership determines local voting status and active peers.
The local exact store must exist in both committed and accepted membership.
A learner stays non-voting; a removed local member is refused. Address hints cannot
bootstrap, enroll or activate it. Member startup never falls back to Create.

The active roster is derived from the recovered core's committed, accepted and
rollback-reachable replica sets. Every required peer needs its exact provisioned
store and TLS credentials, including an old voter absent from an uncommitted final
head. After final commitment, obsolete provisioning may be omitted. Extra
provisioned routes are retained for later admission but grant no votes or active
roster membership. TCP dial authorization and QUIC pins use that same provisioning
map; no identity is guessed from the original bootstrap voter set.

Recovery uses recover_member_replica to verify checkpoint pins, restore the fresh
application and replay committed history before returning the Node. It uses the
same native WAL/snapshot workers, codec, runtime and cleanup contract as static
startup. Recovery can advance store/snapshot sessions and restore the returned
application before a later rejection; cleanup is not rollback. Poll the returned
NativeStartupRejected::try_cleanup before reopening files. Successful shutdown
must drain and reclaim/join the selected workers as usual. No new durable token,
generation, wire/store format or serving authority is introduced.

Eight downstream tests cover learner/joint/final and compacted restart over both
protocols, restored retry state, rejected learner campaign, missing rollback peers,
wrong peer incarnation, uncommitted local assignment, removed local membership,
missing checkpoint data, pre-I/O mode/version rejection, and late worker/socket
cleanup. These histories seed native durable journal fixtures; they do not prove
online distributed creation/commit of those configurations. The counter CLI still
uses static startup. Trusted checkpoint enrollment and enforced counter envelopes
are implemented below. Service administration, generic application-envelope
integration and full faulted remote transitions remain pending; public service
mutation endpoints stay gated. Explicit member assemblies receive validated
configuration replication.

For a promoted leader that an older replica still regards as a learner, use
the older replica's owning Node to request an old-view witness assertion:

```rust
boat.control(group, NodeControl::AuthorizeReplication {
    witness, // exact old-view voter/store supplied by the host
    candidate: promoted,
    configuration: promoted_head,
})?;
```

Poll normally and observe the core's `replication_authorization_status`. Admission
is not authorization; Pending/Granted are volatile local observations. The grant
permits only exact-store/head replication, with ordinary log/snapshot/durability
checks. Hosts choose a trusted old witness and drive expiry: queue
`NodeControl::CancelReplicationAuthorization`, observe its execution, then retry
with a fresh context. Cancellation is queued, not immediate revocation. If all
old witnesses are unavailable or have compacted the required base, this exchange
cannot authorize catch-up. See the [authorization contract](REPLICATION_AUTHORITY.md).

## Explicit learner enrollment from a trusted checkpoint

Rust hosts can provision a new learner with
`NativeMemberStartup::enroll_snapshot(&image, &mut fresh_application)`. Select
`NativeOpenMode::Create` for new native files. Supply the original bootstrap,
exact provisioned stores/credentials and a committed application snapshot whose
stable membership assigns this exact node/store as a learner. The host must
establish the source's authority and commitment; a checksum is not evidence of
remote consensus. Joint membership and voter imports are refused by this path.

Enrollment opens no listener and starts no workers. It validates the application
on a clone, publishes the bounded checkpoint, durably pins it, then persists its
log boundary and verifies combined recovery before returning. It preserves retry
state carried by the application's checkpoint. Set the mode to Recover and call
`open_with_protocol` to start the resulting member over TCP/TLS or QUIC.

A lost completed-enrollment reply can be resolved with Recover and the same
image and a fresh application. Exact completed imports leave the log unchanged;
a different image or an already active/progressed store is refused. Interrupted
publication/pin/log transitions are resolved through provider recovery and the
same image. Errors do not roll back files. If initial file creation stopped before
both the bootstrap and snapshot store were initialized, normal Recover refuses
that incomplete setup: inspect it explicitly; there is no implicit creation or
replacement of a possibly enrolled store. Native file initialization is not a
cross-store transaction.

The shared `snapshot::enroll_learner_snapshot` composition also works with host
LogStore and SnapshotRetention implementations. It requires an explicitly
bootstrapped, otherwise empty destination or the exact completed import. This
is an offline trusted handoff for embedding; counter command endpoints and remote
configuration delivery are still gated pending administration/envelope integration
and fault-tested online transitions.

## Local configuration operation status and counter envelope

Query an exact nonzero configuration operation ID on a specific local node:

```sh
target/debug/voteboat-counter client 43000 1 configuration-status 42
```

The reply labels its evidence `local_durable`, separates the committed prefix
from the durable log end, and reports committed and accepted phases separately.
`inconclusive_local_absence` cannot establish cluster-wide absence or authorize a
replacement operation. `finalize_requires_authorization` is a planning hint;
this endpoint submits no final record. It is available on followers as historical
local evidence, without claiming a fresh quorum-backed read. Automatic client
routing remains restricted to application add/read commands.

The service binds schema 1, eight-byte commands and a 330,032-byte application
checkpoint envelope to Counter's configured 10,000 retained operation IDs. Counter
admission reserves pending IDs, application refuses further distinct IDs at
capacity, and restore requires the identical configured capacity. Retries keep
their original outcomes. `Counter::readiness_requirements` reports the whole
configured lifetime envelope (32 + 33 * maximum operations), including when the
counter is empty; `validate_readiness_requirements` rejects a smaller declaration
or a different schema. Service startup compares this declaration with its selected
native command/checkpoint payload limits before opening resources.

A full counter checkpoint round-trips through native wire versions 1–4 in tests.
This is the application payload bound; growing membership metadata and retained
configuration operation IDs still require the selected codec/transport checks
before each configuration mutation. General host application envelope enforcement,
service mutation endpoints and remote lifecycle release remain work.
