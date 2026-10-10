# Native counter service and Rust embedding

`voteboat-counter` runs one independent node per process, using the library's
`native::node::NativeNode<Counter, NativeServiceConnector>`. Three nodes communicate
over mutually authenticated TCP/TLS or optional QUIC, elect leaders using native
timers, and keep separate durable
WALs and snapshots. The quickstart creates static three-voter membership and one
counter group. An explicit [group manifest](MULTI_GROUP_STARTUP.md#counter-executable)
opens multiple groups in the same process and WAL with group-scoped authenticated
data commands. Explicit member recovery can reopen compatible dynamic histories
prepared through the Rust administration APIs. Peer addresses are configurable;
the command endpoint defaults to 127.0.0.1. Its default mode is trusted
plaintext; `--service-access FILE` requires authenticated, scoped
commands. See [principal permissions and client flags](AUTHORIZATION.md).
An explicit [remote command endpoint](#remote-command-endpoints) requires that authentication.
`--peer-credentials FILE` enables authenticated peer-key rotation and restart
checks; see [peer credential rollout](CREDENTIAL_REFRESH.md#counter-executable-peer-rotation).

An opt-in [leadership maintenance profile](MAINTENANCE.md#authenticated-counter-executable)
adds authenticated move/status/resume/cancel commands with durable original IDs.
It requires a new schema2/wire8 deployment; existing plain counter data is not
automatically converted.
The optional [retained-replica drain commands](MAINTENANCE.md#retained-replica-node-drain)
add durable maintenance/reboot with a restored admission gate.
The [membership-drain workflow](MAINTENANCE.md#executable-membership-drain-workflow)
combines an original plan file with authenticated joint/final configuration commands.
`drain-run` drives that single-group workflow and requests checked source shutdown.

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
addresses cannot change membership or grant voting authority. By default, run client
commands locally; configure the authenticated command endpoints below for remote use.
Actual acceptance tests use explicitly configured non-default loopback endpoints.
Separate-host deployment has not been exercised here.

## Remote command endpoints

Use an explicit listener with the same authenticated command API:

```sh
target/debug/voteboat-counter serve create /your/data/node1 1 43000 /your/tls peers.txt --service-access access.txt --command-listen 10.0.0.11:43101
```

`--command-listen` requires `--service-access`, including when binding loopback.
IPv4/IPv6 wildcard listeners are supported; zero ports and multicast addresses
are rejected before files or sockets open. Omit the option to retain the
default loopback listener. The ready message reports the selected address.

Create a separate command endpoint file; these are service routes, not Raft peer
assignments or membership authority:

```text
voteboat-command-peers-v1
1 10.0.0.11:43101 node1.voteboat.test
2 10.0.0.12:43102 node2.voteboat.test
3 10.0.0.13:43103 node3.voteboat.test
```

```sh
target/debug/voteboat-counter client 43000 auto add 1 7 --command-peers commands.txt --service-tls /your/client-tls --principal 2
target/debug/voteboat-counter client 43000 1 status --command-peers commands.txt --service-tls /your/client-tls --principal 1
```

The client requires both authentication flags with `--command-peers`. It uses
the file's numeric addresses instead of deriving ports from BASE_PORT; the base
argument retains its normal validation. Node IDs are 1..4096. The file allows
1..64 entries and at most16KiB, with unique node IDs and addresses, nonzero ports,
non-unspecified/non-multicast addresses and valid TLS names. IPv6 uses brackets.
Blank/comment lines, extra fields and missing explicit targets are errors.
Each listed target pins `nodeN.der`; the principal uses its own `nodeP.der` and
`nodeP-key.der`. The selected name must match the pinned server certificate.
The retained server certificate/name budget is1MiB.

Auto mode scans the supplied nodes in numeric order, with the existing ten-second
deadline and exact non-acceptance retry rules for writes. Automatic reads can
retry a deadline or empty disconnect on another replica; certificate failures
and unknown writes stop routing. Addresses confer no permissions or leadership. The file is
reloaded for each CLI invocation. Optional endpoint discovery is described below;
server listeners are selected at startup. Commands remain TCP/TLS even when Raft peers use QUIC.

To resolve current command addresses from a running service, add
`--discovery-peers advertised.txt` to that service's startup command. The file
uses the same bounded format above and requires `--service-access`. It is an
immutable startup view; restart with a changed file to advertise changed
addresses. Configure reachable addresses, not wildcard listener addresses.

Clients select a pinned source from their own command file:

```sh
target/debug/voteboat-counter client 43000 auto read --command-peers bootstrap.txt --discover-via 1 --service-tls /your/client-tls --principal 1
```

The source address must be reachable in bootstrap.txt. Other listed addresses
may be obsolete: the client resolves its requested targets over the existing
authenticated discovery protocol before sending any command. Every target still
needs its independent certificate pin and TLS name in the client configuration.
An unavailable source, missing mapping or failed authentication stops before
command submission; there is no fallback to obsolete addresses. Discovery and
commands share one ten-second invocation deadline. Source selection is explicit;
the client creates no background resolver or cache across invocations.

Readers, writers and administrators may inspect endpoint hints in their service
scope. Hints confer no membership, ownership or quorum authority. Endpoint
requests do not touch Raft. Recursive responsibility manifests still require a
separate authority service; this option does not implement that service.

Executable TCP/QUIC cluster tests use wildcard listeners and non-default loopback
ports, verify access denial and TLS-name rejection, then preserve same-ID retries
across leader loss, checkpoint and restart. Separate-machine and macOS execution
remain distinct validation requirements.

## Recover an existing member journal

For stores whose membership has been changed through the Rust APIs, explicitly
select member recovery on **every participating service process**:

```sh
target/debug/voteboat-counter serve recover-member /your/data/node1 1 43000 /your/tls peers.txt
```

Use node 2/3 with their own directories, and append `--transport quic` for QUIC.
This mode selects exact wire format 7, including membership reception, retained
learner repair and committed stable checkpoint learner recovery. Ordinary `create` and `recover` retain wire
format 1 and static recovery semantics; incompatible wire selections cannot form
peer sessions. Stop the participating processes before switching their mode.

Member recovery verifies the existing WAL, pinned checkpoint and exact local
assignment through `NativeMemberStartup`. It neither creates missing files nor
infers assignment from peer routes. Removed local stores are rejected. The CLI
retains the original bootstrap of voters/stores 1..3, group/configuration 1 and
initial incarnations. Default/legacy provisioning covers identities 1..3;
`--deployment` declares additional exact stores/routes as described below. This
does not rewrite bootstrap or assign a replica. Trusted startup administration
plans are available below. Public configuration mutation endpoints remain gated.
Offline enrollment is available below.
Rust hosts can already supply explicit provisioning and trusted enrollment images.

The same enforced counter envelope, local commands, durable configuration-status
observation, checkpoint drain and retry IDs apply. Configuration execution remains
denied by ordinary service polling. Without an explicit `--admin-plan`, no automatic
finalization is implied by starting a member process. Witness queries remain
explicit host controls; hosts must still drive them when
their recovery history requires them.

Executable TCP/TLS and QUIC histories reopen prepared committed joint and final
views, commit/read a counter write, drain a real checkpoint and reopen all three
processes. They preserve retry deduplication and configuration observation; the
final-view learner cannot lead or accept a write. These seeded histories validate
service recovery, not online enrollment or the distributed proposal lifecycle.

## Explicit member deployment

Use `--deployment FILE` instead of PEERS_FILE with member recovery or offline
enrollment. Trailing named options may appear in any order; duplicates reject:

```sh
target/debug/voteboat-counter serve recover-member /your/data/node4 4 43000 /your/tls --deployment deployment.txt --transport quic
target/debug/voteboat-counter enroll create /your/data/node4 4 43000 /your/tls /your/data/node1 1 --deployment deployment.txt
```

The UTF-8 file starts with an exact version header, then five whitespace-separated
fields per line: NODE STORE_ID STORE_INCARNATION SOCKET_ADDRESS TLS_SERVER_NAME.
For example, after the original node 3 has been durably retired:

```text
voteboat-deployment-v1
1 1 1 10.0.0.11:43001 node1.voteboat.test
2 2 1 10.0.0.12:43002 node2.voteboat.test
4 404 7 10.0.0.14:43004 node4.voteboat.test
```

Node IDs are 1..4096. Store IDs are nonzero u128 values; incarnations are nonzero
u64 values. No blank/comment lines or additional fields are accepted. The file
is limited to 64 KiB and 1,024 entries; duplicate nodes, exact store identities
and socket addresses are refused. Addresses must be numeric, non-unspecified and
have nonzero ports. Names must be valid TLS server names. The local node must
appear. Provider/roster capacities can impose lower deployment limits.

Supply `ca.der`, every declared peer's `nodeN.der`, and the local node's
`nodeN.der`/`nodeN-key.der` in TLS_DIRECTORY. Only the local private key is loaded.
Peer certificate/name retention is checked incrementally against the native
1 MiB budget. Declared store identities must match the exact durable assignment;
certificate pins and names are verified during the TLS handshake. A source's declared identity is verified
against its authoritative files during enrollment; IDs are not inferred from
the node number.

The declaration supplies provisioned credentials/routes only. Every accepted,
committed or rollback-required member must still be provisioned; a retired peer
may be omitted after final commitment permits it. Extra provisioned peers receive
no membership, votes or serving authority from this file. Missing member files
remain an error. Static create/recover refuse this option; the original bootstrap
is preserved even when its retired nodes are absent from current provisioning.
Other groups/bootstrap configurations use explicit Rust assembly.

The default local command port remains BASE+100+NODE; reject any base/node combination
that exceeds 65535. Explicit `client BASE NODE ...` supports these node IDs.
Without a command endpoint file, `auto` searches demo nodes1..3. With
`--command-peers`, it searches that explicit list, including later member IDs;
the list itself cannot change membership. Runtime configuration mutation remains gated.

TCP/TLS and QUIC executable histories enroll node 4 at store 404/incarnation 7,
verify its imported counter/retry state, reject changed identity, observe its new
committed replication, drain and restart. The fixture assigns the public node-3
alternative certificate to node 4 alone with its matching DNS name, while node 3
is absent. Source membership is prepared; this is not online configuration
proposal commitment or physical failure-domain validation.

## Write, retry and read

To explain the current configured quorum for hypothetical voter IDs:

```sh
voteboat-counter client BASE_PORT NODE explain-quorum 1,2 0 16
```

Use `-` for no IDs. The last two arguments are a zero-based row offset and count
(1–16). The output reports accepted stable/next configuration IDs and
`evidence=hypothetical_nodes`; it does not contact the listed voters or prove a
live quorum. In a joint configuration both trees must be satisfied. Compare the
configuration IDs across pages and restart inspection if they change.

Rows are `index:parent:view:rule:edge_weight:observed/required/total:satisfied`.
Only satisfied immediate children contribute their weight to a branch. Five
IDs in a nine-voter hierarchy can therefore still fail if two required sites
lack their own quorum. Unknown IDs do not contribute. This read-only command
requires Inspect permission when authentication is enabled and works on a
follower without a live quorum. It cannot change membership or authorize writes.

For local process counters, run `voteboat-counter client BASE_PORT NODE metrics`.
These counters reset on restart and are labelled `evidence=local_volatile`;
they do not establish quorum or durability. See [observability](OBSERVABILITY.md).
For bounded recent operational events, use `client BASE_PORT NODE events 0 0 16`,
then continue with the returned session and next cursor. Events reset on restart;
overflow and cursor gaps are reported explicitly.
Use `client BASE_PORT NODE timings` for local poll and connection duration
summaries. These include interrupted connections and label approximate p99 bucket
upper bounds; they reset on restart and do not measure successful-write latency.

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
command after a failed connection attempt or the explicit `ERR NOT_LEADER`
reply (including an invocation rejected before proposal execution). For reads,
it also retries the exact `ERR NotRead(ReadNotReady)` and
`ERR Unavailable(LeadershipChanged)` and `ERR Draining` responses, obtaining a fresh quorum barrier
on the next attempt. Automatic reads also retry authentication/request/reply
deadlines and connections closed before a complete request or any reply bytes.
Partial/invalid replies, certificate failures and other errors remain terminal.
Explicit node selection makes one attempt. Uncertain writes are never
automatically resent to another node. A lost write
reply prints Unknown; retry manually using the same operation ID and delta.

Automatic mode scans three local command ports by default, or the configured
command endpoint list, with one active socket, at
most 100 rounds, a ten-second absolute observation deadline and bounded reply
storage. Each automatic read attempt has at most two seconds for connection,
authentication and reply, capped by the remaining overall deadline. Partial
reply progress cannot reset either deadline. Each successful read
still obtains a fresh quorum barrier. Automatic mode accepts only add/read;
status, checkpoint and quit require an explicit node ID. Explicit IDs remain
available for writes/reads too. Remote addresses require the explicit authenticated
configuration above; automatic endpoint or manifest discovery remains separate.

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

The default command endpoint is a trusted local-user interface. Authenticated
local or remote mode enforces the selected principal policy. It accepts one active connection, one command of at
most 256 bytes per connection, a five-second observation deadline and bounded
responses. Parsing, partial I/O and response backpressure do not block Raft polling.
A timed-out pending request cancels its wait; exact tickets prevent a late output
from becoming another connection's reply. All consensus/provider queues retain
their own existing bounded credits and control reserves.

Peer addresses and TLS names are configurable. Rust startup is generic over the
application, while the CLI still selects a fixed three-voter counter bootstrap.
Executable manifest discovery, a generic multi-group configuration loader, richer
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
wake/clock. Select membership wire format 2–7; witness controls require at least 3,
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
online distributed creation/commit of those configurations. The counter CLI now
offers explicit `recover-member` startup and offline enrollment. Enforced counter
envelopes and trusted startup administration are implemented below. Generic application-envelope
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
is an offline trusted handoff for embedding. The CLI exposes the same handoff
for its provisioned identities:

```sh
target/debug/voteboat-counter enroll create /your/data/node3 3 43000 /your/tls /your/data/node1 1 peers.txt
```

Stop both source and destination before use; provider locks refuse concurrent
owners. The source is explicitly trusted deployment input. The command verifies
its exact store identity, original bootstrap, authoritative WAL boundary, pinned
checkpoint and counter restoration/replay. The checkpoint must contain a stable
voter assignment for the source and an exact learner assignment for the target.
Its membership must match the source's current committed membership; after a
membership change, checkpoint that committed view first. Application commands
after the checkpoint may be caught up by normal member replication. This is
local durable evidence from a trusted source, not a remotely signed certificate
or a fresh cluster read barrier. Never duplicate a live store or reuse a retired
identity without the deployment's explicit identity/lifetime authorization.

The command opens no sockets and starts no workers. BASE_PORT, TLS_DIRECTORY and
optional PEERS_FILE describe the same checked deployment used by subsequent
`serve recover-member`; no transport flag is needed for offline enrollment.
For a maintenance-schema source, pass `--leadership-maintenance enabled` to
both enrollment and serving; see [maintenance enrollment](MAINTENANCE.md#authenticated-counter-executable).
To resolve a lost completed reply, replace `create` with `recover` and use the
same source image. A changed image is refused instead of overwriting an import.
Missing files are not created in Recover mode. Source recovery may advance its
provider sessions even if destination enrollment later fails. Preserve files on
any error and follow the partial-initialization limitations above.

Executable tests enroll an absent learner directory from a prepared committed
source, repeat exact imports without changing its WAL state, verify the imported
counter and retry outcome directly, then start/restart the TCP/QUIC service and
commit further writes. They also reject voter/joint/stale-membership imports,
missing pinned checkpoints, source/destination aliasing and changed images.
The source membership is seeded; online configuration commitment and authorized
service mutation endpoints remain work. Explicit declarations above support
additional node/store identities within the service's original bootstrap.

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

## Trusted startup administration plan

For shared multi-group nodes, use the explicitly group-bound
[administration manifest](MULTI_GROUP_STARTUP.md#group-membership-administration)
and `group ID INC configure OP` commands. The single-group plan below retains
its existing startup behavior.

Generate learner/replacement/voter plans with the [placement CLI](PLACEMENT.md),
or provide the explicit records below.

`serve recover-member ... --admin-plan FILE` loads operator-owned intent once,
**before opening the WAL, workers or sockets**. Supply the same immutable plan
and deployment to participating processes. Only the current leader drives it;
ordinary polling without a plan still denies configuration proposals. The local
command port gains no configuration mutation command. This adapter is for the
counter assembly and fixed group/incarnation 1/1, with its enforced schema-1,
eight-byte command and 330032-byte checkpoint envelope.

For example, when committed configuration 3 has voters 1/2 and an already enrolled
learner 3 with the legacy exact store identities, this plan promotes node 3:

```text
voteboat-counter-admin-v1
placement 2 false
replica 1 1
replica 2 2
replica 3 3
joint 800 3 4 5 - m:3 v:1 v:2 v:3
final 800 4 5
```

```sh
target/debug/voteboat-counter serve recover-member /your/data/node1 1 43000 /your/tls --admin-plan promotion.plan --transport tcp
```

The example requires that existing view and enrollment; it does not initialize
it. Create a learner assignment through a separate trusted plan, checkpoint its
source, then use offline `enroll` and start the target in member mode before
promotion. Replica declarations must cover current voters/learners as well as
proposed assignments. Store identities come from the checked `--deployment` file
or legacy provisioning; labels do not create stores or grant membership.

The UTF-8 file is at most 64 KiB, with no blank/comment lines or trailing fields.
Its grammar is:

```text
voteboat-counter-admin-v1
placement MINIMUM_VOTING_DOMAINS true|false
replica NODE FAILURE_DOMAIN [EXACT_STORE_ID EXACT_STORE_INCARNATION]
... replica declarations before all intents ...
learners OPERATION EXPECTED NEXT LEARNERS POLICY
joint OPERATION EXPECTED JOINT FINAL LEARNERS POLICY
final OPERATION EXPECTED FINAL
```

`LEARNERS` is `-` or a comma-separated list of distinct provisioned node IDs.
Voters come from `POLICY`, a prefix tree using `v:NODE`, `m:CHILD_COUNT` followed
by that many trees, or `w:CHILD_COUNT` followed by `WEIGHT TREE` pairs. For example,
`w:2 2 m:2 v:1 v:2 2 v:3` needs both its nested 1/2 majority and voter 3.
The existing validated strict-majority policy owns quorum semantics; zero weights,
duplicate voters, excessive tree depth/count and malformed shapes reject. Limits
include 64 intents, at most 64 failure domains and the native plan's retained-byte
ceiling. `placement`'s boolean requests tolerance of any one voting-domain loss;
these labels are trusted operator assertions. Both current and proposed views
must pass placement checks.

Each intent fixes its original nonzero operation ID, expected head and complete
target. A joint's matching final must also appear in the plan with the same
operation and exact reserved target. The owner enforces journal grammar and
selected provider/wire capacities at execution. Malformed files fail before
opening resources. Invalid placement, mismatched heads or permanent proposal
rejection stop automatic administration and log the reason while normal service
continues; correct the trusted input and restart after inspecting durable status.
Transient leadership/readiness changes retry the original intent with backoff.

The leader waits for an actual current-term committed record. Normal application
work can establish it; the adapter does not manufacture a counter command. After
leadership changes, continuing real application work may be needed before the
next administrative phase. Promotion proofs are collected from authenticated
peers and checked against the current term/configuration/commit prefix/session.
One readiness request runs at a time; a two-second observation timeout queues
owner-side cancellation, then a fresh binding-scoped round. Cancellation changes
only volatile readiness, not membership or previously accepted configurations.

The adapter retains one configuration ticket, consumes its outcome and consults
local durable status before further work. Accepted uncommitted records wait;
committed joint records yield their exact final; completed historical operations
are skipped. Unknown outcomes never create a replacement ID or rollback. Local
absence is inconclusive; only the original explicit operator intent authorizes a
fresh submission, still subject to normal leader/journal checks. A completed
historical operation does **not** compare a newly supplied payload, so preserve
the original file and use fresh operation IDs for genuinely new intentions.
`configuration-status OPERATION` exposes the existing local durable evidence.
Shutdown drains configuration observations with the same owner and workers.

Executable tests commit joint/final promotion from a prepared enrolled learner,
reopen all three native stores with the same plan and verify exactly one joint
and final plus preserved counter deduplication. TCP uses an ordinary majority;
QUIC also exercises a nested weighted policy. These are concrete promotion and
restart histories, not full faulted add/enroll/promote/remove release evidence.
macOS execution and separate-host validation remain outstanding.

## Complete executable membership lifecycle evidence

The service tests now exercise an entire lifecycle starting from the original
three-node `create` bootstrap, with **no prepared configuration journal records**.
Every assignment, joint transition and finalization is proposed by trusted startup
plans and committed through real TCP/TLS or QUIC service processes:

| Committed configuration | Result | Original operation IDs |
| --- | --- | --- |
| 1 | Original voters 1/2/3; write operation 700 adds 42. | Bootstrap |
| 3, then 4 | Demote voter 3 to learner, then remove learner 3. | 1000 joint/final, 1001 learners |
| 5 | Declare exact learner 4, store 404/incarnation 7, on voters 1/2. | 1002 learners |
| 7 | Enroll 4 from the source's real pinned checkpoint; catch up and promote to voters 1/2/4. | 1003 joint/final |
| 9, then 10 | With original voter 1 killed and kept absent, demote it to learner then remove it; voters 2/4 remain. | 1004 joint/final, 1005 learners |

The test checkpoints the real source after learner assignment, stops source and
destination for offline enrollment, retries enrollment with the identical image,
and checks that retry does not rewrite the destination state. While the enrolled
node remains offline, existing voters accept an application write but do not
accept the promotion record. The learner must then receive that post-import
command and establish live authenticated readiness before promotion.

After promotion, abrupt loss of original voter 1 must still permit a committed
application write through the surviving quorum before retirement proceeds.
Survivors checkpoint and restart without either retired original route, retaining
all six operation identities, exact voters/learners, Counter value and retry state.
A fresh nonzero write then changes 42 to 43. TCP uses ordinary majority; QUIC's
promotion uses a majority wrapper over weights 2/1/2 for voters 1/2/4. This covers
that concrete policy and failed-voter schedule, not arbitrary recursive layouts.

These histories require maintenance stops for the trusted offline handoff and for
selecting the next immutable startup plan. They do not establish uninterrupted
online enrollment, arbitrary partial joint/final delivery, dropped readiness
reply recovery, divergent retained-only learner repair or unavailable witness
liveness. The broader fault-release gates and public mutation-ingress gate remain.

Authenticated command credentials can be reloaded through `reload-access REQUEST
EXPECTED NEXT`; `credential-status REQUEST` resolves an unread reply. See the
[reload contract](CREDENTIAL_REFRESH.md#executable-command-channel-reload) for
staging, generation, restart and per-node scope.

## Authenticated submission of provisioned configuration intents

Slice115 adds an opt-in command path alongside automatic startup plans:

```sh
target/debug/voteboat-counter serve recover-member /your/data/node1 1 43000 /your/tls --remote-admin-plan /your/administration.plan --service-access /your/access.txt
target/debug/voteboat-counter client 43000 1 configure 15001 --service-tls /your/tls --principal 3
```

Use the existing administration-plan grammar and provision the same original
records/placement/application bounds on eligible servers. --remote-admin-plan is
exclusive with --admin-plan and requires --service-access and recover-member.
It validates these prerequisites before opening stores/listeners. The plan stays
dormant at startup/restart. Configure permission is administrator-only under the
native roles; reader/writer or wrong group/incarnation grants cannot submit it.
The command names an operation already in the plan. Missing operations refuse;
clients cannot send arbitrary target policies/assignments through this command.

Address a node explicitly; automatic CLI routing still accepts only read/add.
A nonleader refuses before submission. For a joint change, the first configure
call observes joint commitment; repeat the same operation ID to request its
provisioned final phase. Fresh learner readiness remains mandatory for promotion.
The service rechecks Configure on the same pending authenticated channel at
execution, followed by the original exact plan, application capacity, placement,
wire/roster and core admission. No operation starts merely because it appears in
the remote plan.

An OK with committed_index/term observes commitment of that phase, not a later
final phase. Historical completed replies explicitly report local durable
operation identity evidence; compaction may no longer retain original payloads.
Keep original operation IDs and plan contents when retrying. The command port
retains one bounded request/result through ciphertext flush. Deadline, observed
channel failure or disconnect cancels observation, not committed work; the CLI
returns UNKNOWN on an interrupted reply and does not automatically reroute.
Queued work cannot reauthorize using a replacement command connection. Use
configuration-status for historical local evidence, which is not a fresh quorum
read and does not prove non-execution from absence.

Real TCP/QUIC tests cover dormant startup, reader/writer denial, missing operation,
learner and joint/final commitment, original-ID completion retries and application
deduplication after checkpoint/restart. A separate fake-peer CLI test covers a
lost reply; arbitrary socket-loss timing across every membership phase remains
unverified. The next section describes the later client-supplied target mode.
Earlier references to mutation ingress being absent describe historical slices;
broader fault/release acceptance remains open.

## Client-supplied configuration targets

Slice116 adds --remote-admin-policy FILE, exclusive with both plan modes. It
requires --service-access and recover-member. The policy file uses the same
header/placement/replica grammar, but must contain no operation intent lines:

```text
voteboat-counter-admin-v1
placement 2 false
replica 1 1
replica 2 2
replica 3 3
```

Replica stores/incarnations come from trusted deployment provisioning. Clients
choose a full target within that authorized deployment; they cannot replace store
identities, credentials, placement constraints or the enforced application envelope.
For example, address the leader explicitly with administrator credentials:

```sh
target/debug/voteboat-counter client 43000 1 configure-record learners 16001 1 2 - m:3 v:1 v:2 v:3 --service-tls /your/tls --principal 3
target/debug/voteboat-counter client 43000 1 configure-record joint 16003 2 3 4 - w:3 1 v:1 1 v:2 1 v:3 --service-tls /your/tls --principal 3
target/debug/voteboat-counter client 43000 1 configure-record final 16003 3 4 --service-tls /your/tls --principal 3
```

Record grammar is the same as administration plan intent lines. Learners takes
operation, expected configuration, next configuration, comma-separated learners
(or -), and a full quorum tree. Joint additionally names the eventual final
configuration after its joint ID. Final names operation, expected joint ID and
final ID. The existing 256-byte entire-command ceiling applies, as do policy
node/depth, membership, retained-record and provider-capacity bounds. A group
whose target cannot fit this demo command format needs the Rust ConfigurationRequest
API; the service does not silently truncate a target.

The shared parser rejects malformed/duplicate/truncated trees, unknown provisioned
stores, duplicate learners and trailing input. It grows child vectors only after
successfully parsing children, rather than reserving from an untrusted branch
count. Constructors validate the complete policy/configuration. One immutable
parsed target is retained while fresh readiness is gathered; execution checks the
same pending authenticated channel, exact target/requirements, placement and all
ordinary Node gates. Startup and restart remain dormant.

While the original record remains in the durable log, an exact committed retry
returns duplicate=true with its original index/term. An exact uncommitted record
returns UNKNOWN. Conflicting operation reuse refuses. A final record can continue
an existing committed joint only when it matches the existing resume record.
After compaction removes a completed record's payload, configure-record refuses
with comparison history unavailable; configuration-status can still report
historical completed identity. That is deliberate bounded retention, not proof
that a new target matches the old one. Preserve the entire original record when
retrying an uncertain result. A later authorized operation uses a fresh ID and
current configuration, rather than reusing a completed ID.

Native TCP/QUIC tests cover those refusals, fresh learner records, a weighted
same-electorate joint/final transition, checkpoint/restart, compacted-history
refusal and another fresh client-supplied operation after restart. The original
automatic and provisioned-intent modes retain separate regression coverage.
Arbitrary disconnect/revocation timing, remote new-voter fault schedules, broader
platform/fault evidence and complete administration release remain open.

Selected native interruption evidence (slice117) now includes clean TLS close
while the TCP socket remains open, a silent-channel deadline during offline
learner preparation, explicit resubmission after learner restart, and leader loss
after a committed record but before the client reads its reply. Exact retry and
conflicting reuse are checked after successor election; reopened WALs retain the
committed operations and omit canceled preparation operations. Clean terminal TLS
state promptly releases remote observation rather than occupying the slot until
the deadline. Cancellation never undoes a persisted configuration. Fixed-field
preparation/cancellation diagnostics expose progress but confer no authority.
These selected TCP/QUIC peer histories do not cover arbitrary revocation timing
or historical promotion repair after compaction.

Slice118 changes explicit member/import peer mode to exact wire7; restart all
participating peers together when upgrading from6. Static create/recover and the
separate authenticated client command protocol retain their existing selection.
Wire7 adds committed stable checkpoint learner recovery under the documented
old-view-voter trust checks; disk formats are unchanged. Existing interruption
histories were rerun with TCP and QUIC wire7 peers. Forced network execution of
the new final-checkpoint path remains a separate acceptance history.

## Optional automatic WAL reclamation

Append `--wal-reclaim-ms 60000` to a serve/recover command to request periodic
physical reclamation every minute. Omit it to retain manual operation. A positive
interval is required; each node schedules its own selected worker. Inspect the
local, volatile result with:

```sh
target/debug/voteboat-counter client 43000 1 maintenance
```

The latest completion reports the worker sequence and before/after byte counts;
it is not a quorum receipt. One job runs at a time, overload retries after the
interval, and shutdown drains admitted work. The native worker bounds each
replacement image by its configured WAL ceiling. This rewrites the live image
and can delay foreground work on that worker; it is not incremental cleaning.
Reclamation cannot remove the live log suffix or advance the durable application
boundary by itself. Use manual `client ... checkpoint` or the automatic policy
below to authorize checkpoint progression.

Rust hosts use `Node::configure_wal_maintenance(Some(WalMaintenancePolicy {
interval_ms, retry_ms, max_bytes }))` and inspect `Node::wal_maintenance()`.
`PersistenceWorker::reclaim_limit()` advertises support; default host providers
decline it. Poll with monotonic host time as usual; no extra thread or clock is
created. Manual `reclaim` results stay in `poll_reclaim`, while scheduled results
replace only the latest maintenance diagnostic. Disabling/changing the policy
requires its admitted job to finish. On restart, configure a new schedule after
normal durable recovery.

## Automatic checkpoints

Append `--checkpoint-entries 1024` to a serve/recover command to checkpoint when
the applied log is at least 1024 entries beyond its durable snapshot base. The
count includes noops and applied configuration entries. The service checks every
100ms, examines at most64 groups and allows at most4 automatic requests. It uses
the same application, snapshot worker, retention and WAL path as manual
checkpoints. Combine with `--wal-reclaim-ms 60000` for physical reclamation.
Both policies are opt-in and should be supplied again on recovery.

`client ... maintenance` reports `checkpoint_enabled`, `checkpoint_pending` and
`checkpoint_base` for the local replica. The base is durable local state, not a
quorum acknowledgement. Admission is not completion. Pending/busy groups are
skipped and missed scan intervals coalesce. Shutdown stops new scans and drains
already accepted requests.

Rust hosts can choose `CheckpointPolicy { min_entries, interval_ms, scan_groups,
max_in_flight }` through `Node::configure_checkpoints`. Inspect tracked admissions
and latest results through `Node::checkpoints()`. Disable or reconfigure after
its pending map empties. These limits bound maintenance admission; they do not
guarantee a write rate, latency, unlimited application growth or a fixed disk
footprint. General retention and incremental cleaning remain separate work.
