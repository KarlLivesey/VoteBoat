# Metadata authority service

`voteboat-directory` hosts a real replicated Directory on Linux/macOS. It uses
native TCP/TLS or optional QUIC between three voters, plus authenticated TCP
commands and the public remote manifest protocol. It shares native startup,
worker cleanup and command transport with the counter executable.

## Start an authority

Build and generate a trusted initial plan for authority42, responsibility10 and
an **already established** execution group100 (all incarnations1):

```sh
cargo build --locked --bin voteboat-directory
target/debug/voteboat-directory plan 42 1 10 1 100 1 > directory.plan
```

This declares an initial assignment; it does not create or activate group100.
The generator selects adapter1/version1, partition scheme1/version1, scope0..256,
ownership epoch1, route generation1, and three independent voting domains with
single-domain-loss tolerance. Review these declarations for the actual owner.
The bootstrap operation is100; publication is101.

Provision TLS and access files using the same certificate format as the
[counter service](COUNTER_SERVICE.md). Example access file:

```text
voteboat-service-access-v1 1
1 reader 42 1
2 writer 42 1
3 admin 42 1
```

Start all three nodes with identical plans, separate empty directories and the
same base port. Each needs its own node certificate/key:

```sh
target/debug/voteboat-directory serve create /your/data/meta1 1 43000 /your/tls directory.plan access.txt
target/debug/voteboat-directory serve create /your/data/meta2 2 43000 /your/tls directory.plan access.txt
target/debug/voteboat-directory serve create /your/data/meta3 3 43000 /your/tls directory.plan access.txt
```

Use separate terminals or your process supervisor. Peer ports default to
43001..43003; command ports to43101..43103. `--peers FILE`, `--deployment FILE`
and `--command-listen ADDRESS` select explicit endpoints using the existing
formats. `--transport quic` requires a build with `--features quic`; command and
manifest sessions still use TCP/TLS. This binary exposes static membership.

`--peer-credentials MANIFEST` enables independent, durable peer key rotation.
The authenticated client supports `reload-peers REQUEST EXPECTED NEXT` and
`peer-credential-status REQUEST`. Command access supports `reload-access`
and `credential-status` with the same arguments. See [credential reload](CREDENTIAL_REFRESH.md)
for manifests, permissions, original request retries and restart checks.

## Publish and inspect

Inspect local election status to find the leader; status is not a quorum read:

```sh
target/debug/voteboat-directory client 43000 1 /your/client-tls 3 status
```

On the leader, initialize the exact plan and publish operation101:

```sh
target/debug/voteboat-directory client 43000 1 /your/client-tls 3 initialize
target/debug/voteboat-directory client 43000 1 /your/client-tls 3 publish 101
```

Only administrators can initialize/publish. The server proposes the original
plan bytes under their original operation IDs; replies contain actual Directory
outcomes and duplicate status. A disconnected write has an unknown outcome:
retry the same plan operation on the current leader. Non-leaders refuse proposals.
Writes select an explicit node; retry their original operation on the current leader.

Fetch a manifest through a fresh quorum-backed read, selecting an eligible
authority endpoint automatically:

```sh
target/debug/voteboat-directory lookup 43000 auto /your/client-tls 1 42 1 10 1
```

The last four numbers are authority group/incarnation and responsibility
ID/incarnation. Readers, writers and administrators with the matching authority
scope may query. An unpublished responsibility returns Missing. Loss of quorum
cannot be replaced by local state or liveness. `lookup` prints the selected
manifest's identity, generation, epoch and execution mode; it is one lookup,
not permission to serve data. For recursive traversal, use `route` below.

`auto` uses the configured command endpoints (the default three nodes or
`--command-peers FILE`) and the existing bounded route discovery client. Every
attempt keeps the same authority and query within one ten-second deadline;
status only selects an attempt, and success requires a validated fresh manifest.
Replace `auto` with a numeric node to pin that source. A pinned follower refuses
the read even if another endpoint can serve it.

Clients can append `--command-peers FILE` for explicit non-default command
addresses and independently pinned TLS names. The complete connection, command
upgrade and lookup share a ten-second deadline. Server connections also have a
ten-second deadline and one outstanding request. The owner drains accepted
reads and their underlying Node credits after disconnect before accepting a
new command. The `manifest read accepted` diagnostic identifies that boundary.

## Resolve a hierarchy

Provide command endpoints and pinned TLS names for each metadata authority:

```text
voteboat-authorities-v1
42 1 1 127.0.0.1:43101 node1.voteboat.test
42 1 2 127.0.0.1:43102 node2.voteboat.test
42 1 3 127.0.0.1:43103 node3.voteboat.test
43 1 1 127.0.0.1:44101 node1.voteboat.test
43 1 2 127.0.0.1:44102 node2.voteboat.test
43 1 3 127.0.0.1:44103 node3.voteboat.test
```

Each row is authority group, incarnation, node, command address and TLS name.
Provision matching server certificate pins in the client TLS directory. A node
identity reused in multiple authorities must use the same certificate and TLS
name. Addresses may differ. The map is bounded to32 authorities,64 endpoints
and32KiB. It supplies connection choices, never committed ownership.

For byte-partition scheme1/version1, resolve key10 starting at responsibility10
in authority42:

```sh
target/debug/voteboat-directory route /your/client-tls 1 42 1 10 1 10 authorities.txt
```

The client discovers missing path segments with authenticated quorum reads and
uses the public checked resolver. It probes candidate leaders, follows only the
selected child path and verifies each child's authority, parent, epoch, scope
and partition scheme. Unrelated branches need no connection. Output identifies
the final execution group and includes `hint_only=true`: servers must still
check committed ownership before admitting or applying work.

Use `--max-hops N` (1..32, default32), `--min-epoch N` or
`--min-generation N` for root observation constraints. A known child locator
can be the starting point without contacting its parent. All authorities on the
selected path must be provisioned and the principal must have read permission.
The client has one outstanding remote read,64-manifest/256KiB caches, at most128
connection probes and one ten-second invocation deadline. Each read attempt is
bounded to two seconds. Explicit unavailability rotates candidates; identity,
protocol, missing-manifest and lineage errors fail without returning a partial
route. Cache contents are per invocation; there is no persisted fallback.
Each connection probe has at most1.5 seconds within that same invocation budget.
An expired authentication attempt drops its socket and permits another bounded
probe; certificate or identity failures remain terminal. Exhausting the total
deadline returns a recursive-lookup error with no partial route.

## Recovery and scope

Use `checkpoint`, then wait for a nonzero `checkpoint_index` in status before
stopping if you need to exercise checkpoint recovery. `quit` drains and joins
workers. Restart with `recover`, the original plan, matching identities and the
same application profile. Changed plans fail recovery instead of silently
replacing existing ownership. Original operation retries survive WAL/checkpoint
recovery; a successful retry may report `duplicate=true`.

Plan files are bounded to128KiB and64 initial manifests. Their header is
`voteboat-directory-plan-v1 GROUP INCARNATION BOOTSTRAP_OPERATION`; remaining
lines contain `OPERATION CANONICAL_DIRECTORY_COMMAND_HEX`, produced by the public
`DirectoryCommand::encode`. Initial commands have no expected generation, and
the plan rejects duplicate operation/responsibility identities and invalid grants.
The allowed records are fixed at startup. Actual command/history/checkpoint
bounds are checked before opening storage. It does not expose lifecycle commands
or advertise Directory's larger lifecycle/membership readiness envelope.

Credential policy is loaded at startup; live reload is not provided by this
binary. Placement orchestration and lifecycle administration remain subsequent
work. The native acceptance tests are local
multiprocess TCP/QUIC histories; separate-host and current macOS validation remain
open. The full P0–P7 roadmap is not complete.

## Transfer preflight

`voteboat-directory split-preview PROFILE` checks an explicit offline split/merge
plan and prints its scopes, placement and payload bounds without submitting it.
See [the profile and Rust contract](TRANSFER_PREVIEW.md). It does not recover
live data or begin a transfer; operator lifecycle execution remains separate.
