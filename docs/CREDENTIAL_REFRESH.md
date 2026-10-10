# Credential generations and session revocation

## Peer connector rotation in Rust

`connect::PeerCredentialControl` is an optional public provider contract, also
forwarded by `PeerDriver::replace_peer_credentials` and
`Node::replace_peer_credentials`. Hosts can implement it without native TLS.
For the native providers, explicitly wrap a fresh, drained TCP/TLS, QUIC or
`NativeServiceConnector` in `native::peer_credentials::RotatingPeerConnector`.
It implements the existing `PeerConnector` and can be supplied in `NodeParts`.
Only sessions established through the wrapper carry its revocation leases.

Supply a prepared `NativePeerMaterial` containing a validated `NativeTlsConfig`
and the complete peer-pin map. Call `replace_peer_credentials(expected, next,
material)` after authorizing and durably recording the intended rollout. The
wrapper requires its current generation to equal `expected`, and `next` to be
greater. Native replacement preserves the exact node/store/incarnation set,
wire version, addresses and limits. Invalid input is returned intact; success
returns the prior material. Membership changes use the membership protocol.

Successful publication invalidates old established sessions and cancels old
connection attempts. Accepted tickets keep their slots until their terminal
receipts are polled. Even a late successful handshake is revoked if it belongs
to the previous generation. New connections retain increasing connection
generations and authenticate against the replacement keys and pins. Closing or
dropping a wrapper revokes its leases; it cannot revoke an unrelated owner's
sessions. Use `close`, drain accepted receipts, then `into_inner` to reclaim the
underlying provider and its native worker.

`NativeServiceConnector::with_peer_rotation` enables the same guards while
preserving the native service connector type. Static connectors reject rotation;
guarded connectors reject direct raw material replacement that would bypass
revocation. `PeerDriver::credential_generation` reports the active generation.

For native static, member or multi-group startup, use
`NativePeerRotationStartup::from_journal(protocol, generation, &journal)` and
`open_with_peer_rotation`. The host first loads the selected TLS/pin bundle and
the existing `NativeCredentialJournal` under exclusive local-store ownership.
Startup checks the latest record's owner, request shape, exact replacement
generation and material digest before binding sockets or opening storage.
`NativePeerMaterial::digest()` binds the exact CA/certificate/key bytes, wire
version, and every ordered peer identity, certificate and server name. Endpoint
addresses are routing input and do not change during credential replacement.

Prepare and validate replacement material off the poll thread, then durably
publish a `CredentialReloadRecord` containing its digest before calling
`Node::replace_peer_credentials`. Retain the original request sequence and both
generations. If the process stops after recording but before in-memory
publication, restart with those recorded credentials; startup installs their
generation and normal WAL/checkpoint replay recovers application state.
An uncertain journal result requires stopping and reopening the journal before
resuming; a recorded result does not prove every node has installed its keys.

A missing journal record selects a host-authorized initial generation. It does
not prove that no prior rotation occurred: the host must preserve/load the
journal and must not silently substitute an empty journal after rotation.
These APIs do not perform file loading or journal I/O inside Node polling.
The counter executable exposes peer preparation/status as described below;
transfer/directory peer administration remains follow-on work.
`reload-access` still changes command-channel credentials only. During a rollout,
incompatible key/pin selections can interrupt connectivity; application work
already admitted retains its normal original-operation recovery semantics.

`secure::SessionValidity` is a public, bounded, nonblocking check for one fixed
credential generation. `GuardedSession<S,V>` wraps the existing `SecureSession`
contract, so native transports and host replacements use the same interface.
It requires an authenticated, Ready session and returns both inputs on rejection.
Capture the validity lease before beginning authentication; never attach a fresh
lease to a connection authenticated with old credentials.

`native::credentials::NativeCredentialSet<P>` holds one prepared bundle, such as
TLS configuration, peer pins and service policy. The host loads and validates the
whole bundle, including generation consistency, before publication. The owner
issues `CredentialLease` values for its current generation. The lease is immutable
and implements `SessionValidity` through one shared atomic generation.

`replace(new_generation, bundle)` accepts only a strictly newer generation,
returns rejected input intact, and returns the old bundle on success. Successful
publication invalidates every old lease; it does not mutate or renew them.
Re-establish connections using the new material, then wrap them with the lease
captured for that material. The set stores no session list or pending queue and
runs no file I/O, callback, destructor of the old bundle, or network work during
publication. Active session limits remain the transport owner's responsibility.

A guard checks validity and the fixed session identity around each I/O operation.
Once revoked it reports Failed, hides its binding and rejects plaintext reads,
writes and polling. A successful underlying call cannot escape as success if
its credential was revoked during that call. A read buffer is cleared in that
case. Bytes accepted or transmitted before revocation cannot be recalled, and
application work already admitted must retain normal operation-ID recovery.
Revocation never rolls back committed work or changes membership.

The next mutable call revokes the underlying session. Closing one guard affects
only that connection. `into_revoked_parts` always revokes before returning the
underlying session and validity provider; it cannot extract a live channel with
its checks removed. Closing or dropping a credential-set owner revokes all its
leases, while unrelated owners remain usable. `into_material` closes the owner
before returning its bundle. It never reopens a closed generation.

Credential generations are host configuration, separate from secure-session
connection generations and durable voter/store incarnations. Restart must select
and validate the intended credentials again. This helper provides no durable
rotation journal, secret distribution, external issuer or automatic file watcher.
The executable composes this helper with the local journal described below.

`tests/credential_refresh.rs` exercises downstream validity providers, input
ownership, calls suppressed after revocation, invalidation during I/O, stable
identity and new permissions after reauthentication. Native TCP/TLS tests replace
an actual certificate/key and reject the stale pin. A native transport returns
an accepted batch with its exact ticket and Failed outcome after revocation.
`tests/quic_connect/credential_refresh.rs` checks native QUIC reauthentication and
revocation. These are selected Linux histories, not complete operational rotation
or macOS/separate-host evidence.

## Counter executable peer rotation

Start the counter with both `--service-access ACCESS` and
`--peer-credentials MANIFEST`. This works with static, member and multi-group
profiles over TCP or QUIC. The manifest contains exactly:

```text
voteboat-peer-credentials-v1 1
tls keys-v1
```

The TLS directory is absolute or relative to the manifest's parent; its path
cannot contain whitespace. It contains `ca.der`, the local
`nodeN.der`/`nodeN-key.der`, and `nodeN.der` for every provisioned peer.
Node/store identities, peer names, endpoint addresses and wire profile come
from trusted startup configuration. Command-channel credentials still use the
ordinary TLS/access paths. Use distinct immutable key directories for peer
versions; install a complete manifest before submitting a request.

To rotate, change the manifest to the next generation and key directory, then
use the authenticated counter client on each node:

```text
reload-peers REQUEST EXPECTED NEXT
peer-credential-status REQUEST
```

Mutation requires Configure permission on every local group; status requires
Inspect permission on every local group. Neither command requires a group1
that the node does not actually own. Requests cannot select filesystem paths
or alter membership. Each node has its own increasing request sequence and
credential generation; apply the rollout explicitly to every node.

`queued=true` confirms admission. One owned worker loads bounded material,
validates its generation and persists the exact digest in
`PEER-CREDENTIAL-RELOAD`; only then does the host replace peer credentials and
revoke old connections. `state=recorded` plus the expected current generation
confirms local publication or reconstruction at startup. After a lost reply,
query/retry the original request. Only the latest durable request is retained;
`unknown` is not proof an older request never ran.

Invalid preparation leaves the current generation usable. An uncertain record
or failed publication fences further reloads and stops the service. Shutdown
joins accepted preparation before releasing the data directory, even when the
prepared keys were not installed in memory. Restart requires the recorded
manifest generation and exact material. Stale, changed or unrecorded newer
material is refused; an existing journal also prevents silently dropping the
startup flag. Preserve the manifest, immutable key directory and journal.
Deliberately deleting a journal is outside this protection.

This is local rollout control, not a cluster-wide transaction. Mixed key/pin
versions can temporarily interrupt peer connectivity; committed data and
original operation IDs retain the normal recovery guarantees.

## Executable command-channel reload

With `serve --service-access FILE`, an authenticated administrator can submit:

```
reload-access REQUEST EXPECTED NEXT
credential-status REQUEST
```

Use the existing `client ... --service-tls DIRECTORY --principal ID` options.
The transfer executable exposes the same commands through its authenticated
`command PROFILE ENDPOINTS TLS PRINCIPAL GROUP ...` interface; see
[transfer access replacement](TRANSFER_SERVICE.md#replacing-command-access).
It shares the same preparation, publication, shutdown and restart implementation.
First stage the intended files at the fixed startup access/TLS paths. The access
file header must name NEXT, which must exceed EXPECTED. REQUEST is a nonzero
local sequence, greater than the last recorded request. Apply the operation to
each service node explicitly; it is not a cluster-wide transaction.

One owned worker reads bounded access, CA, certificate/key and principal-pin
files, validates the complete bundle, then records its exact SHA-256 digest and
request under the stable local node/store identity. The host publishes the new
generation only after that record is durable. Old command sessions are revoked;
fresh sessions use the replacement policy and TLS material. Raft peer sessions
and membership are unchanged. No file I/O runs in the Node poll loop.

`queued=true` acknowledges admission, not completion. Publication may revoke
the requesting channel before its reply arrives. Reconnect and query status:
`generation` is the current generation; `recorded_generation` names the latest
durable preparation. `state=recorded` identifies the latest retained request.
Exact retries of that request return `already_recorded=true`; changed or older
sequences fail. `unknown` is inconclusive for superseded requests because the
journal retains only the latest record. Pending and failed state is volatile.

Validation failure retains the old generation. An uncertain journal result
closes command credentials and stops the service for recovery. Normal shutdown
joins accepted preparation; error exits also join, but may leave a durable
preparation for startup to reconcile. Disconnecting a caller never cancels an
accepted preparation or rolls back admitted application operations.

`CREDENTIAL-RELOAD` is a fixed128-byte, checksummed v1 record; its atomic native
provider writes a staging file, synchronizes it, renames it and synchronizes the
directory. `CredentialJournal` and `CredentialRecordIo` are public replacement
contracts. Hosts must provide exclusive writer ownership until all workers join;
the service retains its existing data-directory lock. A successful publish
means recoverable local preparation, not voter durability or cluster activation.
Checksums detect corruption and do not authenticate a hostile filesystem.

The journal reports invalid record, wrong owner, sequence conflict and generation
floor violations separately. `Io { uncertain: false }` guarantees the prior
record is unchanged; `Io { uncertain: true }` and `Fenced` require reopening before
more writes. Failed construction returns the supplied I/O owner. No retained
history grows with the number of rotations: one128-byte record, one worker and
one prepared bundle are the bounds; each source file also has a checked ceiling.

On restart, a lower file generation or changed material at the recorded
generation fails closed. Restore the exact recorded bundle or deliberately
provide a higher trusted startup generation. Keep staging files and records on
failure for diagnosis. Private-key distribution, automatic peer rotation,
general audit history and cross-node orchestration remain separate work.

The journal tests inject pre/post-replacement uncertainty, corrupt every record
byte, and reopen with partial staging files. Process tests use TCP and QUIC peer
transports with authenticated TLS command channels, lose reload replies, revoke
writer access, preserve retries and reject stale/changed restart configuration.
