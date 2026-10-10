# Credential generations and session revocation

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
