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
The executable's existing access-file behavior is unchanged; integrating staged
credential loading/publication into it remains the next deliverable.

`tests/credential_refresh.rs` exercises downstream validity providers, input
ownership, calls suppressed after revocation, invalidation during I/O, stable
identity and new permissions after reauthentication. Native TCP/TLS tests replace
an actual certificate/key and reject the stale pin. A native transport returns
an accepted batch with its exact ticket and Failed outcome after revocation.
`tests/quic_connect/credential_refresh.rs` checks native QUIC reauthentication and
revocation. These are selected Linux histories, not complete operational rotation
or macOS/separate-host evidence.
