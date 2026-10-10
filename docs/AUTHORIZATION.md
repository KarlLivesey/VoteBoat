# Authenticated service principals

C21 separates service permission from Raft voting, committed configuration and
ownership. `authorization::{PrincipalCredentials, ServiceAuthorizer}` are public
host-provider seams. `authorize_session` requires a Ready authenticated
`SecureSession`, verifies the returned exact session binding and captured
credential generation, checks validity, and checks the channel again after policy
returns. A simulator channel cannot pass. Providers are trusted application code;
these interfaces do not sandbox callbacks or authenticate caller-supplied structs.

`BoundPrincipal` is a host-local snapshot, never a network bearer token. The host
captures `CredentialContext` when its channel authenticates. Rebinding preserves
that timestamp rather than renewing expiry. An authorizer receives a concrete
`GroupIdentity` and `ServiceAction`; neither permission nor successful TLS grants
a vote, leadership, membership activation or ownership.

The native `NativeServiceAccess` is an immutable shared plan with an explicit
nonzero generation, lifetime of 1..3,600,000 ms and at most 4,096 grants/capacity.
One authenticated stable peer maps to one principal and vice versa; duplicate
principal/group grants are rejected. Construction returns the original entries
on rejection. Clones share the plan; closing one view leaves the others usable.
New plans use a new generation. There is no automatic live refresh or durable
policy journal. Native expiry/clock/generation/scope checks also apply when using
its authorizer directly, but a host must use the checked session gate before
executing untrusted requests.

## Executable mode

The counter's command endpoint remains loopback TCP, including when Raft peers
use QUIC. Without `--service-access`, it retains its trusted plaintext demo mode.
Append `--service-access /path/access.txt` to **every** serve/recover invocation to
require mutually authenticated native rustls sessions for commands. The file is
trusted startup configuration, limited to 4 KiB and 64 grants:

```text
voteboat-service-access-v1 1
1 reader 1 1
2 writer 1 1
3 admin 1 1
```

Header value is the credential generation. Each grant is
`PRINCIPAL ROLE GROUP INCARNATION`. Principal numbers are 1..4096. Groups and
incarnations are nonzero. The demo serves group 1/incarnation 1. Invalid grants
fail before listener/store creation. Increment the generation when replacing
credentials or permissions. Apply at restart or use the explicit authenticated
[live command-channel reload](CREDENTIAL_REFRESH.md#executable-command-channel-reload).

| Role | Commands |
| --- | --- |
| reader | status, metrics, configuration-status, credential-status, read |
| writer | reader commands plus add |
| admin | writer commands plus checkpoint, reload-access and quit |

Startup `--admin-plan` remains an operator input with separate committed
configuration checks. Slice115 adds --remote-admin-plan with administrator-only
configure OPERATION_ID for provisioned intents, rechecked on the live channel at
execution. General client-supplied target parsing remains absent; see
[the service contract](COUNTER_SERVICE.md#authenticated-submission-of-provisioned-configuration-intents).

Clients select their certificate and the same principal number:

```sh
target/debug/voteboat-counter client 43000 auto read --service-tls tests/fixtures/tls --principal 1
target/debug/voteboat-counter client 43000 auto add 11001 7 --service-tls tests/fixtures/tls --principal 2
target/debug/voteboat-counter client 43000 1 checkpoint --service-tls tests/fixtures/tls --principal 3
```

The TLS directory supplies `ca.der`, `nodeP.der`, `nodeP-key.der` for the client
and `nodeTARGET.der` server pins. Servers load each granted principal's
`nodeP.der` pin along with their existing local TLS credentials. Certificates use
`nodeN.voteboat.test` names and client/server usages, with existing 64 KiB material
limits and a 1 MiB aggregate principal-pin budget. Fixture private keys are public
and provide only a reproducible local demo.

The bounded plaintext principal selector only chooses an expected certificate
pin. The subsequent TLS authentication and identity hello must match it. Service
client/server transport identities occupy separate namespaces from Raft peers.
A caller cannot obtain permissions merely by changing the selector. Selected
pins remain distinct even when certificates share a trusted CA.

Each accepted command connection has the existing five-second absolute deadline,
256-byte command limit, one command, and bounded TLS polling. Replies remain
owned until ciphertext flush; slow handshakes do not stop Raft polling. Native
credential validity is 30 seconds from authentication, without per-request
renewal. Authorization runs before read/proposal/control admission. Denial returns
`ERR AUTHORIZATION` and admits no operation. Expiry or policy replacement cannot
undo an already admitted command. Lost replies/leadership changes still require
preserving the original operation ID and payload; auto routing stops on uncertain
outcomes and authentication errors.

## Evidence and remaining scope

`tests/authorization.rs` covers downstream providers without native code, invalid
channels/bindings/generations/scopes/expiry, changed sessions during policy, native
shared views and rejected plan ownership. Real TCP/QUIC peer process histories in
`tests/counter_service.rs` exercise reader/writer/admin refusal, wrong certificate
pin despite a valid CA, rejected plain traffic, checkpoint, restart with a revoked
writer, durable deduplication and graceful worker cleanup. These are finite Linux
histories, not arbitrary-fault, macOS or separate-host deployment evidence.

Live credential rotation/revocation, external issuer integration and durable
principal audit trails remain outstanding. This slice authenticates the existing
counter command boundary; it does not complete general service authorization or
public configuration mutation ingress throughout P0–P7.

Slice116 also consumes Configure for `configure-record` with `--remote-admin-policy`.
The same group/session gate is rechecked at execution against one immutable full
target and operator-provisioned placement. See [client target semantics](COUNTER_SERVICE.md#client-supplied-configuration-targets)
for bounded parsing, exact retry comparison and compacted-history refusal.

Slices173–174 add [credential generations, session guards and explicit live reload](CREDENTIAL_REFRESH.md) for Rust hosts and the executable. Prepared command-policy/TLS replacement revokes existing guarded sessions without changing voter membership. The executable records local preparation before publication; automatic peer rotation, external secret distribution and general durable audit remain open.
