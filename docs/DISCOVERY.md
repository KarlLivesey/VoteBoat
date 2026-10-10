# Peer endpoint discovery

C17's initial `discovery::PeerDiscovery` contract supplies endpoint hints for an
exact, already provisioned `PeerIdentity`. `resolve(peer, now)` returns a fixed-size
`PeerEndpointHint`: peer, nonzero `HintGeneration`, numeric `SocketAddr` and
monotonic expiry. `invalidate(peer, generation)` affects only that generation;
`close` closes the selected view. Calls must be bounded and nonblocking. DNS,
network fetch and refresh scheduling belong to an explicitly driven host outside
these calls. There is no hidden discovery worker or global cache.

A hint is not a certificate, membership declaration, leader claim, read barrier,
responsibility owner or permission to recreate a retired group. Actual sessions
still authenticate against the connector's provisioned pins/identities, and the
roster and consensus/application admission paths retain their existing checks.
Resolution never changes quorum weights, voter configuration or ownership epochs.

## Native cache

`native::discovery::NativePeerDiscovery::new(capacity, now)` reserves a finite
logical entry budget of 1..1024 peers. Entries are fixed-size hint/generation
metadata held in a bounded map. `publish(hint, now)` admits atomically or returns
both the typed error and original fixed-size hint. Zero port, unspecified or
multicast address, expired hints and regressing time reject. One cache key includes
node, store ID and store incarnation; a replacement store is a distinct key.

Same-generation identical live hints are idempotent. Same-generation changed
address or expiry conflicts. Lower generations reject. Expiry/invalidation retain
a floor slot; reusing the invalidated generation cannot silently revive it, and
invalidation of an older generation cannot remove a newer publication. There is
no implicit eviction; capacity includes floors. These strict rules apply to the
public `publish` API. A host must size the provider for
its provisioned identities or explicitly construct a new provider. Closing rejects
further publication/resolution. The cache creates no sockets, threads or files.

Restart reconstructs hints explicitly from trusted host input. This volatile
cache does not persist generation floors across restart and does not treat them
as durable protocol authority. Expiry uses host monotonic time, not a serialized
wall-clock timestamp. Hosts must refresh expired hints externally.

## Connector integration and lifetime

`connect::DiscoveryConnector<C,R>` is a public `PeerConnector` over any selected
`SocketAddr` connector and `PeerDiscovery`. It works with native TCP/TLS, selected
native service connectors and host providers through the same public contract.
Select it in `PeerParts.connector` when assembling an embedded `PeerDriver`/Node;
no core edits are required. Static Rust hosts can also use
`NativeStartup::prepare_for_discovery` to obtain recovered native parts before
wrapping their connector; see [prepared startup](NODE.md). The executable peer
startup path retains its configured endpoints.

Construction validates connector limits and quiescence; failure returns both
providers without closing them. Dial submission first checks local identity,
construction-provisioned peer support, deadline and bounded attempt slots. It
then resolves and checks the exact peer, address and expiry before replacing only
the supplied endpoint. A rejection returns the original endpoint/ticket/deadline.
There is no fallback to the supplied stale address. Accept bypasses resolution,
while retaining provisioned-peer authentication. No resolver provisions trust.

Accepted attempts retain their ticket and selected hint generation through the
inner connector's terminal poll. Failed attempts conditionally invalidate that
hint. A newer publication remains intact. Explicit cancellation and close release
work through ordinary terminal outcomes and do not invalidate a healthy hint.
An accepted attempt need not restart merely because its hint later expires;
expiry controls address selection, not credential validity or Raft authority.
Duplicate or mis-scoped completion tickets are provider violations; the wrapper
closes instead of accepting them. A violating provider may require explicit drop
rather than normal drain, just as other malformed asynchronous providers do.

The peer driver treats only Missing, Expired, Unavailable and Overloaded discovery
refusals as transient. Existing roster backoff, poll budgets, connection-generation
ceilings and per-attempt deadlines bound retries. Other discovery errors remain
terminal. There is no nested retry loop or deadline extension. Discovery refusal
admits no socket attempt; a later accepted attempt retains ordinary connector
ownership until its actual terminal receipt. Close delegates to the selected
connector and resolver view; `into_parts` requires close and complete drain,
after which native dialer joining remains explicit. Shared host resources are
still the host's responsibility, and one correctly scoped view must not close
another.

## Executed evidence and remaining scope

`tests/discovery.rs` implements independent downstream providers through the
wrapper, checking address substitution, original rejection, bounded slots,
invalid identity/expiry, conditional invalidation, cancellation, duplicate
completion, failed construction and shared-view close. Native tests exercise
capacity, same/stale/conflicting generations, invalidation/expiry floors and
volatile restart. Core-only tests require no native provider.

`tests/connect.rs` uses real native TCP sockets, rustls sessions and a dial worker:
a held obsolete listener causes a timed-out attempt, discovery invalidates it,
then a refreshed address establishes the expected authenticated identity despite
the request still supplying the obsolete address. Connector close/drain/join
preserves transferred session ownership. `tests/support/peer_driver.rs` checks
transient misses use backoff and wrong scope remains terminal. Full connector,
owner and TCP/QUIC service regressions are recorded in validation/REPORT.md.

This is peer-address discovery, not complete C17. Remote manifest fetching was
added184, command endpoint integration188 and a metadata authority executable189.
Slice209e adds versioned live command-endpoint updates; native startup integration
and broader refresh composition remain open. Slice172 below adds a bounded external endpoint protocol. Real selected discovery connections include
TCP/TLS and QUIC. No macOS/separate-host, arbitrary-fault or performance claim follows.

## Explicit QUIC dial refresh

`NativeQuicConnector::new_with_discovered_dials` permits validated Dial addresses
for already provisioned peers. Wrap it in `DiscoveryConnector` with native or host
`PeerDiscovery`. The ordinary constructor stays static; Accept uses its provisioned
remote address. Refresh requires explicit publication; no DNS worker or executable
hot-reload is inferred. This does not enable live QUIC migration.

The endpoint must have a nonzero port, non-multicast/non-unspecified IP, the bound
socket's address family and differ from its local bound address. Certificate,
peer/store identity, ticket generation and deadline checks remain. Each native
hub retains at most one live lease per provisioned peer as well as per endpoint:
address changes cannot bypass the old peer lease. Refusal returns the original
request and consumes no generation; drop the old session before reconnecting.
Accepted sessions retain their original address and crypto state.

Slice146 real QUIC tests time out an obsolete hint, invalidate its generation,
publish a newer endpoint and establish authenticated bidirectional data despite
stale caller input. Changing the hint while a session is held refuses; release
permits reconnect using the previously refused ticket generation. An independent
host hint provider works through the same wrapper. Closing the connector leaves
transferred sessions usable; final drop releases bound ports. A reachable different
certificate cannot satisfy the pinned peer. Native mailbox checks cover peer/endpoint
collision bounds and generation cleanup.

## Automatic responsibility reads for Rust hosts

`routing::ManifestReadSource` (contract version2) exposes original trusted
Directory reads: immutable binding, pending count, bounded submit, exact terminal
poll and best-effort cancel. Native `Node` implements it for Directory-shaped
readable applications. Host replacements use the same public contract. A read
outcome is not a signed remote credential; the source must preserve the original
Node's quorum-backed authority and receipt ownership.

`native::lookup_discovery::NativeManifestLookup` owns one selected source and a
bounded `NativeAuthorityDiscovery`. Pass it to `resolve_discovered`. A missing,
expired or below-floor observation submits one original read and returns
`Unavailable`. Poll the existing source Node through `source_mut()`, then call
the driver's `poll(now)` and retry resolution. Successful original reads publish
through the existing identity, barrier, replay, epoch and generation checks.
There is no manual `observe` step, hidden worker or second runtime.

One pending read prevents duplicates; another uncached request returns
`Overloaded`. One fixed negative-result slot with a checked retry delay prevents
busy retries. Cache entry/byte limits still bound observations and retained floors.
Construction returns the owned source on refusal and requires no pending reads.
The driver is its source's exclusive read-result consumer while work is pending;
the host may still drive Node polling and other existing operations.

Cancellation, deadline and close suppress publication, including late successful
reads, while retaining the accepted ticket until its actual completion. Close
affects the lookup view, not the underlying Node. Closed and drained `into_source`
returns that original source for ordinary shutdown/join. `into_recovery` returns
the source and unresolved request/ticket explicitly. Alien completions are returned
owned and fence lookup intake; provider replacement requires explicit recovery.
Restart constructs a new driver for the new original read binding; observations
are volatile. No ownership activation or durable protocol state is introduced.

Slice147 tests automatically resolve a real three-replica Directory over TCP/TLS
and QUIC, refresh expired unchanged metadata, reject a higher requested epoch and
suppress a real late positive read after host cancellation. Public host conformance
checks retained deadline/close work, bounded retry, construction refusal and exact
recovery handoff. This adds embedded automatic Directory-read orchestration.
Remote lookup is provided by184 and the metadata authority executable189;
endpoint integration is provided by188. Existing cached child routes retain
their parent-independent behavior. Recursive executable routing remains open.

## Composition with routed applications

The slice148 service histories exercise the complete embedded path using the
existing public contracts. Begin with an empty `NativeManifestCache`, a selected
Directory Node in `NativeManifestLookup`, and bounded `resolve_discovered`
requests. Drive metadata Nodes and lookup polling until resolution returns a
checked `RouteHint`. Use that hint with `encode_routed` for writes and with
`RoutedQuery` for reads; the child application rechecks actual ownership.

On an explicit stale-route signal, invalidate the exact source observation and
cached manifest generation. Subsequent resolution submits a fresh original read;
it cannot renew a replayed receipt. Cache expiry in the lookup provider does not
silently evict otherwise usable child routes. A host can start directly at a
cached child with zero lookup budget while all metadata owners are offline.

For shutdown, close/drain the lookup view, extract its original Node and join
native workers through existing Node ownership. Durable reopen uses a new lookup
driver bound to the fresh original read generation and an empty route cache.
Resolve again, then retry the original child operation ID and bytes before sending
new work. Slice148 checks this over TCP/TLS and QUIC, both WAL-only and checkpoint
recovery: the original7 remains7 on retry, then a new3 reaches10. Metadata logs
remain unchanged throughout offline child writes/reads and checkpoint recovery.
This is Rust service composition; no new remote lookup endpoint or CLI command
is advertised.

## Remote endpoint provider (slice172)

`native::remote_discovery::NativeRemotePeerDiscovery<S: SecureSession>` implements
`PeerDiscovery` using a dedicated, already authenticated session. Construct it
with the expected source `PeerIdentity`, bounded `RemoteDiscoveryConfig` and local
monotonic time. Construction rejects the wrong source or an unready/insecure
session and returns the original session unchanged. No sockets, threads or
runtime are created. TCP/TLS and QUIC use the same implementation.

A cache miss queues one fixed-size request and returns `Unavailable`. Another
request for that peer shares the slot; a different uncached peer receives
`Overloaded`. Call `poll(now, SessionPollBudget)` explicitly; it returns at most
one `RefreshCompletion` with the original request's session binding, sequence,
peer, start and deadline. Cache hits require no source call. One fixed negative
result slot and retry delay bound retries. The existing cache bounds entries,
including invalidated generation floors. A fresh authenticated response can renew
an expired or locally invalidated lease at the same generation only when the exact
peer and endpoint are unchanged. An address change still requires a newer
generation; a lower generation still rejects. Only this checked response path
can revalidate a same-generation lease: ordinary cache publication remains strict.
Local invalidation records a failed address attempt, not credential revocation.
The connector's authentication and membership checks remain independent.

`NativeDiscoveryResponder<S, R: PeerDiscovery>` uses the same authenticated
session contract and accepts an independently supplied native or host provider.
Poll it explicitly. `source_mut()` supports host publication. It retains only
one fixed input/output frame and rejects replayed or malformed requests. Closing
it closes that connection, not its source; `into_parts()` returns both owners.
Extracting parts during a partial reply abandons that non-authoritative reply.

The client can be selected inside `DiscoveryConnector` without core changes.
While retaining the typed wrapper, drive
`connector.discovery_mut().poll(now, budget)` alongside the responder and
`connector.poll`. The remote client is the sole consumer of its dedicated
session's plaintext. Hosts embedding a type-erased connector in Node must retain
an explicit polling arrangement when using the manual constructor. The driven
constructor below lets Node own that progress. The executable does not yet
automatically provision discovery sessions. There is no hidden background worker.

### Driven connector mode

`DiscoveryConnector::new_driven(connector, resolver, now)` selects the public
`DiscoveryDriver` extension. `NativeRemotePeerDiscovery` implements it; downstream
hosts can implement the same bounded progress, pending-work and deadline contract.
Construction requires an idle resolver and connector and returns both unchanged
on refusal. Use the resulting connector in ordinary `PeerParts`/`NodeParts`.
The same Node poll then drives lookup and connection establishment. The manual
constructor and explicit remote-client completion interface remain available.

In driven mode, a transient lookup miss accepts the original connection request
into a bounded waiting slot. This avoids making a short-lived refreshed hint
wait through a second roster retry delay before it can be used. Waiting requests
retain their exact tickets, original deadlines and caller directions; resolution
substitutes only a validated address before ordinary authenticated submission.
Waiting, terminal and submitted requests share the connector's declared request
limit, including one request per peer. Identity, deadline and capacity refusals
still return the original request synchronously. There is no stale-address fallback.

One discovery visit uses one connector visit and its existing per-visit session
I/O budget, then advances at most one waiting request. The waiting queue rotates.
With one visit, discovery and connection work alternate; zero-I/O turns do not
consume the next scheduling priority. Completion counts remain bounded. Wake
deadlines include pending lookups, negative retry delays and original requests.

Cancel suppresses a waiting connection attempt and retains its terminal slot until
polling; it does not revoke cached hints shared by other requests. Close also
closes the resolver view and suppresses pending lookup publication. Draining must
finish both request kinds before parts can be recovered. An already determined
local outcome stays retained if the underlying connector poll fails. Source
timeouts, malformed replies or unavailable endpoints remain resolver outcomes,
so a source outage cannot fence unrelated connected peers or live cached hints.
The host owns initial source-session provisioning and readiness wakes. The
reconnecting wrapper below can own subsequent source-session replacement.
After selecting driven mode, it must not independently consume resolver progress.

Slice209b checks downstream host budgets, exact request/terminal ownership,
construction refusal, cancellation, deadline, underlying poll failure and close.
A native three-node history uses ordinary Node polling,50ms leases, real TCP/TLS,
forced disconnect/reconnect, original retries, file reopen and worker joining.
It uses explicit long election timers to separate discovery from unrelated
hundred-group election pressure. A QUIC history uses connector polling alone to
fetch a hint and authenticate/exchange data with the pinned target. These checks
do not establish automatic source reconnection or the remaining recursive
parent-outage integration.

Cancellation takes the exact `RefreshRequest`, suppresses publication and retains
the slot until the reply or original deadline. It never extends the deadline.
Malformed/alien replies, session errors or timeout fail that source connection;
other live cached hints continue working until their own expiry. Misses return
`Unavailable` while `source_failed()` is true. `replace_session` requires a later
connection generation, the same local recovered-store binding and the same
remote peer/store identity. It resets request sequencing while preserving cache
floors and returns the previous session. Wrong replacements are returned intact.
Explicit `close` closes the view/cache and yields any pending terminal completion
on the next poll. `into_session` requires a closed/failed source and no pending
completion; the returned session remains the host's cleanup responsibility.
Neither cancellation nor local close undoes bytes already sent.

### Endpoint wire format

Version1 uses exactly96 bytes, all integers big-endian, with zero reserved bytes.
The format is separate from Raft RPCs and carries no read proof or credentials.

| Bytes | Value |
| --- | --- |
| 0..4 | `VBDH` magic |
| 4 | Version1 |
| 5 | Request1, hint2, missing3, unavailable4 |
| 6..8 | Reserved zero |
| 8..16 | Nonzero session-local request sequence |
| 16..24, 24..40, 40..48 | Node, store, store incarnation |
| 48..56, 56..58 | Hint generation and port |
| 58..74, 74 | IPv4/IPv6 bytes and family4/6; unused IPv4 bytes zero |
| 75..79, 79..83 | IPv6 scope ID and flow info; zero for IPv4 |
| 83..91 | Remaining hint lifetime in milliseconds |
| 91..96 | Reserved zero |

Fields after byte48 are zero outside hint replies. Decoding requires canonical
encoding; zero identities, unsupported versions, reserved data and invalid family
codes fail. Addresses and lifetimes undergo ordinary cache validation. Each
plaintext poll attempts at most one write prefix and one read prefix, each
bounded by the supplied byte limits and the remaining96-byte frame, alongside
one bounded SecureSession poll. No input length can request allocation.

The server caps remaining lifetime by configuration. The client also checks its
cap and computes expiry from **local request submission**, not response receipt.
Transit and processing delay therefore consume lifetime; a delayed reply cannot
renew an already expired hint. Server and client monotonic clocks need not share
an epoch. Generations/cache floors are volatile endpoint metadata, not durable
owner epochs. Restart requires explicit reconfiguration, as with the local cache.

Tests cover independent host sources, partial plaintext I/O, original completion
ownership, cancellation, negative retry, stale generations, capacity floors,
clock skew, late expiry, malformed frames, replay, source replacement and IPv6.
Real TCP/TLS and QUIC fetch/expiry histories retain unrelated cached peers through
source timeout. A separate real TCP/TLS test fetches an endpoint and uses it in
`DiscoveryConnector` to authenticate the pinned target despite stale caller
input. External manifest-fetch protocols, executable wiring, live QUIC migration,
macOS and separate-host validation remain outside this slice.

Slice209a adds shared short-I/O host, TCP/TLS and QUIC histories using a source
that retains its generation/address and returns a fresh bounded TTL for every
request, as the executable source does. Repeated expiry and local invalidation
renew successfully. Same-generation address conflicts, lower generations and
cancelled renewals cannot publish; exact next-request sequencing rejects a
replayed positive response. Explicit source-session replacement retains the
generation floor and permits fresh unchanged-endpoint renewal. TTL still starts
at local request submission. These checks cover persistent client instances,
not persistent cache files or automatic progress inside a type-erased Node.

### Reconnecting source inside an owning Node

Wrap a provisioned `NativeRemotePeerDiscovery` in
`ReconnectingPeerDiscovery::new(remote, source_connector, reconnect, now)`, then
pass that resolver to `DiscoveryConnector::new_driven(data_connector, resolver,
now)`. Select the resulting connector in the existing `PeerParts` assembly.
The source connector is dedicated to the configured discovery peer; the data
connector independently authenticates every peer selected by a hint. Initial
source authentication remains explicit host work.

`SourceReconnectConfig` supplies a numeric source endpoint, bounded retry delay
and a reserved inclusive connection-generation range. Reserve generations above
the initial source session, without reusing them in the same local store session.
Exhaustion stops reconnecting. Construction returns both original owners on
refusal. Polling the Node drives source repair through the existing bounded
discovery visits; no independent client-side poller is required or permitted.

After source failure, the wrapper releases the failed session before dialing so
QUIC can release its per-peer socket lease. It retains cached hints and generation
floors, verifies the replacement's peer, local store session, wire version and
connection generation, and preserves accepted request ownership through close.
Restart reconstructs the wrapper and its volatile cache from host input; it does
not persist floors or derive membership from discovery. Shutdown drains Node
work before reclaiming the resolver and joining its dedicated native dial worker.

Slice209c exercises host, TCP/TLS and QUIC source reconnection and refusal cases.
Slice209f composes the reconnecting resolver with an owning three-node TCP/TLS
service: the original source closes, a healthy data peer sustains a committed
write, and Node polling repairs the source and missing data connection. Original
operation receipts survive this repair and a full file reopen. A second history
drains pending discovery with the source offline. These selected histories do
not establish combined multi-authority migration, native startup provisioning,
or macOS/separate-host acceptance.

## Remote manifest provider (slice184)

[Remote manifest discovery](REMOTE_MANIFESTS.md) implements `ManifestDiscovery`
for a provisioned authenticated source. It composes with the original quorum-read
lookup driver and the existing route resolver; it does not mint local read
barriers or activate ownership.

## Counter executable integration (slice188)

The counter executable composes NativeDiscoveryResponder with an immutable,
bounded command-endpoint view. `--discovery-peers` loads the existing command
file at startup; `--discover-via` selects a separately pinned source. A flushed
versioned acknowledgement hands the existing authenticated session to the binary
protocol, without losing its credential guard. The original command deadline
bounds all refreshes. The server owner loop remains the progress driver.

The original command v1 profile uses the recovered source store session as its
hint generation and a30-second TTL. Command v2 uses an explicit generation and
supports checked volatile address updates. Slice209i adds an exact Raft peer
profile, `voteboat-peer-discovery-v1 GENERATION`, binding each hint to its node,
store ID and incarnation. It uses the same authenticated source protocol and
bounded updates; command identities cannot resolve its entries. See
[source formats and restart rules](COUNTER_SERVICE.md) for configuration.
The command CLI starts a new cache per invocation and does not compare
generations from different sources or persist addresses after failed refresh.

Only addresses come from discovery. Requested service identities, certificate
pins and TLS names remain client configuration. The source cannot redirect a
client to an unpinned identity. Tests in tests/counter_service/command_discovery.rs
exercise changed versus bootstrap addresses, access denial, missing mappings,
source-name refusal and original retries after checkpoint/restart over TCP and
QUIC peer clusters. These are loopback tests, not separate-host certification.

The peer-source profile is consumed through the public Rust discovery API;
automatic counter Raft-startup consumption remains separate. The directory
executable and remote manifest provider supply responsibility lookup separately;
peer endpoint hints do not publish responsibility manifests or change placement.
