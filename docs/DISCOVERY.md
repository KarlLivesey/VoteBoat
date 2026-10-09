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
no implicit eviction; capacity includes floors. A host must size the provider for
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
no core edits are required. The executable startup path retains its existing
static endpoint configuration in this slice.

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

This is peer-address discovery, not complete C17. External remote manifest fetching,
dynamic executable endpoint refresh, external discovery protocols and refresh
scheduling remain outstanding. Real selected discovery connections include
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

`routing::ManifestReadSource` (contract version1) exposes original trusted
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
An external remote lookup protocol and executable endpoint refresh remain work;
existing cached child routes retain their parent-independent behavior.

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
