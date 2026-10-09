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

This is peer-address discovery, not complete C17. Responsibility/authority-hint
resolution, dynamic executable endpoint refresh, external discovery protocols and
refresh scheduling remain outstanding. Real discovery integration was executed
with TCP/TLS; QUIC service regressions do not prove a discovery-selected QUIC
connection. No macOS/separate-host, arbitrary-fault or performance claim follows.
