# Remote manifest discovery

`native::remote_manifest::NativeRemoteManifestDiscovery<S, C>` implements the
public `ManifestDiscovery` contract over an authenticated `SecureSession` and a
host-selected `ManifestCache`. TCP/TLS and QUIC use the same component. The host
provisions the exact source node/store identity and permitted authority group;
this protocol does not discover trust or grant authority.

`NativeManifestResponder<S, D>` owns a selected `ManifestDiscovery` source. Use
`NativeManifestLookup<Node<Directory>>` for observations checked against original
quorum-backed directory reads. The host polls that original Node, the lookup
driver, the responder and the remote client. A host replacement source has the
same responsibility to supply authorized observations. No worker, socket,
runtime or implicit polling loop is created by these components.

The [metadata authority executable](DIRECTORY_SERVICE.md) now composes this
protocol with a real replicated Directory, explicit initial publication and
quorum-backed lookup. Recursive multi-authority CLI routing remains separate.

## Request and cache ownership

A cache miss queues one request and returns `Unavailable`. The exact pending
`ManifestRefresh` identifies the session binding, sequence, query, submission
time and deadline. Repeating the same lookup does not queue another request;
another uncached query returns `Overloaded`. Polling returns the original request
with its terminal result. Successful lookup returns a provider-local observation
ID and expiry. `resolve_discovered` can consume this provider directly.

Replies must match the sequence, responsibility incarnation, authority and
minimum epoch/generation. The existing canonical manifest decoder and cache
admission rules validate manifests, lineage, generations and conflicts. An
unchanged manifest may receive a fresh observation; invalidation matches both
its locator and observation ID. These are routing hints: the receiver never
constructs a `ReadBarrier`, authorizes a member or bypasses owner-side fencing.

The constructor requires an empty valid cache. Its count/byte limits bound
retained manifests and observation metadata. Expired and invalidated entries
retain their conflict/generation floors; this provider does not silently evict
those floors. Reaching capacity rejects a new responsibility while retaining
existing entries. One input and one output frame are bounded by
`92 + MAX_MANIFEST_BYTES`; decoding checks the header before accepting a body.

`RemoteManifestConfig` selects request timeout, negative retry delay and maximum
hint lifetime. Defaults are 10 seconds, 100 milliseconds and 60 seconds. The
receiver counts lifetime from local request submission, conservatively including
network delay. Clocks need not be synchronized. Source failure preserves unrelated
unexpired cached observations. Cache lookup itself does not contact an ancestor.

## Cancellation, reconnect and shutdown

`cancel` matches the exact pending refresh. It suppresses publication but retains
the slot until its response or timeout. Timeout or protocol/session failure
closes the connection and returns the original pending completion. An explicit
`replace_session` requires the same provisioned peer, the same local identity
and a later session generation; cache floors and request sequencing survive.
Rejected replacements return the original session.

`close` prevents further lookups and closes the session. Poll once to consume an
outstanding terminal completion. `into_parts` returns session/cache only when
closed or failed and no request remains. Cache state is volatile; reopening a new
provider does not recover observations.

Responder close cancels its selected source view. `into_parts` returns the
session and original source, including any accepted read work. With
`NativeManifestLookup`, consume its terminal result and keep polling its original
Node until any underlying read request credits drain. Cancellation can end the
selected wait before the original quorum work finishes. It does not roll back
or abandon the original Node's ownership. Close/join the Node normally afterward.

## Wire version 1

All integers are little-endian. The frame is separate from Raft RPCs. Dedicated
sessions have one client, one responder and one configured authority group.

| Bytes | Value |
| --- | --- |
| 0..4 | `VBMD` magic |
| 4, 5 | Version 1; request 1, hint 2, missing 3, unavailable 4 |
| 6..8, 8..12 | Reserved zero; total frame length |
| 12..20 | Nonzero request sequence |
| 20..44 | Authority group ID and incarnation |
| 44..68 | Responsibility ID and incarnation |
| 68..76, 76..84 | Minimum epoch and route generation; zero means absent |
| 84..92 | Remaining hint lifetime; zero for other kinds |
| 92.. | Existing canonical manifest encoding for a hint only |

Unknown versions/kinds, noncanonical fields, invalid lengths and unsolicited or
mismatched replies close the session. There is no negotiation or signed external
proof format. Compose multiple provisioned authorities explicitly in the host.

Seven downstream host tests exercise short I/O, bounds, malformed requests and
replies, cache capacity, stale generations, cancellation, reconnect and expiry.
Two native TCP/TLS/QUIC histories fetch a three-level route from real directory
quorum reads, drain a disconnect during an accepted read, then serve duplicate
safe child writes/reads with metadata offline and reject a stale ownership hint.
Parent WALs recover unchanged. This is a Rust embedding component; the service
executable does not yet expose remote manifest discovery configuration or commands.
