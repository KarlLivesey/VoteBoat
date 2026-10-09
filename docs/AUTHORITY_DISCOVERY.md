# Responsibility-authority discovery

`routing::ManifestDiscovery` is C17's nonblocking source of previously authorized
manifest observations. `lookup` takes an `AuthorityLocator` naming an exact
responsibility incarnation and directory group incarnation, plus optional minimum
ownership epoch and route generation. It returns an owned bounded manifest,
provider-scoped observation ID and monotonic expiry. `invalidate` names that exact
observation; `close` closes the selected provider view. Hosts drive directory
reads or authenticated remote refresh separately. There is no implicit network
request, worker, DNS, global cache or blocking callback.

`resolve_discovered` consumes this public seam with either host or native sources.
It first calls the existing checked cached-path resolver. Only an exact miss causes
a lookup; child authority and minimum epoch come from that path's child locator.
Source identity, manifest identity/authority, minimum epoch/generation, expiry and
maximum retained bytes are checked before normal `ManifestCache::admit`. The
ordinary resolver then rechecks parent/child bindings, routing scheme, coverage,
cycles, owner epoch and fenced state. A fenced manifest can be cached as a hint,
but cannot resolve an active route.

The request carries the key, maximum path visits (1..32), maximum source lookups
(0..path limit) and host monotonic time. At most that many missing segments are
fetched. Budget exhaustion returns the exact missing responsibility; source/cache
errors do not start an inner retry loop. Earlier successfully admitted segments
remain useful after a later miss/refusal. Hosts must bound retries/backoff outside
this function and preserve operation ID/payload when eventually proposing writes.
A cache-capacity refusal drops only the temporary source result; the provider's
owned observation remains intact.

Starting from a cached child locator requires no parent/source lookup. Expiry
controls discovery admission, not the lifetime of already cached routing hints.
TTL is never a leader lease or permission to execute. Targets still independently
check committed/applied ownership at admission and application, plus leadership,
activation, durable fencing and original operation deduplication. A stale hint
cannot revive a retired/fenced owner. This seam does not create namespaces or
implement dynamic directory ownership changes.

## Native source and read lifetime

`native::authority_discovery::NativeAuthorityDiscovery` retains a bounded
`NativeManifestCache` and fixed metadata per responsibility. Construction supplies
manifest-count/retained-byte ceilings, a 1..3,600,000 ms observation lifetime,
exact `ReadInvocationBinding` and initial monotonic time. Retained floors count
against capacity; invalidation/expiry do not evict them. Payload allocations stay
within the existing manifest bounds; each lookup makes one bounded owned clone.
The observation map has at most the cache's bounded manifest count.

The trusted host submits the original completed Directory read's invocation
ticket and `ReadOutcome<Option<ResponsibilityManifest>>` to `observe`. The binding,
group and request must match, and only a successful read with the exact manifest
responsibility/authority is admitted. The barrier is produced and consumed through
the existing quorum-backed Node read path. A `ReadOutcome` is host-local evidence,
not a signed remote bearer credential: hosts must not attach an unrelated query,
forge ticket fields or substitute an unverified remote response.

A single highest accepted read sequence bounds replay tracking. An older completion
may be rejected even if it was a valid concurrent read; the host can issue a fresh
read. Read index/term cannot regress for a known responsibility. An exact still-
live duplicate returns the same observation ID without extending expiry. After
invalidation/expiry, that duplicate cannot refresh authority. A fresh original
read can refresh an unchanged manifest generation, without a directory mutation.
This separates committed route generation from volatile observation identity.

Successful new observations pass the existing cache's generation, epoch, immutable
lineage/schema and fence constraints before publication. Rejections return the
original outcome/manifest allocation. A successful quorum read reporting None
returns Missing and invalidates a matching previous observation, retaining its
manifest floor; this is a source absence observation, not proof of general namespace
retirement. Other unavailable read outcomes do not fabricate manifests.

Invalidation matches source authority, responsibility and observation ID, so an
old observation cannot remove its newer replacement within the provider. Closing
rejects new observation/lookup; there is no pending network work to drain.
Observation IDs are local to this provider lifetime: discard them on provider
replacement, rebuild the source empty and refetch using the new read binding.
No volatile observation grants persisted authority across restart.

## Executed evidence and remaining scope

`tests/routing.rs` supplies downstream source/cache providers through the common
consumer. Tests cover missing-path-only lookups, wrong source/manifest, expiry,
lookup and cache budgets, minimum epoch, fenced paths, independently closable
shared source views and cached child resolution after source/parent removal.
Existing cache tests cover generation/epoch/lineage and conditional invalidation.

The real TCP/TLS and QUIC histories in `tests/routed/native.rs` now obtain actual
Directory quorum read tickets/outcomes, feed the native discovery provider and
resolve routes through the common helper. They reject wrong source, invalidated
and older read replay, reject stale epoch, refresh from a fresh read, and preserve
a newer observation against old invalidation. After closing discovery and parent
roles, children still commit/retry/read, checkpoint and restart. No ancestor commit
is inserted in that child path. Full routed lifecycle regression execution is
recorded in validation/REPORT.md; these finite histories are not a full proof.

Automated remote fetch/scheduling, signed external directory protocols, dynamic
namespace creation/deletion/reparenting and relocation of directory authority's
own data remain separate work. Native construction does not select an external
issuer or trust unknown groups. Linux execution does not establish macOS,
separate-host, arbitrary-fault or sustainable performance behavior.
