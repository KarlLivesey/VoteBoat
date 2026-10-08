# Delegated-child ownership transfers

A delegated parent routes to a child metadata authority and a specific child
ownership epoch. Updating only the child's execution manifest leaves cold parent
routes stale. `delegation::DelegationPlan` and the existing `LifecycleDirectory`
journal now supply a reserved, durable parent update for a child split/merge.
The same-group and remote-parent variants use the same checked protocol types.

1. Read the current parent and child manifests through authenticated quorum reads.
   Construct a `DelegationPlan` with the parent, exact child before/after manifests
   and child lifecycle operation. The child must remain under the same parent,
   metadata authority, scope, adapter and routing scheme. Its checked transfer
   changes Single/Partitioned execution to disjoint concrete groups with the next
   ownership epoch and generation. Unbound delegated `TransferIntent::new` refuses.
2. Commit the encoded plan in the parent directory under a distinct reservation
   operation. It locks that parent manifest generation and reserves final control
   history before any source fence. Concurrent parent updates return LifecycleBusy.
   Obtain `DelegationReservationStatus` through `DelegationReservation` quorum
   query, then call `child_intent(parent_configuration)`.
3. Authenticate this observation/configuration before using it in the child.
   Contract-version-2 intents encode a 120-byte binding under VBTINT02: parent
   digest/generation, reservation operation/index/configuration, child operation
   and a digest over these facts plus the canonical child transition. Decode
   recomputes the digest before admission/fencing. Existing top-level VBTINT01
   remains unchanged. These hashes detect changed content; they do not authenticate
   invented foreign observations.
4. Commit the child intent, stage targets, freeze sources, import actual source
   images and publish the child handoff using the existing protocol. Source fences,
   target construction/import, later-source freezes and checked publication verify
   the bound child operation. A local parent/child directory checks its actual
   retained reservation rather than trusting an external substitute.
5. Read the committed child publication and encode `DelegationCompletion` with
   that decision and the exact reservation/configuration context. Commit it in the
   parent using a new operation ID. It matches the exact reservation and bound
   child intent before changing one child route epoch and incrementing the parent
   route generation. For same-group metadata, the actual retained child decision
   must match and both configuration observations must name the same group context.
6. Activate targets using the child decision and existing activation gates.
   Parent route publication alone never creates a serving owner. Every old source
   remains fenced. Target activation is locally retained; ordinary child writes
   and warm route resolution need no ancestor commit or parent connection.

The parent's own ownership epoch, authority, scope, other routes and parent binding
stay unchanged. Its grandparent therefore retains a valid child locator. This
updates an existing delegated child's epoch; it does not create/delete/reparent
namespaces or migrate a parent's own application state.

Original reservation/completion status remains queryable after completion.
`DelegationPublication` exposes the parent result through the same quorum read
view. Retries retain original outcomes; replay/checkpoint reconstruct locks,
control reservations and parent manifests from the ordinary Directory journal.
`VBDPLAN1` plans are bounded at 64 KiB; `VBDCOMP1` completion has the existing
64 KiB child-publication budget plus bounded context. Directory control slots
now reserve up to 64 KiB + 128 bytes per configured slot; top-level transfer
reservations retain their existing 64 KiB size. Readiness advertises the larger
command/checkpoint requirement. Existing journal schema/tag is unchanged; old
readers cannot process the new request tags. No mixed-version deployment is claimed.

Preflight parent and child ordinary/control history, target import/export limits
and source admission before fencing. Parent reservation is a conservative lock
with no timeout or unfreeze shortcut. A concurrent remote child
metadata edit can invalidate the reserved before manifest; detect that mismatch
before fencing and report the conflict. Recovery before a successful child intent
can use the permanent-refusal protocol below. Never infer
permission to replace an unreachable child from timeout or parent liveness.

Five downstream deterministic tests exercise real split images/imports/publication/
activation, original target retries, parent/child checkpoint and parent-log replay,
same-group local provenance, three-level cold resolution and parent-free warm
writes. They cover changed operations/configuration/reservation, concurrent parent
updates, ordinary operation/byte exhaustion, competing pending final commands,
all new codec/checkpoint truncations and a bit flip at every byte position of a bound
intent. Two additional regressions check the public child-locator refresh predicate
and native cache admission. At unchanged parent ownership epoch, a newer route
generation can advance child epochs only with unchanged ranges, child identities
and metadata groups. Refused refreshes preserve the previous cached view.

`tests/routed/delegation.rs` adds a native recovery ledger with independent
three-replica grandparent, parent, child metadata, source and two target groups.
Targets are created with bindings derived from the actual committed parent
reservation. Each action completion is discarded; resumption queries committed
status again after all established groups reopen. The 11 phases span reservation
through both activations. Parent outages check source continuity before fencing
and refusal by both old and unactivated new owners after child publication.
The existing parent cache accepts the final child-epoch refresh, while the
grandparent locator stays unchanged. Direct target retries and new writes are
also checked with all ancestor and source workers stopped, then reopened again.
Execution results and transport/storage coverage are recorded in validation/REPORT.md.

`tests/delegation/repeat.rs` additionally composes an actual delegated split,
two-source merge and further split. Each later source is the previous activated
target, carrying its actual data, deduplication and outbox history. Parent/child
journals and later target phases recover from checkpoints; target restoration
uses fresh unstaged applications constructed from retained original child intents.
Original retries preserve their historical outcomes without duplicate outbox
records; new final writes execute with only the child manifest needed for routing.
Parent ownership epoch/ancestry and grandparent locator remain unchanged. Old
owners remain fenced, and stale intents or wrong bound freeze operations refuse.

`tests/routed/delegation_repeat.rs` extends the native assembly through both later
moves over TCP/TLS and QUIC, with WAL and checkpoint recovery. All four selected
histories pass: every created group reopens after each of 20 later committed
phases, including partial merge fencing and parent outages after child publication.
Final child-only resolved retries and new writes succeed with all metadata and
old-owner workers stopped; final values 10/16 survive a further reopen. Immutable
original decisions and the grandparent locator remain unchanged.

This is selected native split and repeated-movement evidence, not arbitrary-fault liveness,
mixed-version, macOS or separate-host validation. Those remain in the active P0–P7
scope; ordinary top-level handoffs and the static service remain usable.

## Abandoned reservation recovery

A parent reservation can be released only before a successful child intent exists:

1. Read the actual committed parent reservation and derive its bound child intent.
   Before creating reservations, preflight ordinary child history credit for a
   possible refusal as well as the normal lifecycle envelopes.
2. Commit `DelegationDecline::new(intent)` at the child under a distinct operation
   ID. The child permanently refuses that reserved child operation. The child log
   orders any racing intent and decline: a successful intent first causes decline
   to return `LifecycleBusy`; decline first prevents the old intent from succeeding.
   A successful intent remains protected even after publication. Continue such
   transfers forward; cancellation does not thaw any source.
3. Obtain the original `DelegationDeclineStatus` through a quorum-backed
   `DirectoryQuery::DelegationDecline(child_operation)`. Commit
   `DelegationCancellation` at the parent, binding the exact reservation index,
   parent/child configurations and that actual refusal. Same-group metadata checks
   the locally retained refusal. For remote groups, the host must authenticate the
   committed observations, as with existing handoff evidence; constructed status
   bytes grant no authority.
4. The parent releases the reservation using its reserved final control credit.
   Every manifest, route generation and ownership epoch stays unchanged. Query
   `DelegationCancellation(reservation_operation)` to recover a lost receipt.
   Replan with fresh operation IDs and actual compatible grants. Cancellation
   cannot repair an unrelated incompatible source application grant.

Refusals, cancellations and original retry outcomes reconstruct from the bounded
Directory replay journal and checkpoints. Child refusal requires ordinary history
credit; parent release can finish with its ordinary history full. New `VBDDECL1`
and `VBDCANC1` requests require the new reader; no mixed-version rollout is claimed.
Six tests in `tests/delegation/cancellation.rs` cover the ordering race, local
provenance, failed old intent, full history, competing final credit, query bounds,
codec/checkpoint truncations and a complete fresh split with actual imported data.
The selected native TCP/TLS WAL and QUIC checkpoint histories in
`tests/routed/delegation_cancel.rs` pass: refusal/cancellation receipt loss and
restart, source continuity during parent outage, permanent old-intent rejection,
all eleven fresh split phases, and final target retry/data recovery. The full
transport/storage cross-product was not run for cancellation. Execution results
and broader limitations are recorded in validation/REPORT.md.
