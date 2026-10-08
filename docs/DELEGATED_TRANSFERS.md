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
with no timeout, cancellation or unfreeze shortcut. A concurrent remote child
metadata edit can invalidate the reserved before manifest; detect that mismatch
before fencing and report the conflict. A safe cancellation/replanning protocol
for such abandoned pre-fence reservations remains outstanding. Never infer
permission to replace an unreachable child from timeout or parent liveness.

Five downstream deterministic tests exercise real split images/imports/publication/
activation, original target retries, parent/child checkpoint and parent-log replay,
same-group local provenance, three-level cold resolution and parent-free warm
writes. They cover changed operations/configuration/reservation, concurrent parent
updates, ordinary operation/byte exhaustion, competing pending final commands,
all new codec/checkpoint truncations and a bit flip at every byte position of a bound
intent. This is selected split evidence, not native delegated-network recovery,
delegated merge/repeated moves, pre-fence cancellation, arbitrary-fault liveness,
mixed-version, macOS or separate-host validation. Those remain in the active P0–P7
scope; ordinary top-level handoffs and the static service remain usable.
