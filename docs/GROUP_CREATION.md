# Metadata-authorized group creation

Creation is opt-in through `Directory::with_group_creation()` before bootstrap or
application. It selects application schema2, initialization `VBDINIT2` and
checkpoint `VBDIR002`. Default Directory remains schema1 with its existing formats
and rejects creation. Mode selection must agree on every metadata replica and on
reopen. Cross-mode replay/checkpoint restore fails closed; this is not live upgrade
or mixed-version support.

`GroupCreationIntent` encodes a bounded `VBGCRT01` command containing metadata
authority, existing parent responsibility and exact generation, fresh child
responsibility, exact group/bootstrap configuration and recursive voter/store
map, target application adapter/version, and empty or staging initialization.
Host authorization and placement validation remain required before proposal.
Constructing or decoding this public value supplies no remote authority.

Applied metadata execution reserves the exact new group/responsibility identities
in ordinary Directory operation/byte budgets. Known IDs, including attempts to
replace a known group using another incarnation, are refused. An unknown/stale
parent, foreign authority or parent lifecycle reservation cannot be bypassed.
The original operation/content returns its original outcome; altered content
conflicts. A new operation cannot claim an already reserved identity. Reservations
and original statuses rebuild from retained command history in WAL/checkpoints.

`group_creation_at(required, group)` is a local applied-prefix lookup. A caller
must establish metadata quorum read authority and authenticate the exact metadata
source before using the status remotely. The status is not a cryptographic or
transferable commitment certificate. Legacy mode returns UnsupportedSchema rather
than reporting absence for an unsupported capability.

`VerifiedGroupCreation::verify` checks the exact assigned node/store and application
adapter against a `CreationAuthority`. `LocalCreationAuthority` requires the actual
metadata Raft committed prefix and matching Directory history; a lagging replica
cannot authorize an unapplied reservation. A remote authority adapter must perform
authenticated source/commit verification under the trusted non-Byzantine model.

`establish_created_group` provisions caller-owned providers offline. The optional
`CreationLogStore` capability reports typed durable presence/absence and refuses
pending writes. `CreationBindings` publishes immutable bounded provenance before
the selected WAL receives Create and its barrier. Native `FileCreationBindings`
uses a scoped lock, synced pending file, non-overwriting hard-link publication and
directory synchronization. Exact retry reestablishes durability after an uncertain
receipt. Conflicting bindings or an existing unbound group refuse; matching groups
retain their progressed logs. There is no rollback/delete-on-error path.

The binding records metadata operation/index, exact intent, node and store. It is
static provenance, not another consensus log. The initial native binding provider
holds one creation per selected directory; sharing a WAL among multiple newly
created groups needs a separately scoped binding provider. Store open/creation and
snapshot initialization are explicit caller responsibilities. A bootstrap receipt
grants neither namespace publication nor serving authority. Native owning-service
creation remains the next integration gate. Imported groups must remain non-serving
until the existing import/publication/activation protocol authorizes them. A creation
intent cannot replace an unreachable owner or discard its data.

Tests in `tests/directory/creation.rs` cover truncation/canonical decoding, bounded
policy/store counts, exact bindings, authority/generation/identity refusals, parent
transfer locks, operation retries/conflicts, pending budget, atomic batch failure,
checkpoint replay and schema refusal. Native directory tests use three Raft cores
with actual WAL/checkpoint files, lose an observation, compact, reopen and catch up
a lagging replica, then authorize an assigned target WAL, reopen it and persist
its campaign ballot before vote messages. `tests/group_creation.rs` exercises host
authority/log/binding replacements, refusal, exact retry, preserved progressed state
and every torn native bootstrap frame plus barrier/publication faults. Native file
unit tests reopen at five binding publication boundaries and reject altered bytes;
these are observation-loss tests, not hardware power-loss simulation. Separate
modeled journal faults include unsynced-byte loss. No arbitrary crash schedules,
TCP/QUIC created-service startup or namespace activation claim is made.
