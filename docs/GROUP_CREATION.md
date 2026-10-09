# Metadata-authorized group creation

Creation is opt-in through `Directory::with_group_creation()` before bootstrap or
application. It selects application schema2, initialization `VBDINIT2` and
checkpoint `VBDIR002`. Default Directory remains schema1 with its existing formats
and rejects creation. Mode selection must agree on every metadata replica and on
reopen. Cross-mode replay/checkpoint restore fails closed; this is not live upgrade
or mixed-version support.

`GroupCreationIntent` encodes a bounded `VBGCRT01` command containing metadata
authority, existing parent responsibility and exact generation, fresh
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
grants neither namespace publication nor serving authority. Native direct-group owning-service
creation is exercised below. Imported groups must remain non-serving
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
modeled journal faults include unsynced-byte loss. That bootstrap evidence does not establish arbitrary crash schedules. Separate
native service and fresh namespace evidence is recorded below.

## Native direct-group embedding path

The existing native owning startup composes with offline creation without a new
worker or provisioning endpoint:

1. Commit the schema2 Directory reservation and obtain the exact status from
   authenticated committed metadata history. Check the target adapter assignment
   with `VerifiedGroupCreation::verify`.
2. Explicitly create or recover the selected `NativeLogStore<FileLogIo>` and open
   `FileCreationBindings` in that directory. Run `establish_created_group`; retain
   files on uncertain errors and retry the same intent after recovery.
3. Explicitly initialize the empty `NativeSnapshotStore<FileSnapshotIo>` with the
   same target group/store identity, or recover its existing manifest. Never reset
   an existing snapshot store. Keep partial initialization as a recovery case.
4. Drop offline file owners, then select `NativeStartup` with `Recover`, the exact
   reserved bootstrap and provisioned peer credentials/endpoints. Open the normal
   application through `open_with_protocol` for TCP/TLS or QUIC.
5. Use the owning Node's existing campaign/propose/read/checkpoint/shutdown paths.
   Creation metadata is historical bootstrap authority; ordinary group operations
   do not require ancestor writes or a live metadata group.

This is direct group embedding. A routed responsibility still needs a separately
validated publication/activation; this recipe does not grant namespace ownership.
Empty initialization is exercised here. Staging/import and a creation RPC/CLI are
separate contracts. A missing/corrupt snapshot manifest after partial initialization
must be inspected; this recipe does not silently fall back to recreating files.

`cargo +stable test --locked --offline --all-features --test routed native::creation`
runs actual TCP and QUIC histories. They commit metadata reservation, provision two
assigned stores, reopen metadata and retry the original reservation, recover those
two stores and finish the third, then stop metadata. The created recursive-quorum
three-node service elects, writes, reads, checkpoints, closes and reopens; exact
bootstrap retry preserves each progressed WAL, application retry preserves its
original operation/outcome, changed bytes conflict, and a new write succeeds.
Every metadata replica's WAL remains exactly unchanged during child operation.
These are selected Linux loopback schedules, not arbitrary interruption or power
failure coverage; no namespace activation or macOS execution is claimed.

## Fresh independent namespace publication and activation

Select `Directory::with_namespace_creation()` before bootstrap on every metadata
replica. This selects schema3, `VBDINIT3` and `VBDIR003`; schema1/2 histories keep
their existing formats. Cross-mode initialization/replay/restore refuses; no live
schema upgrade or mixed-version deployment is supplied.

`NamespacePlan` binds the original `GroupCreationStatus` to a fresh independent
root manifest: parent=None, epoch/generation1, matching application adapter,
Single reserved group and Empty mode. The reservation's existing parent authorizes
the administrative creation intent. It is explicitly not a routing-parent edge
for this new independent namespace. Existing namespace selectors/routes stay
unchanged. Inserting a recursive child into an already covered selector still
requires source fencing and transfer; this path cannot replace that protocol.

`CreatedNamespace<A,P>` wraps the existing RoutedApplication and host application/
partition contracts. Its initialization command binds the full plan, selected
limits and initial application checkpoint. Committed initialization produces
readiness but keeps data writes and linearizable reads non-serving. `NamespaceQuery`
includes an explicit status read; use the normal quorum-read path to obtain original
committed ready facts. `NamespacePublication::from_status` checks the plan digest
and target-ready index. Configuration and metadata creation operation/index remain
bound to the exact original intent.

The schema3 directory accepts publication only for its exact retained reservation
and a previously unknown namespace. It reserves bounded control space when creating
the intent so ordinary operation/history exhaustion cannot strand valid publication.
The publication records the fresh manifest and original outcome; retry/checkpoint
replay reconstruct them. It does not activate the target. Once the authenticated
committed `NamespacePublicationStatus` is verified, the target commits its own exact
activation command and only then serves through ordinary routed owner checks.
Initialization and activation IDs cannot be reused as client operations. Control
retries preserve their original ready/activation indices across checkpoint recovery.

These constructible plan/status/digest values verify structure and exact binding;
they do not authenticate a foreign group or prove commitment. Trusted non-Byzantine
hosts must authenticate target-ready and metadata-publication observations and
restrict control commands before proposal, exactly as for existing transfer
commands. Do not expose an unrestricted client command channel as administration.
There is no creation RPC/CLI or automatic remote authority verifier in this slice.

Fresh namespaces currently use fixed ownership under this guard. They do not yet
participate in general source export/freeze, deletion/reparenting or recursive
selector insertion. Such work remains in the full P5/P6 goal. Imported/Staging
creation remains distinct and is rejected by this fresh-empty plan.

`tests/namespace_creation.rs` covers exhausted ordinary directory capacity,
publication mismatch/refusal, canonical/truncated plan/publication/checkpoint
formats, schema refusal, non-serving admission/apply/read, exact activation,
operation collision, original data retries, receipt capacity and checkpoint control
index consistency. Native ModelIo cuts every activation WAL frame byte and injects
sync/publication faults with unsynced loss/reopen. Two native TCP/QUIC histories in
`tests/routed/creation.rs` checkpoint/reopen after readiness, publication and activation,
retain original publication/data retry results and keep metadata WALs unchanged
while the activated namespace operates offline. These are selected Linux schedules,
not a full distributed proof or arbitrary hardware power-failure coverage.
