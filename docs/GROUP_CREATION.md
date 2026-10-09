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

For fresh metadata groups that need ownership transfers of created namespaces,
select `Directory::with_namespace_transfers()` instead. It includes namespace
creation and binds schema4, `VBDINIT4` and `VBDIR004` before bootstrap. Schema4
admits the published namespace's exact current manifest to the existing transfer
intent/publication protocol, preserving lifecycle locks, target exclusion and
reserved control capacity. A creation reservation alone is insufficient. Schema3
retains its original refusal for namespaces absent from the bootstrap plan; its
checkpoints replay historical commands and must not reinterpret those refusals.
There is no live schema3-to-4 upgrade. Select the same schema on every replica.

The schema4 conformance histories in `tests/namespace_creation/transfer.rs` use
constructed source/import facts to test metadata reservation, publication, split/
merge retry and checkpoint recovery. They do not demonstrate actual created-owner
fencing or target import. `CreatedNamespace` still wraps a fixed RoutedApplication;
source-capable activation-guard assembly is described below.

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

The interrupted TCP/QUIC variants also leave original initialization, publication
and activation client tickets unread. Publication and activation progress on only
two replicas; the third lacks the committed phase and stays non-serving. Owner abort
stops protocol polling, discards worker observations and releases actual native
stores before WAL-only reopen. Exact retries repair the lagging replica and retain
original phase indices/outcomes. Assertions compare actual quorum and laggard WAL
commit prefixes to the phase boundary. This is distinct from hardware power loss;
accepted native I/O can finish after owner abort.

## Source-capable created namespaces

Choose `CreatedNamespaceSource<A,P>::from_source(plan, source)` before any namespace
initialization. Construct `source` with the existing `TransferSource::new(routed,
export_bytes)` over a fresh `RoutedApplication` whose group/grant exactly match the
plan. The application implements the existing scope, checkpoint and bounded result
contracts; the partition provider remains injectable. Constructor refusal returns
the original plan and source. It refuses progressed owners and mismatched bindings.
The same creation/namespace publication protocol supplies readiness and activation.

This selects guard schema2, `VBNINIT2` and `VBNCHK02`; the binding includes the source
bootstrap/export budget. Fixed `CreatedNamespace<A,P>` remains schema1 with identical
initialization/checkpoint formats. No fixed-to-source live upgrade, cross-owner
checkpoint restore or replacement of an established namespace is supplied. Metadata
schema4 is required to record transfer intents for dynamically created namespaces.

`CreatedNamespace` now has a defaulted core owner type parameter. Its sealed
`NamespaceOwner` contract admits only existing `RoutedApplication` and `TransferSource`
guards; host application/partition providers cannot replace core activation/fencing.
`owner()` exposes an immutable selected owner. Fixed `NamespaceQuery`/`NamespaceRead`
remain aliases for the original routed shape; `SourceNamespaceQuery`/`SourceNamespaceRead`
wrap existing `SourceQuery`/`SourceRead`, including explicit frozen status.

Before namespace activation, owner reads/writes and source freeze remain non-serving.
After activation, propose the existing `TransferSource::freeze_command` through the
outer namespace guard under the authenticated trusted host's verified metadata
intent/target staging. Raw owner bootstrap and unbound routed fence commands refuse.
Original namespace creation/activation operation IDs cannot become data or freeze
IDs. On a committed fence, source routed/application state remains exactly at F
while the source/namespace outer applied prefix can advance. Exports come from
`owner().export_target` and retain the existing digest/scope/retry/outbox checks.
Ready/activation status is historical provenance after fencing, not serving permission.
Use the guarded data read and frozen-status query when deciding current service state.

Recovery validates owner format/binding, source recovery, readiness/activation indices,
fence strictly after activation and control operation exclusion. Exact control/freeze
retries retain original facts without reinitializing a frozen owner. Selected
deterministic tests hand actual exports to staged targets, publish the checked
transfer, activate both targets, and verify imported retry/outbox continuity. The
native ModelIo test cuts every fence frame and exercises sync/publication failures:
recovery observes either the old active owner or complete fence, and exact retry
converges to the original export boundary. Existing TCP/QUIC fixed-owner creation
histories pass after guard generalization; selected source-capable native network
phase/reopen histories are described below. No arbitrary-fault, macOS, separate-host, recursive
insertion or complete lifecycle-proof claim is made by these selected histories.

The four source-capable native histories in `tests/routed/creation_source.rs` combine
TCP/QUIC with WAL/checkpoint recovery. Metadata schema4 commits the actual source
assignment; verified assigned bootstrap establishes all three source stores and
immutable creation records before elections. Twelve complete-reopen phase boundaries
cover namespace ready/publication/activation, two data writes, intent, both stages,
source fence, both imports and transfer publication. Original phase results remain
unread until owner abort, committed statuses drive recovery, and original retries
retain indices/digests and source creation records. Snapshot variants compact each
selected prefix before abort; they cannot use graceful drain while client results
are intentionally unconsumed. WAL variants recover from actual base0 logs.

After publication both source and metadata are completely offline for the original
target activations. Each activation remains unread and recovers through target
abort/reopen/retry. The first target serves while the other remains NotActive.
Imported retry values and outbox items survive; new target operations continue
independently. Exact metadata file bytes, including selected WAL/manifests/snapshots,
and recovered GroupLogs remain unchanged during the outage. Final all-group recovery
retains original lifecycle statuses, updated target data/outbox and stale-route/
fenced-source refusal. These selected Linux loopback/owner-abort histories add
native integration evidence; recursive insertion and broader lifecycle/platform/
arbitrary-fault validation remain separate work.

## Inserting children into an existing responsibility

Slice130 adds an explicit same-authority insertion path. Select
`Directory::with_responsibility_insertion()` before bootstrap (schema5). Reserve
fresh child groups in `Staging` mode under the current root generation. Build each
`InsertionChild::from_creation(child_manifest, committed_status)` and a
`TransferIntent::insert_children(before, delegated_after, children)`. Child scopes
must exactly match the parent's complete delegated selectors; each child starts
with epoch/generation1 and its own responsibility identity and concrete group.

The existing source fence/export, target import, complete transfer publication
and individual target activation protocol then applies. Publication installs the
parent and all child manifests atomically. Insertion targets bind schema3 and
serve their exact child identity. Imported deduplication/outbox records keep the
parent's source lineage. A later child split uses the existing parent delegation
reservation protocol and freezes under the child's identity.

A compact creation reference is not a bootstrap or quorum certificate. Assigned
node bootstrap, authenticated committed observations, and host placement checks
remain required. Current evidence covers deterministic handoff, checkpoint
recovery and selected native intent WAL faults. Slice131 adds selected assigned
native TCP/QUIC WAL/checkpoint phase recovery, partial provisioning, unread results,
exact retries and individual child activation with ancestor/source offline. Root insertion requires one metadata authority, complete scope movement and fresh
Single child owners. Slice132 adds same-authority nested insertion through an
explicit parent reservation; slice133 adds selected native recovery evidence.
Cross-authority and retained local scope insertion, deletion/reparenting and
authority movement remain planned.

### Nested insertion

Select Directory::with_recursive_insertion() before bootstrap (schema6). Reserve
fresh Staging grandchildren under the existing child's current generation. Use
DelegationPlan::insertion(parent, before, after, children, child_operation) to
reserve the exact parent route and entire mapping. After authenticated committed
reservation observation, child_intent(configuration) produces VBTINT04. Changing
a child manifest or creation ID/index/configuration breaks the parent binding.

Follow the existing fence/import/publication path, then commit DelegationCompletion
to refresh the parent's child epoch locator, and activate each grandchild using the
exact child publication. Dynamic parents may reserve later lifecycles; local
ancestry must be rooted and leave space within MAX_ROUTE_HOPS. Nested targets bind
schema4; source lineage and imported operations remain unchanged. Existing original
decline/cancellation remains available before a successful child intent.

Current nested evidence is deterministic handoff, checkpoint/partial-progress
recovery, exact retries/outbox, parent refresh, dynamic-parent later freeze and
legacy codec/schema checks. Slice133 adds selected Linux TCP/TLS and QUIC WAL/checkpoint
histories with actual assigned grandchildren and a previously activated child as
source. Every named parent reservation/intent/stage/fence/import/publication/parent
refresh phase recovers original facts after unread-result abort/reopen and exact
retry. Before refresh, stale root routing refuses; afterward it resolves exact
grandchild grants. Original grandchild activations, imported retries and new writes
work with metadata and source stopped; their file bytes and recovered metadata logs
stay unchanged. Original source activation lineage and immutable creation bindings
survive. Subsequent nested movement/retirement,
cross-authority/retained-scope insertion, arbitrary faults and macOS/separate-host
execution remain open.

Slice134 checks subsequent grandchild split and compatible merge with actual
retained nested activation lineage, original retries/outbox, parent refresh and
root routing. Owners select RetirementGuard before their first command. Partial
activation and incorrect retention scope refuse cleanup; complete retained
activation evidence plus explicit host retention release permits payload removal
while fence/activation tombstones remain. Selected schema4 source native-file
publication interruption/replay/reclamation is exercised through the same shared
fixture as ordinary retirement. Networked subsequent movement/retirement remains
the next deliverable, with broader lifecycle/platform gates still open.

Slice135 adds selected TCP/TLS and QUIC WAL/checkpoint later split/merge recovery
from an actually assigned inserted grandchild. All20 later phases recover unread original results
and exact retries; dynamic parent locator refresh, partial fencing/activation,
imported outcomes/outbox and final ancestor/source-offline service are checked.
Metadata and stopped source files remain unchanged during merged writes; original
creation bindings survive reopens. Later groups use explicit trusted bootstrap,
not a new creation RPC. Networked retirement and broader lifecycle/platform
evidence remain open.
