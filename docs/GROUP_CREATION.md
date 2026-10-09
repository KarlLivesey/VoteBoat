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

This slice does not create a store, start an election, publish a namespace route,
activate a responsibility or import data. The next step binds assigned-node
bootstrap to a verified exact committed intent and persists group identity and
configuration before campaign. Imported groups must remain non-serving until the
existing import/publication/activation protocol authorizes them. A creation intent
cannot replace an unreachable owner or discard its data.

Tests in `tests/directory/creation.rs` cover truncation/canonical decoding, bounded
policy/store counts, exact bindings, authority/generation/identity refusals, parent
transfer locks, operation retries/conflicts, pending budget, atomic batch failure,
checkpoint replay and schema refusal. Native directory tests use three Raft cores
with actual WAL/checkpoint files, lose an observation, compact, reopen and catch up
a lagging replica. Separate modeled native journal faults cover every appended
frame byte and sync/publication boundaries. They do not prove arbitrary crash
schedules, network creation, native assigned-node startup or namespace activation.
