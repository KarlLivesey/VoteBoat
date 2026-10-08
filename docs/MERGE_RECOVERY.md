# Compatible merge and recovery

A top-level merge changes one responsibility from `ExecutionMode::Partitioned`
to a new concrete `Single` group through the existing checked transfer intent.
The sources cover disjoint complete scopes with the same responsibility identity,
application adapter and partition scheme. The new ownership epoch and route
generation advance once. Old and new concrete groups are disjoint; the metadata
authority remains the same. This is application authority movement, separate
from replica membership changes within a group.

The public [scope](SCOPE_APPLICATION.md), [source fence](SOURCE_FENCING.md),
[target import](TARGET_IMPORTS.md), [publication](TRANSFER_PUBLICATION.md) and
[activation](TARGET_ACTIVATION.md) contracts already support the multi-source
shape. There is no additional consensus engine, atomic cross-group transaction,
worker, store, persistent format or hidden coordinator in this composition.

## Trusted host sequence

Commit the exact merge intent and stage the target first. Each source then commits
its own irreversible freeze and exports its final scope through its own F. Source
indices are independent; do not substitute a maximum or treat them as the target's
applied prefix. Preserve every source's original operation results and outbox.
The target's complete ordered inline import carries actual images, source fences,
configuration identities and content commitments. Its own I is a target log index.

The authenticated trusted host obtains quorum-readable source statuses and target
readiness. `TransferPublication` requires every source and target intersection
with matching fence/configuration/scope/digest. After directory publication, the
target verifies that decision against its retained original import and commits
local activation. A hash without durably imported data, a route hint or a cached
leader is insufficient. Foreign provenance/configuration remains host-verified;
serializable evidence is not a cryptographic certificate.

Once publication is committed, activation needs the retained decision and local
import, so both old source groups can be offline. Source availability must not be
an extra activation dependency; their durable fences remain part of the decision.

One source can be frozen while another still serves its old scope. If the remaining
source is unavailable, pause rather than treating its fence as absent or inventing
an empty source. The frozen source stays frozen. On recovery, the unfrozen source
can still accept old-epoch writes before its own fence; the final export must include
them. Resume from fresh quorum-observed state, never from a timeout or remembered
phase flag. After all sources are fenced, finish forward through import/publication/
activation. There is no automatic unfreeze or partial-source publication.

`BucketCounter` deterministically combines disjoint images, including original
semantic requests/results and pending outbox items. Repeated original operations
return original results even if subsequent source/target writes changed the value.
Conflicting original IDs across source histories refuse the whole import atomically.
The target stays inactive and cannot produce readiness. Do not silently rename IDs,
drop one history or unfreeze sources to make that merge pass. Hosts must select
compatible provider capacities before fencing; insufficient combined capacity also
refuses safely. The failure may require explicit recovery outside this first normal
handoff, not an invented successful owner.

After local activation, ordinary target writes and quorum reads need no metadata or
source access. Source fences and original target import/publication/activation
identities remain retained through WAL/checkpoint recovery. External outbox delivery
and reclamation are separate work; these histories verify retained pending items.

## Selected evidence

Three downstream deterministic tests in `tests/transfer_merge.rs` combine actual
source images through activation and checkpoint recovery, preserve original data
results/outbox, refuse duplicate source IDs without target mutation, and reject
incomplete source coverage/publication. They use public application contracts and
do not claim foreign quorum provenance on their own.

Six native histories in `tests/routed/merge.rs` use actual three-replica metadata,
two independently bootstrapped source groups and one target over TCP/TLS or QUIC.
Four complete variants close/join/reopen all groups after intent, staging, each
source fence, import, publication and activation, selecting WAL or checkpoints.
After the first fence, all replicas of the other source stop. The trusted test host
records Unavailable separately from a quorum-observed absent fence and makes no
phase progress. Reopening that source permits another old-epoch write before its
fence; the original first-source export remains unchanged.

Before activation, both source groups stop. The target commits activation using
the retained publication/import, serves while they remain offline, and preserves
that activation when all groups reopen with both original source fences intact.

The completed merge serves both key ranges with metadata/sources stopped, returns
original results for three imported operations, applies new writes, and recovers
again with retained phase identities, values and outbox. Both old sources reopen
fenced and old target route contexts refuse. Two additional native WAL histories
exercise conflicting source IDs: import admission refuses without target mutation,
publication remains absent, and both sources stay fenced with an inactive target
after reopen. No error is converted to an empty or partial successful import.

These are finite graceful committed-boundary recovery histories under the
ordinary three-voter configuration. Stopping all source replicas models a known
offline group; it is not a packet-level quorum-partition or timeout experiment.
They do not cut power, roll back uncommitted phase tails or prove arbitrary-fault
liveness. The independently bootstrapped sources are not prior activated split
targets, so merge-after-split/repeated movement remains unimplemented. Recursive
parent coordination, retirement, autonomous resumption/operator endpoints,
macOS and separate-host validation remain work. Earlier fault tests are separate
evidence, not automatically a proof of this composition.

Run the selected checks with:

```sh
cargo +stable test --locked --offline --all-features --test transfer_merge
cargo +stable test --locked --offline --all-features --test routed merge_ -- --nocapture
```

Independent native histories serialize worker/socket topologies inside the test
binary; replicas/groups remain concurrent within each history. Next make activated
targets reusable as safe sources and retire transferred data with durable tombstones,
then coordinate delegated-parent ownership changes and measure P7 performance.
Full P0–P7 remains active; P8 is deferred and CI remains background feedback.
