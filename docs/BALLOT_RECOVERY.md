# Historical ballot promises

A `GroupLog` pairs `HardState` with `BallotOrigin`: the accepted configuration
ID and exact candidate store identity at the original vote. This is historical
local persistence metadata, never current voting authority. It is not copied
from a remote application snapshot or inferred from the latest electorate.

`apply_batch` checks each genuinely new promise against the predecessor accepted
membership. A learner, absent candidate, or candidate authorized only by the
same atomic update's incoming suffix/snapshot is rejected. An unchanged promise
keeps its original scope. `HardState::follows` still forbids erasing or replacing
a vote in its term. A higher term can clear it and cast a new eligible ballot.
Suffix rollback, later removal and logical compaction preserve the old promise,
even when its origin lies beyond the surviving head or before the snapshot base.

Raft local campaign, proposal, read and vote admission require the exact assigned
local store. Incoming candidate stores already pass envelope identity checks;
a same-term repeat grant additionally requires the original candidate store.
Reusing a NodeId with a replacement store does not inherit that old ballot.
The normal exact `LogTicket` / `DurableLog` dependency still gates vote replies;
Written is insufficient. No additional escaping effect, generation or watermark
is introduced. The origin occupies bounded inline per-group state.

Ordinary format-2 WAL bytes are unchanged: replay derives origin at the original
vote before applying later transitions. Full-image reclamation may discard that
configuration history, so [VBLCPT02](WAL_RECLAMATION.md) persists origin explicitly.
Bootstrap-representable images remain VBLCPT01. Recovery reconstructs canonical
log invariants separately from restoring the already durable historical promise.
It rejects missing/inconsistent origin and bootstrap scope/store mismatches,
including when a host codec returns the map. Host providers remain trusted for
persistence provenance: a constructible non-bootstrap origin is not cryptographic
proof of a discarded history. CRC detects corruption, not a malicious provider.

Public dynamic Raft recovery and ingress remain disabled. These storage and
internal-core changes do not supply learner readiness, lagging-peer request
scopes, a retiring leader's final propagation or online administration.

# Evidence

`tests/ballots.rs` applies shared conformance histories to host and native stores,
retains promises across rollback/removal/compaction, rejects second same-term
votes, cuts each byte of a vote frame, injects synchronization/publication and
replacement failures, restarts actual native files, and rejects every extended
image truncation/single-bit corruption. Resealed semantic corruption and invalid
host-decoder output fail before recovery-session publication. Logical snapshot
compaction tests assume the externally verified snapshot pin; they do not claim
application snapshot publication. Existing snapshot tests cover that separate seam.

The actual-core store-reuse test in `src/raft/membership_tests.rs` uses prepared
committed fixtures and host-asserted completion tokens. It exercises admission
and exact-completion logic, not a networked membership transition.

`tests/ballot_model.rs` is an independent finite local state-machine model. See
[validation scope and results](../validation/REPORT.md). It does not prove joint
Raft leader completeness, quorum intersection across network histories or liveness.
