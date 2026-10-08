# Durable configuration journal

`EntryPayload::Configuration` now stores membership intent and accepted-log
activation state in the same atomic log transitions as entries, hard state and
commit boundaries. This is groundwork for P3. The live Raft core
deliberately refuses these entries until every quorum use, snapshot path, learner
catch-up gate and service-removal rule is integrated. The explicitly selected
[native wire format 2](WIRE_FORMAT.md) carries them; default format 1 refuses them. There is no online
administration API yet. The full P0–P7 objective remains unchanged.

`membership::Configuration` validates a recursive policy, its exact voter/store
map, and a separate learner/store map. Voters and learners are disjoint; each
configuration and the combined joint replication set are bounded to 4,096 nodes.
Policies retain the existing depth/node limits. Retained tree capacities and
B-tree metadata count toward payload, fetch, effect and ingress/outbound budgets.
Configuration-bearing Append input is data traffic, preserving control reserves.
The current payload limit also bounds configuration records; large configurations
need an explicitly larger compatible limit.

The journal grammar is:

1. `Learners`: change non-voting assignments while retaining the exact voter
   policy and voter/store map. The new configuration ID exceeds the current ID.
2. `Joint`: record an operation ID, expected current configuration, a new joint
   ID, and a complete final target configuration. The IDs increase in that order.
   Every newly promoted voter must already be an exact-store learner in the
   preceding committed configuration. A retained voter cannot change stores.
   The effective predicate immediately becomes `old && new`, before commitment.
3. `Final`: match the joint operation, joint ID and reserved final ID. The joint
   record must be within the committed contiguous prefix. Receipt of the final
   record activates the new predicate, even while that final record is uncommitted.

Another learner/joint transition waits for the preceding configuration record's
commitment. A joint transition remains exclusive until its matching final.
Operation reuse is rejected across the surviving journal. Suffix replacement
replays from the snapshot base (or bootstrap) to reconstruct effective configuration;
replacing an uncommitted final restores joint rules, and replacing an uncommitted
joint restores the previous stable configuration. Committed entries remain
protected by the existing replacement guard. Strictly increasing configuration
IDs are an implementation decision for fencing request contexts; operation IDs
provide lifecycle identity separately.

`GroupLog::membership` is deterministic derived state, **not a durability or
commitment receipt**. Its `is_satisfied` and `frontier` require already scoped,
authenticated ballots or matching durable prefixes. The latter returns a
contiguous matching prefix satisfying both policies. Leadership, current-term
commit rules, local durability and learner readiness remain core obligations.
The journal validates assignment order, not network catch-up or application
compatibility. A future core must gather that evidence before proposing promotion.

There are no new escaping effects, storage tokens or generation domains here.
Existing `LogTicket`/`DurableLog` dependencies guard durable state exposure;
Written never becomes Durable by reading journal state. Existing suffix-generation
fences invalidate old tickets and fetches. Recovery replays complete checked WAL
frames through the same mandatory logical validator used by host stores.

The native format-2 WAL adds entry tag 2, containing configuration-journal
subformat version 1, operation/expected IDs and a learner/joint/final record.
Configuration content includes the validated policy and both store maps. Existing
format-2 logs remain readable. Older binaries reject the new entry tag; downgrade
requires explicit migration. Unknown versions, invalid trees/IDs/maps, duplicate
nodes, oversized counts and incomplete records fail closed. Full-image physical
reclamation retains the snapshot configuration base and surviving journal.

Snapshots now carry the complete stable/joint configuration base and operation
identities, allowing matching-prefix compaction without losing membership.
See [configuration-aware snapshots](CONFIGURATION_SNAPSHOTS.md) for validation,
format compatibility and crash evidence. A hard limit of 16,384 configuration
operations prevents unbounded identity retention; exhaustion refuses further work
without evicting identities. The bootstrap ballot guard remains static pending
live-core integration. Storage acceptance does not authorize starting a dynamically
configured replica: core recovery still refuses such state.

Tests in `tests/membership.rs` use independent host storage and the native WAL.
They cover stage ordering, identity/store mismatch, same-voter recursive weighted
policy changes, exhaustive joint frontiers for 3,125 prefix assignments, rollback,
atomic multi-group rejection, Written/Durable separation, bounded ingress/ranges,
snapshot refusal and live-core/default-wire refusal. Native tests cut every byte of all
four transition/replacement frames, inject failed sync/manifest publication, and
reclaim/reopen joint and final histories. Native codec tests check every truncated
record, unknown tags, zero IDs, count limits, duplicates and overlapping maps.
These are finite storage/journal checks; they do not establish a complete joint
Raft protocol, networked membership history, power-failure certification or macOS
execution. The formal membership model with volatile/durable ballots and all live
quorum sites remains a gate for enabling online changes.
