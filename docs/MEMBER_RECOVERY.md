# Dynamic member restart and verified import

`Raft::recover_member` explicitly restores a voter or learner from host-supplied
durable state. Both the committed prefix and latest accepted membership must
assign the exact local NodeId and StoreIdentity. An uncommitted initial assignment,
accepted removal, foreign/replaced store or invalid journal refuses recovery.
The host authorizes the restart/import and supplies state from its selected
LogStore. Constructible Rust state and a claimed commit index are not remote
commitment certificates; network traffic cannot invoke this entry point.

The accepted log determines the recovered predicate, following the design's
log-activation rule. A joint record uses both old and new policies even before
commit. A final record uses the new policy even before final commitment; its
preceding joint record must already be committed. Truncating an uncommitted final
restores joint rules. Stable, weighted and recursive validated policies retain
the same meaning after restart. A member committed as a learner but promoted in
its accepted joint suffix reconstructs that voter role. A learner remaining
outside both voting sets cannot campaign, vote, lead or establish read authority.

Recovery starts as a follower and reconstructs no volatile leader, quorum vote,
replication acknowledgement or read authority. Historical BallotOrigin and
HardState promises survive configuration change/rollback; a same-term request
cannot replace the old candidate/store promise. A new campaign still requires
its exact self-vote LogTicket in DurableLog before election requests escape.
The subsequent leader no-op has its separate durability dependency.

## Application data and snapshots

The raw method refuses a compacted log. Use `snapshot::recover_member_replica`
with the selected LogStore, SnapshotRetention and fresh application. It checks
the local committed/accepted assignment, verifies the exact pinned snapshot
identity/bootstrap/membership/index/term/schema, restores application data and
replays the committed tail before returning the core. Missing referenced data
prevents recovery. Application deduplication survives checkpoint/reclaim/restart.
As with existing recovery helpers, a later provider failure can leave verified
local restoration/reconciliation work performed; the returned error exposes no
live core and does not promise rollback of provider state.

`Raft::recover` / `snapshot::recover_replica` keep their static-voter contract.
The original learner helpers keep their bootstrap-electorate assignment contract.
NativeStartup and the counter executable retain static construction. Hosts opt
into dynamic recovery and compose the recovered core through public runtime/Node
parts with authorized routes and the matching restored application. This slice
adds no WAL/wire record, generation, effect or durability token.

## Evidence and remaining gates

Eleven downstream tests in `tests/member_recovery.rs` exercise host/native joint
and uncommitted-final elections, weighted final recovery, exact committed/accepted
assignments, replacement stores, learner exclusion, historical ballots, final
rollback, verified snapshots and missing data. A native file history closes and
reopens the log and snapshot stores after compaction/reclaim, then checks dynamic
membership and a duplicate application operation. Native model-I/O histories cut
every byte of a joint frame and inject synchronization/manifest publication
failures; recovery selects a complete old or new membership. These fixtures
explicitly assume authorized committed imports and simulate vote replies; they
do not establish a networked administration operation or transferable election
proof.

Recovered prepared members now accept an exact
[retired voter's final commitment](RETIRING_LEADERS.md) after their own barrier,
without restoring the sender's leadership or changing local voting authority.
Promoted-leader authorization at lagging receivers, readiness/capability evidence,
faulted retirement propagation, route/roster admission, distributed activation modeling
and faulted network membership histories remain prerequisites for online changes.
Default cores retain the configuration-bearing Append and membership Snapshot
gate. `Raft::with_configuration_replication` explicitly selects ordinary validated
receive-side replication for member assemblies; NativeMemberStartup selects it
after exact member/store, codec and route admission. This grants no administrative
authority, sender identity or promotion readiness. Public service mutation
endpoints and the complete remote lifecycle release remain gated.
An attached configuration or newer scope alone cannot self-authorize a learner.
Linux checks do not establish macOS execution or performance.

## Retained promotion repair after final configuration

With batched joint repair selected (native wire5, also selected by wire6 snapshot
repair), a stable candidate can recover an exact lagging promoted learner using
a retained committed historical joint. The candidate must be an exact old-view
voter and a current voter; the peer must be the exact old learner promoted by that
joint and still assigned as a current voter. The candidate sends bounded batches
only through that joint, scoped to its historical configuration. Receiver checks
remain the ordinary strict pre-election learner checks; persistence precedes its
reply. No repair reply is a ballot, quorum acknowledgement or commitment receipt.
Only a subsequent ordinary Vote/Voted under the candidate's current configuration
can complete election. A matching historical response observing a higher term
steps down and persists that term without importing the response's configuration.

The volatile request context is campaign/session scoped. Lost observation and
restart rebuild the transfer from durable history; matching overlap is preserved.
Wrong identities and stale contexts cannot supply progress. A newly promoted
candidate that was not an old-view voter cannot use this path to self-authorize.
If compaction removed the historical promotion, this retained-log path sends no
repair; ordinary elections remain available. Recovery of that lagging learner
from a historical checkpoint after final is still an explicit release gate.

### Wire7 committed stable checkpoint mode

The retained-log limitation above remains true for formats5/6. The separately
selected wire7 mode can restore a committed stable final checkpoint from an
already trusted old-view voter after the promotion log was compacted away. See
[the exact source/receiver contract](MEMBERSHIP_CORE.md#committed-stable-checkpoint-learner-recovery-native-format-7).
NativeMemberStartup selects the matching core/codec for wire7; the counter
executable's explicit member/import modes now select7 on all peers. Existing
library assemblies selecting6 retain their old behavior. Disk formats are unchanged.
Host-provider restore/lost-completion, ordinary ballot, protocol/refusal and native
WAL fault evidence is recorded in slice118. Existing interrupted TCP/QUIC service
histories pass under7, but do not force this new checkpoint path; that native
history and the accepted-final/older-joint checkpoint bridge remain open.

Slice119 adds the accepted-final/committed-joint bridge using the image's own
historical joint scope, without exporting or certifying the uncommitted final.
Four forced native TCP/QUIC histories now cover joint and final checkpoint
boundaries, abort observation before the candidate processes a repair reply,
reopen both native stores, then elect/apply and preserve snapshot-carried retries.
See tests/native_member_startup/checkpoint_final.rs. Older mismatching stable
checkpoints, arbitrary fault schedules and the broader P4 release remain open.
