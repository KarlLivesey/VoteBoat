# Final commitment from a retiring leader

Accepting a final configuration immediately removes or demotes an old leader
from voting and new service admission. It may still finish committing that final
entry under the accepted new policy. Previously its durable completion cleared
leadership before the normal commit broadcast, suppressing the final commitment
announcement. Receivers that had accepted the final entry also rejected ordinary
replication from that now-retired voter.

The core now emits bounded commit-only announcements after the exact admitted
LogTicket appears in DurableLog for final commitment, then clears leadership,
reads, ballots, requests and progress. Admission/written progress cannot release
them. They target the current replication assignments, including learners, and
use the existing Append encoding: no entries, previous index/term equal to the
final record, and leader_commit equal to that same index. The message uses the
accepted final configuration and a fresh existing RequestContext. No new wire
format, generation, watermark or durable token is introduced. The index is one
known contiguous boundary, never a maximum acknowledgement.

## Restricted receipt

Before ordinary voter authorization, the receiver recognizes this narrow case:

- Exact group incarnation, recipient, non-self sender, final configuration,
  positive sequence and context origin equal to the authenticated sender binding.
- Sender is no longer a current voter, but its exact node/store was a voter in
  the preceding joint configuration recovered from the local surviving history.
- The local latest configuration entry is that final record. Its expected joint
  configuration matches, and the joint predecessor is already locally committed.
- The Append has no entries. Previous index and leader_commit equal that exact
  final index; previous term matches the local entry. Message term equals both
  the local current term and the final entry term.

Only that already stored final commitment can advance. The notice grants no
role or voting authority and leaves membership, hard term/vote and election
timers unchanged. Normal local retirement cleanup still applies if the receiver
itself is removed/demoted by the newly committed final. It persists a changed
commit index before releasing Committed/application work or its Appended reply.
An exact duplicate after commitment needs no additional persistence. The reply
does not resurrect a retired leader's replication requests or establish a quorum.

A retired voter cannot append data, raise terms, elect, probe reads, claim another
configuration/boundary or authorize an unrelated learner/store. A receiver in a
newer term or configuration rejects the notice. A verified joint snapshot base
can preserve the predecessor authorization while the final remains in its tail.
After compaction through the final record, the old sender history cannot be
recreated from a newer configuration ID; the already committed receiver rejects
the retired sender. Existing normal replication from active voters is unchanged.

## Scope and evidence

This uses the authenticated non-Byzantine peer model already used by Raft's
leader_commit assertions. The sender's core produces the announcement only from
its durable final commitment; this is not a transferable signed certificate.
The network owner must still authenticate exact node/store/session bindings.

Announcements are one-shot output through existing bounded send ownership.
There is no persistent retirement outbox or retired-leader retry loop. A lost
notice cannot roll back commitment; a valid new leader can subsequently commit
its current-term prefix. This slice does not solve catch-up for a receiver that
lacks the final entry, promoted-leader authorization at older receivers, roster
retention/retirement, or full faulted online reconfiguration.

Three actual-core tests cover removed and demoted leaders, exact durability,
final receipt and duplicates, unchanged authority/timers, eleven malformed or
obsolete cases, and joint/final snapshot compaction boundaries. The compacted
fixture explicitly asserts verified local pins and uses the private verified
recovery helper; it does not establish snapshot publication on disk. Three
downstream host/native tests use public dynamic recovery and receipt APIs,
including failed sync and manifest publication, power loss, recovery and retry.
Imported committed membership in these fixtures is an explicit host premise.

Linux default library/member/membership/Raft/replication-scope/runtime/snapshot
suites pass 29/14/27/22/4/25/19 tests (140). Core-only versions pass
21/8/16/10/1/8, with runtime omitted. TCP/QUIC service, QUIC session/connector and
startup regressions pass 7/9/4/6 tests. The final added newer-term negative
assertion passes its focused check. These finite results are not a full joint
consensus proof, online administration history, macOS run or performance claim.

Configuration-bearing Append and membership Snapshot ingress remain gated.
