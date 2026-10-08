# Request-scoped replication across accepted configurations

The configuration ID in a message identifies the requester's accepted head.
Replies echo that ID and the original request context; they do not advertise the
responder's current membership. These are distinct from the configuration at an
application snapshot's included prefix. No wire bytes or persistent format change.

An Append or Snapshot request can bridge differing accepted configuration IDs
when its sender is an exact voter/store in the receiver's current membership.
The same group incarnation, recipient, non-self sender, request-origin binding,
positive term/sequence and existing log/snapshot validation remain mandatory.
A learner cannot originate replication by declaring a newer configuration ID.
An incoming configuration entry cannot activate beyond the declared sender head;
a snapshot base cannot exceed it either. The target ID reserved in a joint entry
may exceed its joint ID because that target is not activated by the joint entry.

This supports catch-up from a retained voter leader, including failure probes,
bounded chunks that stop before the sender's configuration entry, and replacing
an uncommitted final record with a higher-term suffix from a joint leader.
Receiving a scope alone does not activate it or grant election/read authority.
Membership still derives from the surviving accepted log and verified snapshot
base. The contiguous matching prefix in a reply is independently checked;
a newer echoed scope cannot turn a shorter chunk into a longer matching prefix.

Outstanding leader replication requests pin configuration, context and range.
Heartbeat retries keep that scope. A response must pass the current configuration
check and match the admitted request, term, exact store and local-origin session.
Configuration changes clear old requests and matching progress before recollection.
Relabeling a delayed response's configuration cannot reuse its context. Append
and snapshot success prove only the exact sent end; a compacted hint must match
the leader's checked log boundary. Learner progress remains absent from quorum
predicates. Elections, ReadProbe and ReadAck still require equal configurations.

A reply after a changed term, suffix or commit prefix waits for the exact admitted
LogTicket in DurableLog. Written never releases it. A staged snapshot additionally
waits for verified local snapshot persistence, durable log installation and the
application-install completion. Its saved reply retains the original request's
head scope throughout. Snapshot data loading on the leader requires the original
outstanding request; a configuration change invalidates that deferred send.
No new escaping effect, durability token, generation or watermark is introduced.
The pinned configuration is bounded inline per-peer state; output fanout is unchanged.

## Evidence and remaining work

Seven actual-core tests in `src/raft/membership_tests.rs` exercise lagging probes,
joint receipt, final rollback, partial chunks, snapshot/application dependencies,
compacted hints, stale scopes/contexts and learner/read/election exclusion.
Prepared committed fixtures and host-asserted completion tokens isolate these
rules. They are not networked online-administration or physical-storage proof.
The public gate still rejects configuration-bearing appends and membership
snapshots; the tests call the actual private receive path behind that gate.

Four downstream tests in `tests/replication_scope.rs` use the public core with
host/native storage: differing scopes on static log replication, missing exact
completion, failure-probe replies, rejected read/election/foreign-store traffic,
failed sync/manifest publication followed by power-loss recovery, and native
format-2 round trips preserving request, response and snapshot-base scopes.
A timed-runtime test checks that valid retained-voter replication contact resets
the election deadline without activating the echoed scope or changing its vote.
These complement existing static TCP/TLS histories, which do not exercise online
membership.

An exact voter from the preceding joint configuration can now send a restricted
commit-only notification for the receiver's already stored final record, even
after removal/demotion. It cannot append entries, raise terms or reset election
timers. See [retiring leader commitment](RETIRING_LEADERS.md); this is separate
from ongoing replication authority.

A newly promoted leader that is only a learner or absent in an older receiver's
current view remains rejected. That path needs validated catch-up authorization;
blindly trusting the declared head would let a learner self-authorize. Initial learner-only
assignment recovery is available through an explicit host entry point; see
[learner recovery](LEARNER_RECOVERY.md). Explicit
[dynamic member recovery](MEMBER_RECOVERY.md) and prospective fanout reservation
are now available. Readiness evidence, a retiring leader's
final propagation, route/roster admission, distributed activation modeling and
faulted actual network histories remain unfinished prerequisites to releasing
online configuration changes. Linux evidence does not establish macOS execution,
hardware power-failure behavior, liveness or performance.
