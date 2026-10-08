# Promoted replica catch-up authorization

`Event::AuthorizeReplication { witness, candidate, configuration }` asks one
exact current voter store for a direct membership assertion. The candidate must
be a different node, currently outside the receiver's electorate; the requested
head must be newer than the receiver's accepted configuration. One pending
request and one installed permit are retained inline per core. A second request
returns Busy. `CancelReplicationAuthorization` discards both; hosts drive expiry
and retry with a new request context. There is no hidden timer, queue or fanout.

The network provider must authenticate Message.from/sender before delivery, as
for existing Raft traffic. The witness independently reconstructs the receiver's
base configuration from retained committed history and requires the requester to
be its exact assigned replica and itself to be an exact voter there. It describes
only its durable committed membership. The candidate must be an exact voter in
that membership; the requested head must equal that committed head or the final
target reserved by its committed joint configuration. An accepted uncommitted
promotion is insufficient. Missing compacted history is rejected, never inferred
from the request. A retired witness can still describe retained committed history.

AuthorityRequest and AuthorityReply are control messages. The reply echoes the
base scope, exact candidate/store and requested head, plus the committed index
and term. A denied reply carries zero boundaries. The receiver requires its exact
pending context, local store session, witness node/store, unchanged base head and
matching request fields. A granted committed boundary must be later than the
base's last configuration entry. A valid reply consumes the pending request exactly once.
It grants no term, ballot, role, commit or election-timer change. Positive envelope
terms keep wire shape valid even for a term-zero requester; control handling does
not treat those terms as leader evidence.

A granted permit admits only Append/Snapshot requests from the exact candidate
store with the exact requested head while the base configuration remains current.
Normal log-prefix, term, committed-prefix, snapshot and exact storage-completion
checks still apply. An actual authorized leader contact can reset an election
timer. Votes, read probes, read acknowledgements and replication responses gain
no authority from the permit. Partial matching-prefix repair and higher-term
persistence retain the same-base permit. Durable configuration change, rollback
to a different head, cancellation, storage fencing and recovery discard it.
Recovery must use a fresh persisted StoreSession; a restarted replica must query
again. Connection generations and transport revocation remain separate contracts.

The grant is an authenticated non-Byzantine peer assertion about its own durable
state, using the same trust premise as leader_commit. It is neither a quorum
certificate nor a transferable signature. Its new output needs no new storage
write: the witness's exact earlier DurableLog completion/recovered durable state
is the prerequisite. An in-flight dependency blocks queries. The returned index
is that source's contiguous committed prefix, not receiver progress or a maximum
observed network index. The volatile request sequence uses the existing checked
RequestContext counter and persisted local store session to reject old replies.

## Wire and integration

`NativeWireCodec::with_authority` explicitly selects format 3. It includes format
2 membership payloads and adds tags 10/11 for request/reply (tag 9 remains the
explicit membership snapshot). Formats 1/2 reject authority traffic. Decoding
checks identities, scope, boundary, booleans, lengths and retained-memory budgets;
a checksum is integrity detection, not authentication.

Native TLS/QUIC startup defaults to wire format 1. Select format 3 explicitly
with `NativeTlsConfig::with_wire_version(3)` on every peer. Native sessions require
an exact encrypted hello match, and startup selects its codec and roster from the
same config. Existing connections retain their selected version. Default/static
cores retain their configuration-bearing Append and membership Snapshot gate.
Explicit member assemblies select validated receive-side configuration
replication. The owning-node controls below exercise real promoted-leader catch-up
through that path. Public service mutation endpoints remain gated pending full
enrollment/admin integration and faulted lifecycle release.

If every old witness is unavailable or has compacted the required old view, this
exchange cannot authorize catch-up. Retaining historical evidence, authenticated
route/roster admission, readiness capabilities, faulted full activation/retirement
histories and administrative integration remain required.
No fallback accepts a candidate's self-reported configuration as authority.
When the witness cannot reconstruct the requester's historical membership base,
it refuses the request with `WrongIdentity` and emits no authority reply. It
cannot authenticate a negative reply from that missing base either. A host may
still observe Pending and must drive cancellation/retry explicitly; silence is
never a grant.

## Evidence

Eight actual-core tests cover committed/uncommitted promotion, reserved final
heads, direct promoted joint receipt and exact durability, identity/context/store
session mismatches, duplicate/canceled/stale replies, restart, partial progress,
read/vote exclusion, higher-term persistence, fencing, retired witnesses and
retained/compacted-away historical bases. Verified snapshot pins in these core
fixtures are host assertions, not native snapshot publication evidence.

Three downstream tests cover public host/native query/grant/probing and loss of
volatile authority on fencing/recovery, including a term-zero request. A native
codec test covers all authority variants, old-format rejection, round trips,
invalid booleans/boundaries/identities, every truncation, batch limits and existing
membership snapshots. Two further native-storage histories run the direct
witness exchange through actual TCP/TLS and QUIC format-3 framed sessions with
exact outbound credit completion. They exercise public core/storage/transport
composition, not an online membership service. These finite checks do not prove
arbitrary distributed membership schedules, macOS execution or performance.

## Owning Node controls and local status

`NodeControl::AuthorizeReplication { witness, candidate, configuration }` queues
the existing core event through bounded owner admission. Node rejects absent
networking or peer roster formats below 3 before admission. The core checks the
current base/witness again when executing; the selected authenticated transport
and existing route/connection reservations still validate every reply. Admission
is not a grant. Candidate hints alone cannot confer authority.

`NodeControl::CancelReplicationAuthorization` queues cancellation of both pending
request and installed permit. Cancellation takes effect when the owner executes
it, rather than immediately at host submission. Closed nodes reject both controls.
No hidden deadline, retries, discovery or witness selection is introduced: hosts
drive bounded timeouts, cancellation and a new request context.

`Raft::replication_authorization_status` returns local volatile None, Pending or
Granted with exact identities, base/head and the pending request context. It is
an observation of existing inline state, not a portable credential or durable
operation outcome. Pending takes precedence if a new query coexists with a prior
permit; it does not mean that prior permit was revoked. Cancel first if revocation
is intended. Configuration change, fencing and restart discard authority as before.

Four owning native-node histories cover retained and compacted promoted-leader
sources over TCP/TLS and QUIC. An old-view voter cancels a query before its reply,
refuses the late grant, then queries again with a distinct context. The fresh
grant changes no durable log/term/commit or election reset. A promoted final-view
leader then transfers the missing joint/final records or a pinned final snapshot,
replicates a client write, and the receiver recovers its demoted learner role.
All three native file stores reopen with the write and no volatile permit. A
separate old-view voter retains the historical base needed to witness the compacted
leader; if every witness loses it, the existing fail-closed limit still applies.
Initial membership records are seeded, not distributed enrollment/proposal evidence.

Four additional owning native TCP/QUIC histories acknowledge a write on the
promoted leader, stop it, and campaign the old-view replica with both leader and
witness unavailable. It cannot accept a client write or invent catch-up authority.
Restoring a retained-history witness grants a fresh-context query; reopening the
compacted promoted leader preserves its acknowledged write and permits snapshot
catch-up. With a compacted witness, restored ingress is refused and no grant is
installed; the host cancels explicitly. The old view stays unchanged while the
current weighted group resumes and commits another write. All actual file stores
reopen with their respective membership/data and no volatile permit. Ballots may
legitimately advance during election retries; the old-view data/configuration
prefix, snapshot and commitment cannot. Initial memberships are seeded, and this
finite weighted schedule does not replace broader recursive partial-delivery
release checks or establish arbitrary availability after losing witness history.
