# Validation report — slice 35

The independent Rust model in `tests/ballot_model.rs` explores the complete
reachable closure within explicit finite bounds: seven configuration phases,
four candidate/store identities (including one reused NodeId), two local replica
identities, terms 0–2, and at most one restart. No search-depth cutoff is used;
a three-million-state resource cap fails the test if reached.

Actions separate accepted state, write completion, synchronization, exact
completion, durable crash recovery and recovery of a complete unsynchronized
tail. They include learner staging, two joint/final transitions, store replacement,
uncommitted rollback, committed-prefix compaction and higher-term observation.
Commit advancement is an external quorum oracle. Stale completion is ignored;
restart does not produce a vote reply.

The correct model explores **237,556 states and 665,097 edges** with no invariant
violation. It reaches all seven phases, historical origins ahead of a rolled-back
head and behind a compacted base, and a replacement-store ballot. Invariants check
prefix/term bounds, original candidate eligibility, retained per-term durable
promises, receipt durability/incarnation, and quiescent core/disk agreement.

Three negative controls produce counterexamples:

| Deliberate defect | States to counterexample | Observed violation |
| --- | ---: | --- |
| Erase vote when rollback removes candidate | 5,835 | Durable promise erased in its term after joint rollback |
| Reply at Write | 44 | Reply precedes recoverable promise |
| Compare candidate NodeId without store identity | 75,905 | Replacement store changes a durable same-term promise |

Run `cargo +stable test --locked --offline --no-default-features --test ballot_model -- --nocapture`
for exploration counts and exact counterexample traces. The implementation's host,
native, actual-file and actual-core tests are separate evidence; the model does
not import production helpers or establish their refinement automatically.

This finite local model is not a full distributed membership proof. Network
partitions, log matching/leader completeness, distributed quorum commitment,
unbounded terms/restarts, liveness and resource fairness remain outside it.
Online reconfiguration stays gated pending its distributed model and faulted
actual network histories. Linux local execution does not establish macOS or
hardware power-failure behavior. CI is background feedback, never a required
merge gate or a reason to stop local implementation.

## Slice 36 — actual-core replication scope evidence

Seven additional internal tests drive the real receive, persist, completion and
snapshot-install paths behind the public configuration-data gate. They exercise
retained-voter catch-up, matching-prefix bounds, joint/final rollback, request
scope independent of snapshot base, compacted hints, exact application dependencies,
and stale-context/learner/read/election rejection. Four public downstream tests
exercise host/native static-log scope bridges, failed barriers/recovery and native
wire format 2. See [scope rules and limitations](../docs/REPLICATION_SCOPES.md).

The independent local ballot model above is unchanged. These finite core tests
are not a distributed activation model, a refinement proof or online network
membership evidence. Newly promoted sender authorization and the other listed
release gates remain incomplete. CI remains background feedback.

## Slice 37 — learner enrollment/recovery evidence

Eight downstream learner tests use public contracts with explicit host-authorized
assignment import. Host/native histories check exact committed and accepted local
store assignment, service/vote/read exclusion, completion ordering, snapshot/data
verification, application replay and refusal of joint/policy recovery. Native
histories cut every assignment-frame byte and inject failed barriers. An actual
log/snapshot-file history closes and reopens a compacted, physically reclaimed
learner and verifies its exact state. One timed-runtime test checks durable
replication with no learner election timer. See [learner recovery](../docs/LEARNER_RECOVERY.md).

The assignment import is a trusted host action, not proof of an online remote
commit. Promoted-sender authorization, learner readiness, dynamic recovery and
the distributed model/network membership gates remain unfinished. The local
ballot model above is unchanged; no full proof or macOS/hardware claim is made.

## Slice 41 reservation evidence

The final default-feature library/runtime/effect-owner run passes 26/25/94 tests;
the executable service/routing and downstream startup suites pass 6/5 tests.
These are Linux checks, including actual loopback TLS/worker histories. Four new
core fixtures cover prospective append/snapshot and intermediate joint fanout,
rollback through learner/final shrink, committed shrink, compacted membership,
checked overflow, pure inspection and continued rejection of online configuration
ingress. Two downstream tests cover exact non-consuming queue selection and
oversized-input rejection without queue/ticket/core/timer mutation, plus continued
control service and admission under held output reservations. The core-only run
passes 18/18/90 tests; its run preceded the final held-lease assertion checked in
the default suite. This slice does not change the finite ballot model or establish
distributed membership activation, macOS compatibility or performance evidence.

## Slice 42 dynamic recovery evidence

Eleven new downstream member recovery tests exercise host/native joint versus
final elections, weighted final policy, exact committed/accepted assignments,
replacement stores, learner exclusion, historical ballots, final rollback,
verified snapshots and missing data. Native model histories cut every byte of a
joint frame and inject synchronization/manifest failures; the recovered role
matches a complete old or new membership. A real native-file history compacts,
reclaims, closes/reopens both providers and checks dynamic membership, application
state and duplicate operation behavior. Imported committed assignments and
simulated election replies are explicit fixture premises, not network certificates.

The default-feature library/ballot/learner/member/membership/Raft/snapshot suites
pass 26/10/8/11/27/22/19 tests; core-only versions pass 18/3/4/7/16/10/8.
The final service/startup run passes 6/5 tests after fixture listeners are retained
for fake peers and all placeholder child listeners are released before launching
the first child. Earlier fixture bind failures are not counted as passing runs.
No persistent/wire format or finite ballot model changed. These Linux checks do
not establish promoted-leader network authorization, online membership,
distributed activation proof, macOS execution or performance.

## Slice 43 QUIC evidence

The optional QUIC backend passes nine Linux loopback tests using actual
quinn-proto/Rustls encrypted UDP. They exercise partial ordered plaintext,
authentication pins/names/exact store assignment, revocation, clocks/deadlines,
foreign datagrams, handshake byte limits, one-call poll fairness, clean close
with unread data, timeout with unacknowledged closing output, framed outbound
credit lifetime and retransmission after deliberate UDP loss. A three-replica
history elects a leader, commits its no-op and a counter command, and applies
the same value everywhere using host log tickets and native QUIC framing.

The final combined run passes QUIC/secure/transport/startup/counter-service suites
9/12/13/5/6 tests. All-feature all-target Clippy passes with warnings denied.
Earlier hello-FIN flow-control and closing-peer-loss failures were repaired and
their focused checks plus this final run passed. These are finite checks, not
formal protocol, native-file QUIC crash, macOS, remote deployment or performance
evidence. At the end of that slice the shared listener/connector and counter CLI
remained TCP/TLS; slice 44 adds QUIC establishment and service selection.

## Slice 44 QUIC service evidence

The final Linux run passes library/connect/service/QUIC/QUIC-connector/secure/
startup/transport suites 27/12/7/9/4/12/6/13 tests (90 total). Four new downstream
connector tests exercise simultaneous authorized peers through one UDP socket,
small-budget fairness, terminal slot retention, cancellation, expiry, stale/wrong
requests, fresh-generation reconnect after lease release, constructor socket
return and transferred sessions surviving connector close/drop. An internal UDP
test checks bounded packet queues, drop behavior, charged routed/unknown packets
and retired queue disposal. Late failed QUIC startup releases UDP and joins its
two storage workers before reopening the WAL.

The actual three-process QUIC history uses native WAL/snapshot files and checks
write/read/dedup, abrupt leader loss, replacement writes, recovered former leader
catch-up, checkpoint/drain/join, restart and further read/retry. It supplements
the earlier host-log session history. Core-only/native-only all-target compilation
and all-feature all-target Clippy/API docs pass. Default TCP service/startup
regressions pass 7/5 tests, including unavailable QUIC selection before store
creation. The QUIC process history also passes separately after switching to
non-default addresses through a peer file together with --transport quic.
These finite tests do not establish formal
protocol proof, arbitrary fault coverage, macOS execution, remote deployment,
production readiness or performance.

## Slice 45 retiring leader evidence

Three actual-core tests cover durable final announcements from removed/demoted
leaders, exact completion ordering, prepared receiver commitment, duplicates,
unchanged authority/timers, eleven invalid/obsolete messages and joint/final
compaction boundaries. Compacted fixture pins are host assertions, not native
snapshot publication proof. Three downstream host/native tests use public
dynamic recovery and commit receipt, including failed synchronization/manifest
publication, power loss, recovery and retry. Fixture committed membership is an
explicit authorized import premise; it is not an online admin or election proof.

Default library/member/membership/Raft/replication-scope/runtime/snapshot suites
pass 29/14/27/22/4/25/19 tests; core-only versions pass 21/8/16/10/1/8, omitting
runtime. A focused run passes the final added newer-term negative assertion.
TCP/QUIC service, QUIC session/connector and startup suites pass 7/9/4/6 tests.
All-feature Clippy, native-only all-target compilation and all-feature API docs
pass. Notifications are one-shot; faulted delivery, missing configuration catch-up,
promoted-leader authorization and full distributed activation remain open gates.
These finite Linux checks establish no macOS execution or performance claim.

## Slice 46 direct witness authorization evidence

Eight actual-core tests cover direct promoted-replica joint catch-up, exact
storage completion, committed versus uncommitted promotion, reserved final head,
identity/context/session mismatch, canceled/stale/duplicate replies, restart,
partial progress, higher-term persistence, read/vote exclusion, fencing, retired
witnesses and compaction boundaries. Three public host/native tests cover query,
grant, prefix probing and volatile authority loss, including term-zero queries.
One codec test checks format 3 variants, explicit old-version rejection, bounded
batches, malformed identities/boundaries/booleans, every truncation and existing
membership snapshot layouts. These tests use authenticated-identity premises;
they do not constitute a new end-to-end wire-3 TLS/QUIC membership history.

Default library/member/membership/Raft/scope/runtime/snapshot/wire suites pass
37/17/27/22/4/25/19/13 tests (164 total). Core-only equivalents without runtime
pass 29/10/16/10/1/8/1 tests (75 total). Actual TCP/QUIC service, QUIC session,
QUIC connector and startup regressions pass 7/9/4/6 tests. All-feature all-target
Clippy and API docs pass. The scheduler byte-bound fixture derives exact enum
size and still proves two admitted data events plus one reserved control event.

Witness output relies only on earlier completed/recovered durable committed
membership. It is a direct authenticated non-Byzantine assertion, not a quorum
certificate. Public configuration ingress, native wire-3 session negotiation,
readiness/roster integration and faulted complete activation remain unfinished.
Finite Linux checks establish no macOS execution, formal completeness or
performance claim.

Additional connect/effect-owner/outbound/peers/secure/transport/worker regressions
pass 12/94/4/13/12/13/9 tests (157 total), including actual TLS loops and bounded
accepted-work ownership. Native-only all-target compilation, formatting,
inventory conformance paths, changed documentation links and diff checks pass.
The public host/native witness histories round-trip their query/reply through
format 3 when native support is enabled; this is codec/provider composition,
not encrypted format-3 session negotiation. Final focused library/member checks
also cover rejecting a claimed committed boundary at or before the base's last
configuration and ignoring a high term carried by a control query.

## Slice 47 authenticated wire selection evidence

Two new TLS tests and one QUIC test verify explicit versions 2/3, invalid
selection, independent config clones, fragmented authenticated stream transfer,
real sockets and mismatch before Ready. One TCP and one shared-QUIC connector
test verify the selected version survives handoff and mismatches release terminal
ownership without a Ready session. Two native-storage public witness histories
send real core query/reply through authenticated TCP/TLS and QUIC format-3 framed
transports with original outbound credits held until exact local completion.

Two startup histories run both versions over three-node native TCP and QUIC
clusters, commit a write, drain/join workers and restore applied state after
recovery. The fixture's initially missing parent directory was corrected;
production startup semantics remain unchanged. Final QUIC-enabled library/
connect/member/QUIC/QUIC-connector/secure/startup tests pass 38/13/19/10/5/14/8
(107 total). Existing service/effect-owner/peers/transport regressions pass
7/94/13/13 (127 total). Core-only and native-only all-target builds, all-feature
all-target Clippy and API docs pass.

The prior inventory accidentally placed an implemented authorization record in
the deferred-name array. The record now belongs to contracts. The new Node-only
validation/check-inventory.mjs verifies array shapes, unique names, required
fields and actual conformance files; it passes all 46 contracts and rejects the
previous committed inventory as a negative control. This is metadata validation,
not new consensus evidence.

This selects one exact supported wire version; it does not negotiate a list or
establish membership readiness. NativeStartup still exposes the static service;
public configuration ingress and complete faulted activation/retirement remain
gated. There is no macOS execution, formal completeness or performance claim.

Default TCP/TLS member/secure/startup suites independently pass 18/14/6 tests.
Formatting, changed-document links, inventory shape/conformance paths and diff
checks pass. The QUIC-enabled combined history also passes after the final
assertion uses a borrowed expected-message slice.

## 2026-10-08: membership-derived shared connection reconciliation (slice 48)

- QUIC-enabled library, effect-owner, peers and startup suites pass 40, 97, 17
  and 8 tests respectively (162 total). Startup uses real loopback TCP/TLS and
  QUIC: reconcile unchanged assignments, commit writes, drain/join and recover
  with exact selected wire formats 2 and 3.
- Two actual-core tests retain pending/rollback peers and the committed joint
  predecessor until exact final-commit durability. Four roster tests cover
  shared-group requirements, cancellation/stale Ready, send ownership/session
  floors and preflight rejection. Three driver/Node tests cover terminal provider
  receipts, retained ingress, staged/accepted output and returned route ownership.
- Core-only all-target check, all-feature/all-target Clippy with warnings denied,
  all-feature API docs, formatting and diff checks pass. Inventory validator
  passes with 47 implemented contract records; this is metadata evidence only.
- During validation, a test assertion was corrected to inspect the existing
  ConnectionRejected.reason rather than compare the owning rejection wrapper.
  Store replacement was tightened to require drained handoff after identifying
  session-floor loss across an ABA identity switch; its rejection and subsequent
  same-store floor retention are tested.
- No live credential rotation, prospective membership resource admission,
  distributed readiness, public online configuration ingress, macOS execution,
  performance or complete protocol proof is claimed. Full P0–P7 remains active.
