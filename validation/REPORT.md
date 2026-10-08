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

## 2026-10-08: prospective connection preview and recovery admission (slice 49)

- QUIC-enabled library/effect-owner/peers/startup suites pass 41/99/18/8 tests
  (166 total), including actual recovered-core constructor rejection and native
  TCP/TLS plus QUIC commit/drain/recover histories with wire formats 2/3.
- Actual-core prospective resource checks cover learner append growth,
  intermediate joint/final peers, compacted joint snapshot membership, limits,
  conflicting stores, wrong scope and fencing. Downstream checks cover shared
  group unions, route/pin availability, retained-history capacity and pure clocks.
- Core-only all-target build, all-feature/all-target Clippy with warnings denied,
  all-feature API docs and inventory shape/path checks pass. One test fixture's
  scheduler capacity was corrected after SchedulerContract rejection; the actual
  recovered rollback-peer construction test then passes.
- A preview does not retain capacity or admit protocol state. Queued-event
  reservations, readiness and complete distributed activation remain unfinished.
  Online configuration ingress stays gated. No macOS, performance or full proof
  claim; the complete P0–P7 objective remains active.

## 2026-10-08: retained connection capacity in the owner (slice 50)

- QUIC-enabled library/effect-owner/peers/runtime/startup/counter-service suites
  pass 43/104/18/25/8/7 tests (205 total). Native TCP/TLS and QUIC process histories
  still commit, replace leaders and recover native files. Existing 100-group
  callback/worker histories also pass with the networked Node budget installed.
- Five downstream budget histories cover competing queued inputs, exact returned
  events/tickets, shared-peer/store conflicts, protocol rejection, close/stop,
  retained history, budget tightening, witness-reply reservations and registration
  while a future peer already occupies capacity. The registration test explicitly
  succeeds after releasing the queued reservation, avoiding a capacity assertion
  that would fail even without the competing queued event.
- Two actual-core histories cover pending/durable/rollback retention, fencing an
  unreserved callback before effects escape, staged membership snapshots before
  log persistence and witness-authorized connection to a promoted node absent
  from the receiver's membership. No simulated token is claimed as device proof.
- Core-only all-target build, all-feature/all-target Clippy with warnings denied,
  all-feature API docs, formatting/diff and inventory shape/path checks pass.
- Capacity union recomputation is bounded; no throughput or allocator claim.
  Credential/route-plan admission and readiness remain incomplete, so online
  configuration ingress remains gated. No macOS execution or full protocol proof
  claim. The complete P0–P7 objective remains active.

## 2026-10-08: retained credential/route admission (slice 51)

- QUIC-enabled library/effect-owner/peers/runtime/startup/counter-service suites
  pass 43/109/18/25/8/7 tests (210 total). This includes native TCP/QUIC wire-2/3
  create, live plan replacement, committed write, drain and recovery histories.
- Five new downstream histories cover missing pins, queued route withdrawal,
  stale budget restoration, witness-driven actual connections, per-peer original
  send-credit retirement, live Node replacement and failed construction return.
  Existing failed factory recovery also verifies retained admission hints.
- Core-only all-target check, all-feature/all-target Clippy with warnings denied,
  API docs and inventory validation pass (49 contract records; inventory checks
  shape and paths, not behavioral correctness). Formatting/diff checks pass.
- An earlier service run failed with AddrInUse. Inspection confirms the fixture
  reserves ports then releases them before child binding, leaving a race window.
  The subsequent full run passes without code changes; the specific competing
  socket owner was not established and collision-free startup is not claimed.
- Plans and identity maps are metadata-bounded; host endpoint heap/cloning obeys
  the connector contract. No new persistence receipt, watermark or generation.
  Live credential rotation, learner readiness and full faulted online membership
  histories remain unfinished. Public configuration ingress remains gated.
  No macOS execution, performance or full protocol proof claim. P0–P7 stays active.

## 2026-10-08: fresh host-driven learner readiness (slice 52)

- QUIC-enabled library/effect-owner/learners/peers/runtime/snapshot/snapshot-worker/
  startup/counter-service suites pass 43/109/13/18/25/19/14/8/7 tests (256 total).
  Static TCP/QUIC service leader-loss/recovery and existing native 100-group
  histories remain green alongside readiness and snapshot conformance.
- Five new downstream readiness histories use actual core election/replication,
  exact selected providers and application checks. Coverage includes pending and
  Written work, applied lag, schema/size requirements, wrong authenticated store
  sessions, altered complete requests, duplicate/old replies, commit/term/config
  invalidation, missing compacted data and native file checkpoint/reopen with
  fresh peer session. Initial assignments are explicitly host-imported.
- HostSnapshots was corrected to include the 48-byte minimum file envelope for
  small checkpoints. Two readiness fixtures were corrected to preserve a durable
  ballot and finish an existing heartbeat before acknowledging a newer index;
  the production storage and request-scope checks were retained.
- Core-only all-target check, all-feature/all-target Clippy with warnings denied,
  API docs, formatting/diff and inventory shape/path checks pass (50 contracts).
- This is the host-driven readiness contract, not native readiness RPC/worker
  integration or a released promotion endpoint. Placement authorization and
  faulted online membership histories remain required. Receipts use the existing
  authenticated non-Byzantine provider model; no new persistent record,
  watermark or generation, macOS execution, benchmark or protocol proof claim.
  Public configuration ingress remains gated and P0–P7 remains active.

## 2026-10-08: host-authorized local configuration proposals (slice 53)

- QUIC-enabled library/effect-owner/learners/membership/peers/runtime/snapshot/
  snapshot-worker/startup/counter-service suites pass
  43/111/19/27/18/25/19/14/8/7 tests (291 total). Static service TCP/QUIC
  leader-loss/recovery and native 100-group histories remain green.
- Six new learner/proposal histories cover missing/duplicate/extraneous and
  mismatched proofs, journal rejection purity, exact persistence before dependent
  output, committed joint before final, native file reopen, old/new recursive
  weighted commitment, failed native barrier fencing and modeled unsynced-byte
  loss, plus queued execution-time authentication and retained-capacity rejection.
  Application replay emits no command receipts for configuration entries;
  administrative events produce no ordinary client operation/position.
- Two new owner/peer-driver histories cover prospective local configuration
  fanout, queued shared peer/store/capacity union, exact returned rejected inputs,
  retained credential/route admission and refusal of queued route withdrawal.
- The oversized-vector test initially cloned away its spare capacity. It now
  moves the original input and checks the returned retained capacity. This was a
  fixture correction, not a production relaxation. Clippy's redundant closure
  was also corrected; all-feature/all-target Clippy with warnings denied passes.
- Core-only all-target check, API docs, formatting/diff and inventory shape/path
  checks pass (51 records). Inventory validation is metadata checking only.
- These are local administrative and storage histories. Initial assignments and
  peer-2 replies are host assertions, not proof of remote configuration delivery.
  Native readiness RPC/worker integration, placement authorization, distributed
  activation modeling and faulted online transitions remain required. Public
  configuration ingress remains gated; no macOS, physical power-loss, benchmark
  or full protocol proof claim. The complete P0–P7 goal stays active.

## 2026-10-08: native readiness exchange (slice 54)

- Four new learner histories and one wire history pass. Learners/wire/outbound/
  snapshot-worker suites pass 23/14/4/14 tests. Core-only learner histories pass
  13 tests and core-only all-target builds pass. Final focused runs cover the
  shared scope/progress checks, boxed memory accounting and local promotion use.
- Actual TCP/TLS and QUIC histories carry fresh requests and denial/positive
  replies through native framing, the native snapshot worker over a host-selected
  pinned snapshot provider, and the original EffectOwner/SnapshotRouter. They
  verify a compacted learner, another group progressing while completion is
  withheld, stale ticket rejection, accepted-work shutdown drain, exact original
  send credits and checked evidence feeding local joint-journal persistence.
  Initial learner assignment and peer-2 quorum replies remain host assertions;
  these do not claim remote online enrollment or configuration delivery.
- Core histories cover future-term inertness, missing-prefix denial before I/O,
  wrong sessions, duplicate responses, capability denial without fencing,
  storage failure fencing without a reply and recovery dropping volatile state.
  Existing native-file readiness/reopen histories remain green separately.
- Format-4 tests cover every truncation, valid-checksum old-version relabeling,
  invalid envelope/capability/Boolean fields and exact decoded/outbound memory
  ceilings. TLS, dedicated/shared QUIC and three-node TCP/QUIC startup histories
  now exercise exact versions 2/3/4 plus mismatches with no downgrade; startup
  writes, drains/joins and recovers native files.
- The affected library/effect-owner/runtime/Raft/snapshot/secure/QUIC/connector/
  startup/service suites pass, including existing native 100-group histories and
  seven static-service tests. After boxing, owner/runtime/learners/wire/outbound
  checks were repeated; after shared-check changes, focused learner/wire/worker
  checks were repeated. No unrelated unchanged test loop was used.
- An unrelated-group fixture initially canceled a nonexistent read; its corrected
  inert event verifies scheduling without that semantic error. Clippy identified
  excessive Event size from an inline request; the request is now boxed and its
  allocation is explicitly charged before native decoding/encoding. A focused
  core-only test then caught premature pinned-snapshot I/O for an unmatchable
  prefix during shared-helper extraction; prefix checks were restored before I/O.
- All-feature/all-target Clippy with warnings denied, API docs, formatting/diff
  and inventory shape/path checks pass (52 contracts). Metadata validation is
  not protocol conformance. No new durable token, watermark or generation.
- Readiness uses authoritative recovered core log limits/state and selected
  snapshot/application capabilities. Custom hosts must honor that provider
  binding; online administration must additionally validate wire/transport
  capacities and placement. Lost replies require explicit host cancellation and
  retry. The service CLI has no online administrator or automatic readiness loop.
  Configuration-bearing public Append and membership Snapshot remain gated.
  No macOS execution, physical power-loss, benchmark or full protocol proof
  claim; online activation and P5–P7 remain outstanding under the active goal.

## Slice 55 — mixed-head election recovery

- Reproduced WrongIdentity when a final-view candidate requested ballots from
  committed-joint survivors after retiring-leader loss. The actual-core regression
  now elects, catches up, commits and reconstructs the final view on recovery.
  Its journal fixtures and durability receipts are host assertions.
- Independent activation model: 3 tests; 3,007 accepted-prefix placements,
  20,029 certificates and 98,488 certificate pairs. Majority/weighted/nested
  old/new policies and overlap/disjoint sets are covered. Negative controls find
  new-only joint, premature-final, equal-scope stall and early-vote-reply failures.
  Factored fixed-term linear-prefix/certificate enumeration and separate ballot
  receipt checks do not establish arbitrary distributed traces or liveness.
- Public replication-scope suite: 7 tests, including host/native vote promises,
  stale candidate logs, exact completions, uncertain synchronization/publication,
  modeled power loss and actual native-file reopen. Native wire round trips now
  include differing-scope Vote requests. No persistent or wire schema changed.
- All-feature affected library (44), activation/ballot models, ballots, learners,
  member recovery (19), Raft (22), request scope (7) and runtime (25) pass.
  Existing learner/witness TCP/QUIC checks remain green; these do not test the new
  mixed-head election over sockets or full online configuration delivery.
- Core-only library (35), activation model (3), ballot model (2) and public
  request scope (2) pass. No macOS, physical power-loss, performance or full proof
  claim. Configuration ingress stays gated and administrator integration remains
  required; the full P0–P7 goal stays active.
- Final all-feature/all-target Clippy with warnings denied, core-only all-target
  compilation, formatting/diff and inventory shape/path checks pass (52 contracts).
  Inventory validation checks metadata rather than protocol behavior.

## Slice 56 — owned administration requests

- Node now composes configuration authorization through the existing client,
  replica and owner path. The public bounded observer reports committed only
  for an exact record/position in durable committed state; admission, Written
  and pending membership produce no committed receipt.
- Host Node tests cover joint/final sequencing, execution authorization denial,
  static-wire/missing-network rejection, cancellation followed by actual commit,
  queued campaign timing, byte/count retention, Written storage failure and
  shutdown/recovery preserving earlier committed or unresolved outcomes.
- Native startup commits a same-electorate single-voter joint/final journal via
  the selected worker, joins it, reopens real files and reconstructs exact member
  state. This is not multi-node configuration delivery, learner promotion or
  dynamic recovery through the static startup convenience path.
- Affected all-feature owner tests (118 at the broad run), learners (23), Raft
  (22), runtime (25) and startup (9) passed. Later focused Node checks include
  queued-term and selected-network preflight cases. Existing TCP/QUIC/100-group
  histories remain green. A service QUIC case encountered AddrInUse during the
  combined run; its isolated rerun passed. The other six service cases passed in
  the original run. No production transport behavior was changed for that error.
- A fixture's zero-effect budget was invalid and was replaced with one submitted
  persistence effect, preserving the pre-durability assertion. The existing
  queued-promotion check was updated from absent operation/position metadata to
  exact operation 53 and position (3,2), without weakening its binding/persistence
  assertions. A queued request no longer inherits an unrelated admission-time
  election term; committed evidence uses its actual proposed term.
- Native placement/failure-domain policy, complete selected codec/transport
  capacity validation, durable operation status/resumption, service endpoints
  and faulted remote add/promote/remove remain pending. Configuration-bearing
  public Append and membership Snapshot remain gated. No macOS execution,
  physical power-loss, performance or full protocol proof claim; P4–P7 remain
  active within the full objective.
- Final focused Node suite passes all 27 cases with all features and core-only.
  All-feature/all-target Clippy with warnings denied, core-only all-target
  compilation, all-feature API docs, formatting/diff and inventory shape/path
  checks pass (53 contracts). Metadata validation is not protocol conformance.


## Slice 57 — durable operation status and safe resumption

- GroupLog/Raft/Node expose historical durable committed versus accepted phases;
  Node resumption reconstructs only a committed joint final and uses fresh normal
  admission/authorization. Written/pending state gives no advancement. Local
  absence is inconclusive and completed identity is not new-payload equivalence.
- All-feature membership suite: 32 pass, including shared host/native phase and
  rollback conformance, snapshot/reclaim, missing compacted joint term, modeled
  sync/publication failures and actual-file joint/final/checkpoint reopen.
- All-feature focused Node configuration checks: 10 pass, including lost wait,
  default-authorization denial, successful fresh resumption and recovery-required
  status rejection. Native startup selected-worker local finalization/resumption
  and actual WAL reopen: 1 pass. This is same-electorate one-voter evidence, not
  remote enrollment or distributed membership delivery.
- Final all-feature Node facade suite: 28 pass. Core-only membership suite: 18
  pass; configuration-filtered owner checks: 13 pass.
  All-feature/all-target Clippy with warnings denied, core-only all-target
  compilation and all-feature API documentation pass. Formatting/diff and
  inventory shape/path checks pass (54 contracts); metadata is not conformance.
- No journal/checkpoint schema, durability token, generation or watermark was
  introduced. Compacted IDs outside the active joint establish completed identity
  under existing journal grammar, without discarded phase/payload/position.
  Retained active joints may have no original term below the snapshot boundary.
- Native policy/capacity admission, service endpoints, native enrollment and
  faulted remote add/promote/remove remain pending. Configuration ingress stays
  gated, including pending partial-joint activation/catch-up histories. No macOS,
  physical power-failure, performance or full proof claim. Full P0–P7 stays active.


## Slice 58 — native placement authorization

- Public PlacementAuthorizer, exact ReplicaPlacement and typed FailureDomainId
  compose through Node execution authorization. NativePlacementAuthorizer checks
  current stable/joint and proposed configurations; no persistence effects or
  durability evidence come from placement approval. No quorum callback replaces
  mandatory consensus rules.
- Six downstream native-policy tests pass: exact group/store incarnation,
  unknown assignments, learners excluded from voting-domain count/survival,
  concentrated and valid weighted/recursive policies, colocated domains,
  accepted joint finalization, unprepared-voter journal rejection and bounded
  constructor failure returning original assignments. The initially private
  validator call was corrected to public Membership replay, not exposed publicly.
- All-feature Node facade: 29 pass, including selected host policy revocation
  after queued admission, no persistence/state mutation on denial and fresh
  allowed retry. The new host check also passes core-only. Native startup
  selected placement, joint/final resumption and actual WAL reopen: 1 pass.
- All-feature/all-target Clippy with warnings denied and core-only all-target
  compilation pass. No remote configuration delivery, physical failure-domain
  independence, multi-domain-loss guarantee, macOS execution or full proof claim.
  Domain labels are deployment assertions. Full selected codec/transport capacity
  admission, service enrollment/endpoints and faulted multi-node activation remain
  required. Public configuration ingress stays gated; P0–P7 remains active.
- Final all-feature API documentation, formatting/diff and inventory shape/path
  checks pass (55 contracts). C18 planning/scoring/move proposals remain pending;
  this implements its authorization subset. Inventory checks validate metadata.


## Slice 59 — selected codec/transport configuration envelope admission

- Public optional capacity queries default to unsupported. Native encoder counting
  validates proposed append, declared command and prospective membership checkpoint
  with retained operation IDs; opaque application sizes use virtual counts rather
  than application-sized allocations. Native factory and roster independently
  check selected wire version, valid footprints and frame/decoding budgets before
  Node configuration persistence. Transport contract version is now 3; wire and
  persistent bytes are unchanged.
- Four downstream capacity cases pass: counts match actual encode/decode frames,
  exact/one-byte-short boundaries, operation history growth, unsupported version,
  policy limits, invalid envelope and selected native factory. Host Node injection
  tests unsupported/mismatched/oversized/malformed and explicit positive capacity.
- All-feature and core-only Node facade suites: 30 pass each. Native startup
  pre-persistence oversized-envelope rejection, valid joint/final resumption and
  actual WAL reopen: 1 pass. Shared membership suite: 32 all-feature and 18
  core-only pass. Existing learner cases: 21 pass without sockets; TCP/QUIC
  readiness histories: 2 pass with socket permission. Wire suite: 14 pass;
  transport suite: 12 non-socket cases plus 1 TCP/TLS case pass. Socket checks
  initially encountered sandbox PermissionDenied and were rerun with socket
  permission; production behavior was not changed for those environment errors.
- Test fixture fixes preserve semantics: retain connector-compatible roster limits
  rather than generic defaults, explicitly copy owned proposal data, and compare
  decoded messages by borrowed slices. Journal preview shares the existing clone
  and accept grammar with validate_next; it does not create membership authority.
- Declared-envelope checks do not enforce future schema/application/dedup growth,
  arbitrary batching or remote enrollment. Service integration must bind/enforce
  actual command/checkpoint bounds and support dynamic recovery/admin endpoints.
  Partial-joint catch-up/election and full faulted remote add/promote/remove remain
  release gates. Public configuration ingress stays gated; full P0–P7 stays active.
  No macOS, physical failure, performance or full proof claim.
- Final core-only host wire default-capability check passes (1). All-feature/
  all-target Clippy with warnings denied, core-only all-target compilation,
  all-feature API docs, formatting/diff and inventory shape/path checks pass
  (56 contracts). Inventory checks metadata, not protocol conformance.


## Slice 60 — explicit native member restart

- NativeMemberStartup uses existing verified member/snapshot recovery and shared
  native assembly, with explicit bounded exact provisioned store/credential input.
  Recover and membership wire are required. Both committed and accepted local
  assignment must match. Roster peers include rollback history and require exact
  provisioning; future/retired hints grant neither active roster nor voter status.
  TCP dial authorization and TCP/QUIC pins use the same map. No implicit creation,
  enrollment, durability effect/token, generation, watermark or format change.
- Eight downstream cases pass: seeded native learner/joint/accepted-final/final/
  compacted histories reopen over TCP and QUIC, restore application retry state,
  preserve non-voting campaign rejection without term advance, refuse uncommitted
  local assignment, missing/wrong rollback-peer identity, removed local member
  and missing checkpoint data. Static startup remains strict; obsolete peer
  credentials can be omitted after final commitment. Pre-I/O mode/version denial
  and TCP/QUIC late failure exercise original application/worker/socket cleanup.
- Existing startup suite: 9 pass, including selected worker administration,
  static three-node wire 2/3/4 TCP/QUIC histories, independent host wake and
  construction failure cleanup. All-feature/all-target Clippy with warnings
  denied and core-only all-target compilation pass. All-feature API docs pass.
- Histories seed native durable journal fixtures; they do not show online remote
  commit/enrollment of those configurations. Recovery may advance store sessions
  and restore the returned application before a later rejection; cleanup is not
  rollback. Native service enrollment/admin endpoints, enforced application
  bounds and full faulted remote transitions remain required. Configuration
  ingress stays gated; full P0–P7 remains active. No macOS, physical failure,
  performance or full protocol proof claim.
- Final eight-case restart rerun also verifies recovered joint finalization hints
  and accepted-final WaitForCommit resumption. All-feature/all-target Clippy with
  warnings denied, formatting, diff checks and inventory shape/path checks pass
  (57 contracts). Inventory validation checks metadata, not protocol correctness.

## Slice 61 — trusted learner checkpoint enrollment

- Shared enroll_learner_snapshot uses public log/snapshot/application contracts;
  NativeMemberStartup::enroll_snapshot provisions selected native files without
  opening sockets or starting workers. Exact stable learner assignment, original
  bootstrap, schema/data, restored boundary and bounded envelopes precede import.
  Publication and durable pin precede log barrier and verified member recovery.
  Source commitment remains explicit host authority, not checksum evidence.
- Five all-feature enrollment cases pass; three core-only host cases pass. These
  cover native/host import, retry/dedup, identity/schema/data/budget refusal, seven
  snapshot stage/publication/pin failure points, and five native WAL append/sync/
  manifest failures with power-loss recovery and same-image retry. Changed images
  and progressed stores refuse replacement. Snapshot faults use a host recovery
  model; the native snapshot suite separately exercises real provider failures.
- Native member startup suite: 10 pass, including two new filesystem enrollment,
  exact retry and TCP/QUIC reopen cases. Existing member recovery: 17 non-socket
  cases pass, then 2 authenticated TCP/QUIC cases pass with socket permission.
  The first socket invocation encountered sandbox PermissionDenied; no production
  change was made for it. Existing snapshot suite: 19 pass, including interrupted
  chunk/seal/publication, pin recovery and snapshot/log crash histories.
- All-feature/all-target Clippy with warnings denied, API documentation and
  core-only all-target compilation pass. Formatting/diff and inventory shape/path
  checks pass (58 contracts); inventory is metadata validation only.
- Native initial WAL/bootstrap/snapshot-store creation is not one transaction;
  interruption before both stores exist requires explicit inspection and Recover
  refuses missing state. Once initialized, exact publication/pin/log retries
  recover through existing contracts. No rollback, automatic store replacement,
  copied source ballot, new durability token/generation/watermark or format.
  Fixture source snapshots assume trusted commitment; they do not demonstrate
  distributed configuration commitment, online enrollment, macOS or performance.
  Service administration/enforced bounds and faulted remote membership release
  remain pending. Full P0–P7 remains active; public configuration ingress is gated.

## Slice 62 — service configuration status and counter lifetime envelope

- Native local configuration-status OPERATION_ID reads the existing Node durable
  observer, separating committed and accepted phases, contiguous committed prefix
  and durable log end. Reply labels local evidence and inconclusive absence;
  finalization is only a planning hint. Nonzero ID parsing and fixed scalar phase
  output retain no mutation request or configuration policy payload.
- Counter readiness_requirements covers schema 1, eight-byte commands and every
  retained operation outcome: 32+33*configured capacity checkpoint bytes. Declaration
  validation rejects smaller bounds/wrong schema. Existing admission, application
  and restore enforce capacity, including pending IDs and no implicit retry eviction.
  Service startup binds its 10000-operation, 330032-byte declaration to those
  bounds and selected native command/checkpoint payload limits before resources.
- Application suite: 9 pass all-feature and 9 core-only. New full-capacity case
  checks exact/one-byte-short checkpoint size, overflow/retry retention, no new
  operation beyond capacity, stable envelope across restore, undersized/wrong
  schema declarations and changed-capacity restore refusal.
- Configuration capacity suite: 5 pass. New service-size case fills all 10000
  identities, round-trips the 330032-byte checkpoint through actual native wire
  formats 1–4 and restores full retry history and applied value. This is a single
  checkpoint with small fixture membership, not arbitrary metadata growth/batching.
- Actual executable service suite: 7 pass with socket permission. TCP/QUIC process
  histories query local absence on leaders/followers and after leader restart,
  reject operation zero, and retain write/read/retry/leader-loss/checkpoint/shutdown
  checks. These CLI histories have static membership; dynamic journal observation
  semantics are covered by existing Node administration tests rather than these
  process fixtures. Read-only fixture polling waits for the reopened command port.
- All-feature/all-target Clippy with warnings denied, all-feature API docs,
  core-only all-target compilation, formatting/diff and inventory checks pass
  (59 contracts). Inventory checks metadata, not protocol correctness.
- Counter-specific enforced bounds and local status do not release remote mutation
  ingress or establish generic host application envelopes. Growing membership
  metadata still requires selected codec/transport configuration checks. Mutation
  endpoints, provisioning/readiness integration and faulted online transitions
  remain pending; P0–P7 stays active. No new token/generation/watermark/format,
  macOS execution, performance or full protocol proof claim.

## Slice 63 — confirmed partial-joint election release blocker

- Actual core diagnostic prepares committed learner assignment and partial joint
  delivery with weighted old/new required voters 2/5. Node 1 disappears while
  2/3/5 retain both physical quorums. Four complete campaign/restart rounds deliver
  every surviving vote request/reply but produce no leader: node 3 is stale,
  node 2 lacks the required new vote, node 5 remains a learner. Exact public member
  recovery preserves each durable prefix and cannot manufacture delivery.
- Explicit internal delivery of the joint to learner 5 isolates the missing
  transition. Public configuration ingress refuses it. Pending accepted membership
  is not durability evidence: Campaign and Vote return Busy until the exact
  completion. Only afterward can node 5 grant a durable vote and node 2 elect.
  No production recovery transfer is implemented; this is a release diagnosis.
- Focused actual-core diagnostic passes all-feature and core-only. Independent
  activation model: 4 pass in both builds, including the available-quorum/no-
  election case and feasibility after joint delivery. Existing all-feature library
  tests: 44 pass under ordinary permissions; the remaining QUIC socket case
  encountered PermissionDenied and passes when rerun with socket permission.
- All-feature/all-target Clippy with warnings denied passes. Formatting, diff and
  inventory checks pass (59 contracts). No new public contract or production
  behavior. The diagnostic encodes an unresolved defect and must become a positive
  production recovery regression after repair; green tests do not release P4.
- Exact completion tokens are host-asserted fixtures; this adds no new filesystem,
  network, fork, macOS, performance or full proof evidence. Service mutation
  integration stays behind the partial-joint repair and remote lifecycle gates.
  Full P0–P7 remains active; P8 is deferred.

## Slice 64 — append-only production joint election repair

- Campaign's exact durable term/self-ballot completion precedes sending the
  accepted uncommitted joint entry to exact promoted learners, before ordinary
  Vote requests. Existing Append shape, one entry, leader_commit zero. Sender
  must be an old-view voter. Receiver must be an exact committed/accepted stable
  learner with a matching log end, trusted sender/store, valid promotion/grammar,
  term/context and byte budget. No voting history replacement or commit advance.
- Actual-core weighted history uses public Campaign/Receive after four dropped-
  repair/restart rounds. Vote-before-repair is denied; pending joint persistence
  blocks Campaign/Vote. A repair acknowledgement leaves the sender Candidate;
  only a subsequent durable ballot elects it. Application proposal is NotLeader
  before election. Fourteen refusal variants preserve term/role/timer/log state;
  lost ack and restart preserve activation and duplicate repair cannot mutate it.
- New native provider tests: 3 pass. Existing wire versions 2–4 round-trip repair
  into the native WAL; injected short append, sync and manifest failures fence
  without acknowledgement and recover old or whole joint assignment; actual
  filesystem reopen preserves the joint after losing its acknowledgement.
- Library tests: 46 all-feature pass with socket permission; 37 core-only pass.
  Affected suites: learners 23, member recovery 22, wire 14 and independent
  activation model 4 pass. Existing learner/readiness and witness TCP/QUIC
  histories pass; they do not exercise this repair over network sockets.
  Clippy all-feature/all-target with warnings denied, all-feature docs and
  core-only all-target compilation pass. Formatting/diff and inventory shape/path
  checks pass (60 contracts); inventory is metadata, not protocol conformance.
- Protocol behavior changes only through a strict public joint-repair exception;
  general configuration Append and membership Snapshot remain gated. Existing
  accepted activation and exact LogTicket/DurableLog dependencies are reused;
  no new token/generation/watermark or format. Safety rationale is documented in
  MEMBERSHIP_CORE.md, including why arbitrary candidate suffix replacement is
  forbidden. This is bounded evidence, not a full fork/term/liveness proof.
- Reproduced exact-prefix weighted stall is repaired. Behind/compacted learners,
  candidate suffixes after the joint, promoted senders, broader recursive-policy
  histories and actual remote repair still require work. No general P4 release,
  macOS execution or performance claim. Service mutations remain gated and full
  P0–P7 stays active; P8 remains deferred.

## Slice 65 — authenticated TCP/QUIC joint repair through native nodes

- Two new native histories start from persisted partial-joint fixtures with
  different required weighted old/new voters and the former leader unavailable.
  Normal native polling, TLS-authenticated TCP or QUIC sessions and WAL workers
  deliver the production repair, elect the surviving candidate and commit/apply
  an application command on both replicas. No test-injected receive or election
  is used in the live portion. The seeded starting records do not establish
  distributed enrollment or initial configuration commitment.
- The test drains nodes and joins worker ownership, then reopens actual native
  WAL/snapshot files. Both replicas recover their joint membership, voting status
  and the application value at the actual applied client completion position.
- Focused remote repair: 2 pass. Full all-feature native_member_startup: 12 pass
  with local socket permission. All-feature/all-target Clippy with warnings
  denied passes. Formatting and diff checks pass; inventory shape/path validation
  passes for 60 contracts (metadata evidence, not protocol conformance).
- This adds native remote evidence for the strict matching-prefix repair only.
  Behind/compacted prefixes, candidate tails, promoted senders, recursive-policy
  recovery and full remote membership lifecycle remain unvalidated. No general
  P4 release, macOS, performance or complete protocol proof claim. Full P0–P7
  stays active; P8 is deferred.

## Slice 66 — retained-range pure-extension joint repair

- Candidate transfer now contains at most 64 retained entries ending at the
  uncommitted joint, within a conservative replication byte budget. It begins
  after the stable configuration and never before the sender's retained floor.
  The receiver verifies contiguous indices/terms, identical retained overlap,
  budgets and the original exact committed-learner/trusted-old-voter promotion
  conditions before role/term changes. A matching local checkpoint boundary
  permits trimming already-compacted overlap; mismatches refuse. No overwrite,
  commit claim, new RPC/format, cursor or durability token.
- New core histories cover missing/partial/full overlap, 64-entry truncation,
  a longer source with an in-window learner, sender byte truncation/oversized
  joint omission, forked overlap, invalid ranges/configurations and receiver
  budgets. A host-verified checkpoint fixture covers compacted-overlap trimming;
  it is not native snapshot transfer or publication evidence. Pending repair
  blocks voting/campaign, acknowledgements cannot elect and restart retains the
  durable joint without advancing commit.
- Native wire versions 2–4 and native WAL fault/reopen tests now use a 32-entry
  tail. Short append, failed sync and failed manifest publication fence without
  reply; modeled power loss recovers only the old prefix or whole repaired
  assignment, then exact retry succeeds. Actual native files preserve repair
  after losing its acknowledgement. ModelIo power-loss assumptions remain
  distinct from hardware power-failure validation.
- TCP/QUIC native nodes missing 32 entries repair, elect, commit/apply a client
  write, drain/join workers and reopen application state. Starting membership
  records are seeded premises, not distributed enrollment/commit evidence.
- Actual final runs: all-feature library 50 pass, core-only library 41 pass;
  native member startup 14, member recovery 22, learners 23, wire 14 and independent
  activation model 4 pass. Socket suites use local socket permission. All-feature
  all-target Clippy with warnings denied passes. Formatting/diff and inventory
  checks pass (60 contract metadata records). No broad test rerun is treated as
  a complete protocol proof.
- Learners beyond the retained entry/byte window, missing checkpoint boundaries,
  multi-batch repair, transferred snapshots, promoted senders and broader
  recursive/fork/term histories remain gates. General configuration ingress stays
  closed. This advances P4 recovery toward native enrollment/admin integration,
  then faulted remote membership release; P5–P7 and full P0–P7 remain active.
  No macOS execution or performance claim; P8 remains deferred.

## Slice 67 — native format-5 multi-batch retained learner repair

- New separate repair request/reply messages transfer at most 64 entries per
  range, with repeated durable joint assignment metadata and no commit claim.
  Exact stable committed learner/old voter identity, journal/terms/context,
  identical overlap and byte limits precede receive mutation. Ordinary atomic
  LogUpdate plus its exact durable completion permits a dependent reply. Existing
  durable overlap/checkpoint hints may reply without new storage; neither is
  counted toward elections, read authority or replication commitment.
- Actual core: 160-entry transfer after first-cursor reply loss and restart;
  three bounded batches, no premature voting/commitment; duplicate/stale replies
  cannot advance a new cursor. Final repair resends a normal Vote, rather than
  electing. Delayed repair after promotion refuses without mutation. Ten malformed
  authority/range cases, wrong reply end/context, higher-term persistence and a
  matching compacted learner hint are covered. A candidate with an already-
  committed joint also repairs three batches without exporting commit authority.
  A compacted-away source joint skips transfer while preserving ordinary election
  requests. Checkpoint inputs are host-verified fixtures, not new snapshot
  publication/transfer evidence.
- Native wire 5 explicitly selected; older wire rejects new tags, all truncated
  repair frames and constrained entry/decoded budgets refuse. Request/reply
  round-trips traverse all three ranges. Native WAL short append, sync and
  manifest faults fence without reply. Model power loss recovers only the old
  prefix or complete first batch, then exact retry completes the joint. Existing
  modeled storage assumptions do not establish hardware power-failure behavior.
- Native TCP/TLS and QUIC nodes missing 160 entries repair, elect, commit/apply
  and reopen a client write. Existing 32-entry and exact-prefix cases still pass.
  Initial membership records are seeded; distributed enrollment/initial joint
  proposal commitment remains unproven by these fixtures.
- Final affected runs: all-feature library 55 pass; core-only library 46 pass;
  native member startup 16, member recovery 24, learners 23, activation model 4,
  outbound 4, runtime 25, secure 14, startup 9, transport 13, wire 14 pass.
  Owning-node/effect-owner suite: 125 pass, including wire-5 construction refusal.
  Readiness also round-trips through wire 5 in its focused check. Socket suites
  use local socket permission. Clippy all-feature/all-target with warnings denied
  and all-feature docs pass; formatting/diff and inventory metadata checks pass.
- Validation corrected the old TLS unsupported-version fixture (5 now supported,
  6 refused) and the new node fixture's scheduler capacity. The final reruns above
  pass. The independent activation model is unchanged and does not model the
  whole new multi-batch protocol. Finite actual-core histories are not a full
  fork/term/liveness proof. Generic direct core hosts must select compatible
  codec/transport budgets; Node checks the advertised roster version, and native
  startup selects matching default providers.
- Missing source prefixes/snapshot transfer, conflicting learner tails, promoted
  senders and broader recursive-policy histories remain release work. General
  configuration ingress stays closed. Mini item 1 advances toward native
  enrollment/admin integration, then faulted remote membership release; P5–P7
  and full P0–P7 remain active. No macOS execution or performance claim.

## Slice 68 — committed snapshot learner repair through native format 6

- Historical committed source checkpoints use existing SnapshotRequired/load
  ownership and exact current reference/context checks. Exact old-view voter to
  committed stable learner only; bootstrap, stable assignment, promotion, retained
  operation IDs, scope, terms and application bounds precede installation. This
  trusts authenticated non-Byzantine Raft providers exporting locally verified
  committed images; no independent commitment certificate is added.
- Host providers demonstrate stable and joint images, no reply before WAL plus
  application restoration, Busy election boundaries and restart after lost restore
  completion. Existing voters, wrong store/scope/history and missing/empty images
  reject. Invalid application schema fences before image publication/WAL. A
  committed checkpoint supersedes a divergent uncommitted learner tail; after
  joint activation late snapshot repair rejects without mutation.
- Native WAL: every byte of the exact snapshot binding record is torn, with sync
  and manifest faults. No successful reply escapes; model power loss and public
  member recovery yield old learner/application or complete checkpoint/joint.
  Existing shared snapshot/worker suites cover publication/pin faults and retained
  ownership; these checks do not prove real hardware power-loss semantics.
- Native TCP/TLS and QUIC nodes load actual pinned source files, repair via a
  stable pre-joint checkpoint or compacted committed joint, elect, commit/apply
  a client write, drain/join and reopen native files. Four new native histories
  pass. Initial membership is seeded, not distributed enrollment evidence.
- Explicit format 6 round-trip, every truncation, old-format refusal and narrow
  snapshot budgets pass. TLS versions 1–6 authenticate exact selections; 7 and
  5/6 mismatches refuse. Owning Node rejects missing/wrong selected rosters for
  both repair options before work. Existing bounded snapshot queues/connections
  are used; no new public provider seam (60 inventory contracts).
- Affected all-feature library, member_recovery, native_member_startup, snapshot,
  snapshot_worker, effect_owner, wire, secure, transport, startup, learners,
  outbound and runtime suites pass. Core-only library: 46 pass. Final
  member_recovery rerun: 29 pass, including divergent-tail recovery. Final
  all-feature/all-target Clippy with warnings denied, all-feature docs,
  formatting/diff and inventory metadata checks pass (60 contracts).
- Broader recursive/fork/candidate-tail/promoted-sender histories and the complete
  remote membership lifecycle remain open. General mutation ingress stays gated;
  P4 advances toward service integration and release, then P5 routing/P6 ownership
  movement/P7 tuning. Full P0–P7 remains active. No macOS/performance claim.

## Slice 69 — explicit member replication and recursive activation recovery

- The new actual-core recursive history failed at the default configuration
  receive gate after election. An explicit construction-time receiving mode
  resolves old-voter catch-up while preserving ordinary sender/journal/log and
  durable completion rules. Static/default gate tests remain. NativeMemberStartup
  selects the mode after exact member/route checks; Node refuses missing/old
  rosters before work. No new format, token, generation or provider seam.
- Actual-core nontrivial weighted/nested old/new policies require different nested
  two-voter subtrees. Learner repair, election, lagging old-voter catch-up,
  application commitment and joint read barrier pass through public step calls.
  Completion receipts in this fixture are host-asserted, not hardware evidence.
- Native TCP/TLS and QUIC three-store histories repair an 80-entry prefix, catch
  up another old voter, commit an uncommitted candidate command beyond the joint
  plus a new write, drain/join and reopen exact native state (application 18).
  This native nested one-leaf branch has ordinary two-voter Boolean semantics;
  nontrivial recursive semantics are exercised by the core history. Initial
  journal assignments are seeded; enrollment/proposal delivery is not proved.
- The fixture's identical election seeds initially caused lockstep elections;
  independent per-node seeds and an explicit initial campaign resolve it. Competing
  campaigns may produce intentional InvalidMessage/WrongIdentity ingress refusals
  for late repair/promoted candidates; these are allowed, but the test still
  requires eventual correct commitment/application/recovery and no storage/fencing
  error. A full target run exposed sequential shutdown closing an owned reply
  channel. Coordinated cluster quiescence/drain fixes that fixture cleanup race.
- Host/public tests cover old-voter default refusal, opted-in acceptance only after
  durable completion, foreign/new-only sender, store/context/scope and malformed
  journal rejection without durable mutation. Every byte of a native joint record
  is torn, plus sync/manifest faults; model power loss recovers old or complete
  joint, and retry completes. Membership snapshot reception releases normal ack
  only after application restoration, preserving Busy boundaries.
- Affected all-feature library 56, activation model 4, effect_owner 125, learners
  23 pass; final member_recovery rerun 32 pass including membership snapshots.
  Native member startup final rerun 22, replication_scope 7, snapshot 19,
  snapshot_worker 14 and startup 9 pass. Core-only library 47 pass. The independent
  activation model remains factored fixed-prefix evidence, not a complete model
  of this receiving mode or repair protocol.
- Promoted-leader/witness integration, divergent retained-only learner histories,
  broader failures and full remote lifecycle remain open. Public service mutation
  endpoints stay gated. P4 → P5 routing → P6 ownership movement → P7 and full P0–P7
  remain active. No macOS execution/performance or general protocol-proof claim.
- Final all-feature/all-target Clippy with warnings denied, all-feature docs,
  formatting/diff checks and inventory metadata verification pass (60 contracts).

## Slice 70 — owning witness controls and promoted-leader catch-up

- NodeControl exposes existing authority request/cancellation through bounded owner
  admission. No-peer and format-1/2 queries refuse before admission; closed nodes
  refuse both. Local volatile status reports existing request/permit data without
  becoming an authority token. Pending may coexist with a prior permit; cancel
  first when revocation is intended. Cancellation takes effect on execution.
- Four owning native TCP/TLS and QUIC histories cover retained and compacted
  promoted final-view sources. Cancel before reply, refuse the late grant, retry
  with a distinct request context, and verify the new grant changes no durable
  state or election reset. The promoted leader then transfers joint/final history
  or a pinned final snapshot, commits/applies a write and demotes the older voter
  to a learner. Coordinated drain/joins and all three file reopens recover exact
  membership/application and no volatile permit.
- A separate old-view witness retains the committed base needed to authorize the
  compacted leader. This adds no portable/Byzantine commitment certificate and
  cannot authorize catch-up if all witnesses lose the required history. The
  initial assignment records are seeded; these histories do not prove enrollment
  or the original distributed joint/final proposal commitment.
- Public host/native witness histories now assert pending/granted status and
  revocation on fencing/recovery. The format-2 facade fixture initially used
  incompatible default roster limits; copying its selected bounded limits fixes
  construction. Final affected runs: all-feature library 56, effect_owner 126,
  member_recovery 32, native_member_startup 26 and runtime 25 pass. Socket suites
  use local socket permission. The independent activation model is unchanged;
  finite histories are not a general fork/term/liveness proof.
- Mini plan advances to native enrollment/admin integration, then remote lifecycle
  release with remaining divergent retained-only learner and broader policy/fault
  gates, then P5 manifests/routing. Public service mutations stay gated. Full
  P0–P7 remains active; no macOS execution/performance claim and P8 stays deferred.
- Final core-only library 47 pass; all-feature/all-target Clippy with warnings
  denied, all-feature docs, formatting/diff and inventory metadata checks pass
  (60 contracts). No new provider seam or production dependency.

## Slice 71 — executable member recovery

- Explicit `serve recover-member` selects NativeMemberStartup and wire format 6
  for existing service stores among the original three provisioned identities.
  Static create/recover retain wire 1. Shared counter envelope and startup/shutdown
  ownership remain in use; ordinary polling denies configuration execution.
- Actual TCP/TLS and QUIC executable histories reopen seeded committed joint/final
  views, observe configuration operation 500, commit/read counter value 42, drain
  and verify native checkpoint compaction, restart all processes and preserve retry
  deduplication. The final learner cannot lead/accept a write. Static recovery
  refuses the dynamic journals and missing member files are not created.
- The initial parallel run failed before QUIC startup because clock-derived
  temporary directory names collided. A per-process atomic allocation sequence
  fixes that fixture issue; both corrected histories pass. Full all-feature
  counter_service target passes 9 tests, including existing static leader-loss,
  recovery, input bounds, quorum loss and absolute client deadline tests.
- All-feature/all-target Clippy with warnings denied passes. No new provider seam
  or production dependency. Seeded assignments do not establish online proposal
  commitment, enrollment or a complete remote lifecycle. Enrollment/deployment
  inputs and authorized administration remain mini item 1; remaining faults stay
  in release item 2 before P5 routing/P6 ownership movement/P7. Full goal remains
  active. No macOS execution, performance or complete protocol-proof claim.
- Default-build counter_service passes 8 tests, including rejection of unavailable
  QUIC before store creation. Formatting/diff and inventory metadata checks pass
  (60 contracts). No consensus/storage changes required broader protocol tests.

## Slice 72 — offline executable enrollment

- CLI `enroll create|recover` reuses the native learner import from an explicitly
  trusted stopped source. It verifies source identity/bootstrap, authoritative
  pinned image, application recovery/replay and current committed membership.
  Old membership snapshots cannot provision a learner removed in committed tail.
  Serving/source/destination use the same enforced counter-envelope constructor.
- TCP/TLS and QUIC process histories enroll an absent directory, repeat exact
  imports twice without changing WAL state, directly reopen the destination and
  verify counter 42 plus operation-700 duplicate outcome. They then start all three
  members, commit/read 43, checkpoint/drain, restart and retry operation 701.
- Negative CLI histories reject absent pins, missing target recovery files,
  voter/joint imports, changed-image replacement, same-directory source/target
  and stale snapshot assignment after durable learner removal. Create retries
  cannot reset an existing import. Existing checkpoint/pin/WAL durability and
  partial-initialization limitations remain; no new storage protocol is added.
- Isolated enrollment tests passed. The initial full parallel suite saw two
  fixture WouldBlock lock refusals immediately after parent-side seeding. Test
  native-handle lifetimes and process creation now share a short gate; child
  waits/execution remain parallel. The corrected final all-feature counter_service
  target passes 12 tests. All-feature/all-target Clippy with warnings denied passes.
- Source assignments are prepared committed histories, not distributed online
  configuration proposal proof. Provisioning stays fixed to the service's three
  identities; arbitrary deployment inputs and authorized administration remain
  current work before remote release, P5 routing, P6 ownership movement and P7.
  Full goal active, P8 deferred; no macOS/performance/general proof claim.
- Final default-build counter_service target passes 10 tests. Formatting/diff and
  inventory metadata checks pass (60 contracts); no new production dependency or
  provider seam. No consensus/storage design change required broader protocol runs.

## Slice 73 — explicit service deployment identities

- Versioned --deployment input supplies exact provisioned node/store/incarnation,
  numeric address and TLS name within the preserved original bootstrap. Header,
  shape, 64 KiB/1,024-entry limits, duplicate node/store-pair/socket, ID/incarnation,
  endpoint/name and local-entry checks run before store mutation. Native metadata
  loading enforces the 1 MiB retention bound incrementally. Static modes refuse
  the flag; legacy three-node defaults and exact member checks remain in use.
- TCP/TLS and QUIC executable histories enroll node 4/store 404/incarnation 7,
  directly verify imported counter/retry outcomes, repeat exact imports, reject
  changed incarnation without WAL rewrite, commit/observe new replication at the
  learner, reject its write and restart. The fixture's public alternative cert
  is assigned to node 4 only; retired node 3 is not provisioned. A valid declaration
  cannot recover a missing member. Source membership is prepared, not online
  distributed configuration proposal evidence or physical-domain validation.
- Initial full run passed all deployment cases but an existing reconnect test
  encountered Unknown LeadershipChanged after sampling a role. Its explicit caller
  retry now preserves operation 2/payload 3 within a deadline; exact value 10 and
  duplicate outcome remain required. Automatic client Unknown handling is unchanged.
  Corrected all-feature counter_service passes 15 tests; Clippy warnings-denied
  all-feature/all-target passes. Final checks below cover the last added missing-
  member negative case and default build.
- No new provider seam, core effect, wire/checkpoint/WAL format or production dependency.
  Placement-policy/service authorization and admin outcomes remain current work,
  before fault-tested remote membership release, P5 routing/P6 ownership movement
  and P7 tuning. Full goal active; no macOS, throughput or general proof claim.
- Final all-feature counter_service passes 15 tests, including missing-member
  refusal with valid credentials/declaration; default build passes 12. Inventory
  metadata checks pass (60 contracts), formatting/diff checks pass, and no protocol
  or storage design change required broader core test runs.
- Final focused invalid-deployment rerun also passes independent malformed TLS
  name and missing-field cases added after the full target run.

## Slice 74 — exact administration scope and native promotion

- NativeAdministrationPlan binds exact group/record/application envelope and
  selected host/native PlacementAuthorizer through the existing Node callback.
  Tests refuse changed operation/expected head/payload/envelope, placement failure,
  empty/duplicate/count/capacity/byte excess and invalid requirements, preserving
  all constructor inputs. No new effect, token, generation, watermark or protocol
  format is introduced. Application envelope enforcement remains a host obligation.
- Actual TCP/TLS and QUIC owners authenticate learner readiness, refuse withdrawn
  queued scope without durable mutation, resubmit/cancel observation, then really
  commit joint/final through old 1/3 and new 1/2 voters. Durable status/resumption
  drives finalization. They commit/apply counter 8, checkpoint and reopen all native
  files with final membership/completed operation and no volatile readiness token.
  The initial learner record is prepared; joint and final records are proposed
  through Node, not seeded. No full add/enroll/remove-fault release claim.
- Initial blanket event-success assertions failed on unadmitted WrongIdentity
  ingress in final view 12. Core response validation requires current scope; the
  fixture now permits only that refusal after membership transition and retains
  strict administrative/application/storage/error and committed-outcome checks.
- Complete native member target initially passes 28 and placement target 8;
  final reruns cover the last generic host-placement/public API changes. Native-
  only placement, all-target Clippy and metadata/docs checks are recorded below.
- Inventory extends existing callback/placement contracts. CLI plan parsing and
  authorized mutation adapter remain mini item 1, followed by fault-tested release
  and P5 routing/P6 ownership/P7. Full goal active, default service mutations denied,
  P8 deferred; no macOS/performance/general proof claim.
- A later full run exposed AddrInUse in an older QUIC fixture's probe/rebind
  handoff. Native fixture endpoints now remain unique for this test process's
  lifetime, preventing parallel reuse of released probes. Corrected final native
  member target passes 28 and placement passes 8. Native-only placement passes 8;
  all-feature/all-target Clippy warnings-denied and all-feature docs pass. Inventory
  metadata (60 contracts), formatting and diff checks pass.

## Slice 75 — trusted executable administration

The counter loads bounded trusted startup intent/placement files before opening
native resources, then drives existing Node authorization, authenticated readiness,
exact proposals, durable status and shutdown. No public command-port mutation API
is added. Current-term application commitment remains required. Owner cancellation
of a lost readiness round uses the existing volatile core event; it is neither
membership rollback nor a durable receipt.

Validation run:

- `cargo +stable test --locked --offline --all-features --test counter_service --test native_member_startup`: service 18 and native member 28 passed. The two new executable histories commit a real joint/final promotion from a prepared enrolled learner over TCP/QUIC, reopen all three files with the same startup plan, inspect exactly one joint/final and preserve counter retry state. QUIC uses a nested weighted policy; this is a fault-free concrete policy history, not full recursive fault coverage.
- The parser rejection test covers malformed header/64-KiB limit/empty plan, duplicate or unprovisioned replicas, duplicate voters/learners, zero weight, branch-count/depth overflow, invalid IDs/trailing fields, duplicate intent keys/intent-count ceiling and unsupported static startup modes before native store opening.
- Native promotion histories additionally cancel an observed readiness result through Node, execute the cancellation, then obtain fresh authenticated readiness before promotion. Closed cancellation rejects after shutdown starts.
- `cargo +stable test --locked --offline --no-default-features --features native,tls --test counter_service trusted_startup_plan_promotes_and_restarts_tcp`: one TCP-only assembly history passed.
- All-feature public docs built; inventory remains 60 implemented contracts with valid conformance paths. Final lint/format/check results are recorded below after completion.

During implementation, an attempted binary precheck called crate-private
Membership::validate_next; compilation rejected that call. The adapter now checks
its exact expected head and delegates complete journal validation to the existing
Node/core admission path. No core visibility or validation semantics changed.
Clippy also identified a nested cancellation condition; it was collapsed with no
behavior change.

Broader faulted add/enroll/promote/remove, deliberately dropped readiness replies,
unavailable/compacted witnesses, divergent retained-only tails, promoted-leader
failure schedules and macOS/separate-host execution remain outstanding. The
operator must preserve the original trusted plan; historical completed operation
identity does not compare a newly supplied payload. Full P0–P7 remains active.

Final slice-75 checks: all-feature/all-target Clippy passed with warnings denied;
format and diff checks passed; inventory passed at 60 contracts. After the final
retry classification change, both TCP/QUIC executable administration histories
passed again. After adding the shutdown assertion, both native promotion histories
passed again. All-feature docs and the TCP-only feature assembly passed as above.

## Slice 76 — executable-created full membership lifecycle

Two real-process histories start from the original three-node public bootstrap.
No membership record is seeded and no test directly mutates WAL history. Trusted
startup plans commit six operations: demote/remove node 3, add exact learner 4 at
store 404/incarnation 7, promote it, then demote/remove absent original voter 1.
The final stable configuration is 10 with voters 2/4 and no learners.

The source checkpoint is produced by the executable after learner assignment.
Stopped-source offline enrollment and identical-image retry preserve target state.
A post-import application write succeeds while target 4 remains offline, with
promotion still locally absent on the existing voters. Starting 4 then requires
catch-up and authenticated readiness before joint/final commitment. Original
voter 1 is killed abruptly after promotion and stays absent through retirement;
a successful new exact zero-delta operation on the survivor quorum is required,
so historical configuration completion alone cannot satisfy the failure check.
Final snapshots/reopen preserve exact membership and all six operation IDs while
both retired original routes are omitted. Application recovery and duplicate
operation 700 are checked throughout; a fresh operation after final reopen changes
Counter 42 to 43. QUIC promotion uses a majority-wrapped weighted 2/1/2 policy;
TCP uses ordinary majority. This is that concrete policy/fault schedule.

Validation: the all-feature counter_service suite passed all 20 tests. All-feature
all-target Clippy passed with warnings denied. Inventory shape/conformance paths
passed at 60 contracts. Stronger assertions that source configuration 5 and final
configuration 10, including their operation IDs, reside in actual pinned compacted
bases are verified in the final runs recorded below. TCP-only assembly validation
and final formatting/diff checks are also recorded below when complete.

The initial fixture completion condition was strengthened before final validation:
a post-failure stage now requires a successful fresh survivor-quorum write even
when its configuration operation was historically completed. An inventory-update
script initially used a guessed contract prefix and aborted before writing; the
actual inventory names were inspected and the corrected update passed validation.
No production change or protocol weakening was required.

Explicit maintenance stops remain part of offline enrollment and immutable plan
selection. These histories do not prove continuous online enrollment, arbitrary
partial joint/final delivery, dropped-readiness-reply recovery, divergent retained-
only learner-tail repair, unavailable/compacted witness liveness or broader
recursive-policy faults. Public mutation ingress remains gated. macOS/separate-
host validation and P5–P7 remain outstanding under the active full goal.

Final slice-76 runs: both all-feature TCP/QUIC lifecycle tests passed with the
pinned-base assertions; the TCP-only lifecycle passed with
`--no-default-features --features native,tls`. The full 20-test service suite
passed before that assertion strengthening; final targeted runs cover the changed
checks. Warnings-denied all-target Clippy, formatting, diff and inventory checks
passed. No additional provider, core or storage implementation changed.

## Slice 77 — divergent retained learner-tail repair

The initial native TCP regression failed with InvalidMessage at the first repair
batch. Its exact committed learner had an abandoned durable-but-uncommitted term-2
command suffix extending beyond the candidate's term-3 joint record. Retained
repair required identical overlap even there. The focused format-5/6 change now
permits different-term uncommitted command/noop replacement through ordinary
atomic suffix persistence. Committed entries, same-term payload forks, local
voters and accepted configuration work remain protected. Formats 2–4 are unchanged.

Validation completed:

- Four new native TCP/QUIC histories pass for weighted and recursive policies,
  repairing a 199-entry abandoned suffix in three batches before normal election,
  commitment, application and native-file recovery. Reopened files contain no
  abandoned command IDs; application values are 7/18 as expected.
- The all-feature member_recovery suite passed 33 tests, including the new native
  modeled-power-loss replacement test. Formats 5 and 6 each inject zero/short
  append, sync and before/after-publication failures. Failed transitions fence,
  expose no replacement prefix/reply, and recover exactly the old suffix with old
  hard state or the complete first repair batch with its new hard state. Both
  outcomes are required. Retry completes three batches with unchanged commit=1;
  only ordinary Vote resumes. This is modeled power loss plus native WAL, separate
  from the actual-file TCP/QUIC process histories.
- The all-feature native_member_startup suite passed all 32 tests after the change.
- Core-only library tests passed 48 tests, including a new protected committed-
  divergence/accepted-joint refusal test. Existing same-term fork and authority/
  range/cursor refusal tests pass. Core-only activation_model, learners,
  membership and raft suites passed 4, 13, 18 and 10 tests respectively.
- Inventory remains at 60 implemented contracts; this changes the existing core
  repair semantics, not its provider seams. Final lint/format and service
  regression results are recorded below after completion.

One fault-test compile initially lacked a qualified HardState type; it was
corrected to the existing public contracts type. No production API was added.

Held/dropped readiness replies across cancellation/session change, unavailable/
compacted witness plus promoted-leader failure, and broader recursive partial
joint/final delivery schedules remain release work. Public mutation ingress stays
gated. The full P0–P7 goal remains active; P5 routing over established groups can
proceed independently, while P6 movement retains its own and P4's release checks.

Final slice-77 checks: warnings-denied all-feature/all-target Clippy passed;
format/diff checks and inventory validation passed at 60 contracts. The full
20-test executable service suite passed against the changed core. The strengthened
native replacement fault test requiring both old/new outcomes passed for formats
5/6, and also passed in the native-only build without TLS. The full 33-test
member_recovery suite passed before that assertion strengthening; its changed
test passed afterward. The 32 native membership and 48 core library regressions,
plus the four core-only integration targets above, passed. README now distinguishes
working explicit member administration from the remaining public-ingress gate.

## Slice 78 — responsibility routing foundation

Implemented public immutable checked manifests, deterministic partition-policy
and volatile manifest-cache contracts, native byte-key/cache providers, bounded
recursive resolution and a separate local committed-owner context check. No
consensus/storage or persistent/wire format changes. See
[contract and limits](../docs/RESPONSIBILITY_ROUTING.md).

Actual checks:

- All-feature routing target: 11 passed. Native-only routing target: 11 passed.
  Core/host-only routing target: 7 passed. Every bucket in the 256-bucket universe
  resolves through native/host providers with the expected group. Tests include
  exact 32-visit success and 33-level refusal, composite local/delegated scopes,
  gaps/overlaps/ordering, malformed schemes/modes, exact parent/child bindings,
  wrong provider results, indirect cycles, epoch/incarnation/owner/scope/key
  refusal, admission-before/apply-after fixture fencing, generation conflict and
  stale invalidation, original allocation return and spare-capacity accounting.
- Native warm child lookup still resolves after parent cache invalidation; cold
  root lookup returns the missing identity. This checks cache independence only.
- All-feature library regressions: 57 passed with local UDP socket permission.
  Initial sandboxed run passed 56 and denied the existing QUIC socket test's bind
  with Operation not permitted; rerun with socket access passed all 57.
- All-feature/all-target Clippy with warnings denied, fmt/diff checks and inventory
  validation pass (62 public contract records). No new dependencies.

The first test compile found a harness function shadow and was fixed. Clippy's
large-error warning is narrowly allowed at the two owned rejection contracts so
failure returns original inputs without an extra box/allocation. No runtime
failure was suppressed or converted to success.

Evidence limits: manifests in these tests are trusted fixtures. No replicated
directory, directory codec/checkpoint/replay, routed Node command execution,
actual committed ownership fence, parent-quorum outage child-write history,
source export/import or target activation is established by these checks. Cache
hints do not grant authority; local owner checks still need admission/apply wiring.
Those remain the current P5 deliverable, followed by the explicit remaining P4
release-fault ledger and P6 split/merge. Full P0–P7 remains active, P8 deferred.
No new macOS or separate-host execution evidence.

## Slice 79 — replicated fixed-bootstrap directory application

Directory now implements the existing public application/admission/read/checkpoint
contracts. Fixed explicit grants plus bounded generation-CAS commands publish
routing metadata; owner/epoch/scope/topology/adapter/lifecycle changes reject.
A committed initialization command binds the whole plan and capacities before
publication, including during WAL-only recovery. Publication/manifest format 1
and checkpoint schema 1 retain complete original retry
content and reconstruct outcomes in first-application order. Local ancestry is
validated; external bootstrap grants require trusted source verification.

Actual evidence:

- All-feature directory target: 14 tests pass. Core-only target: 13 tests pass;
  native-only target: 14 pass. Tests cover atomic application and restore, original
  successful and failed outcomes after retries/replay, conflict retention, exact
  plan/capacity recovery, operation and pending-byte limits, complete command and
  checkpoint truncations, finite canonical byte mutations, maximum partition map,
  malformed checkpoint order/IDs/counts/lengths, local cycles/bindings, repeated
  external child delegation, foreign authority aliases, depth limits, independent
  external-grant child directory and inline/nested read-result credits.
- Native three-replica history commits through actual Raft and three filesystem
  WALs with bounded host-driven delivery. It first commits initialization and
  verifies pre-checkpoint recovery rejects altered child grants, altered capacities
  and even an unused added grant. It reopens retained logs after losing
  results, retries original publication, isolates the leader and verifies an
  appended command is neither applied nor acknowledged, elects survivors and
  publishes generation 2, checkpoints/compacts, reopens, installs a snapshot into
  the older replica, serves a fresh quorum read and reopens again. Dedup counts
  exclude the abandoned command; retry generation 1 is preserved alongside
  current manifest generation 2. No directory commands or membership states are
  fabricated as precommitted fixtures in that history.
- All-feature application regression target: 9 passed; routing: 11 passed;
  snapshot: 19 passed. All-feature/all-target warnings-denied Clippy, fmt/diff and
  inventory checks pass (63 records). No added dependencies or runtime resources.

The initial proposal admission compile failed on chaining references with
unrelated lifetimes; one shared reservation closure now processes both inputs
without changing their borrowing contract. The initial native harness omitted
creation of its temporary root directory; it now creates that directory before
opening stores. Both issues were corrected before the native history passed. Final schema review
also exposed the missing WAL-only bootstrap-plan binding; committed initialization
now fixes it and the native history exercises all three mismatch cases before any
checkpoint. Initialization is bounded to 8 MiB, publication to 32768 bytes, and the
actual whole-plan initialization size participates in the declared provider
envelope. The constructor requires enough history space for that binding.

These checks establish fixed-bootstrap replicated directory persistence and
snapshot recovery, not a dynamic creation/ownership protocol. Existing provider
crash/fault suites remain existing evidence; this slice did not run a new modeled
directory power-failure schedule. Routed Node data-command ownership checks,
actual durable child writes during parent quorum loss, TCP/QUIC directory ingress,
cross-authority ancestry protocol and source fence/import/activation remain work.
MacOS and separate-host execution are not established. The full P0–P7 goal remains
active, with P4's explicit fault-release gates and P6 lifecycle still required.

## Slice 80 — routed application and native parent-independent progress

Ten all-feature routed tests cover fixed committed bootstrap binding, context
rechecks at admission/apply, key/payload semantic retries, bounded pending/history
reservations, atomic invalid commands/batches/restores, local fencing of queued
data/reads and checkpoint truncations. A downstream host application supplies
allocated receipts, queries and results and restricts its own hosting group;
outer and nested bounds are checked independently. Wrong-group native startup
returns the fresh application before creating files/listeners/workers.

TCP/TLS and QUIC histories each exercise WAL-only and compacted checkpoint
recovery. Three real directory replicas publish a three-level forest and provide
quorum-backed grants. Orders/jobs use distinct {1,2}/{2,3} child voter sets.
After every parent role stops and ancestor cache entries are removed, children
commit, retry and serve fresh quorum reads, close/join, recover and return original
results. Changed grants and limits refuse recovery and clean up their failed
startup handles. Parent WAL state compares exactly unchanged after all child work.

The first network attempt exposed a missing caller-owned parent directory in the
test setup. QUIC then exposed two harness scheduling issues: a constant clock
prevented timed close from draining, and polling only parent roles during their
shutdown starved the still-live child roles into election timeouts. The harness
now uses one explicit advancing host clock and polls live children while others
drain. The resulting histories pass without modifying QUIC or election behavior.

Final checks (overlapping scopes, not unique-test totals):

- `cargo +stable test --locked --offline --all-features --test routed --test directory --test application --test routing --test snapshot --test snapshot_worker --test effect_owner --test startup`: 212 passed, including 10 routed tests and four native routing/recovery histories across TCP/QUIC and WAL/checkpoint modes.
- `cargo +stable test --locked --offline --no-default-features --test routed --test directory --test application --test snapshot`: 37 passed, including 7 routed tests.
- `cargo +stable test --locked --offline --no-default-features --features native --test routed --quiet`: 7 passed.
- `cargo +stable test --locked --offline --all-features --lib --quiet`: 57 passed.
- All-feature/all-target Clippy with `-D warnings`, formatting and diff checks pass. Inventory validation passes for 64 implemented contracts; it checks metadata shape/paths, not consensus correctness.

The source fence is local committed application state, not an import/activation
certificate. No new general lifecycle model, power-failure schedule, directory
wire endpoint, cross-authority protocol or split/merge implementation is claimed.
Existing provider fault evidence remains separate. These Linux loopback tests do
not establish macOS or separate-host operations. The mini plan proceeds to the
remaining P4 fault ledger, P6 lifecycle and P7 measurements; the full goal remains
active and CI stays background feedback.

## Slice 81 — owning readiness cancellation and session-change schedules

The downstream owning-Node test holds a readiness reply across cancellation,
delivers it before and during a fresh round, and checks no readiness/durable
change. A separate one-step schedule leaves another old-session reply retained
in ingress/runtime during disconnect/reconnect. A previously admitted promotion
with an old authenticated store session returns AuthenticationRequired before
persistence, with unchanged membership state and a still-running Node. A fresh
current-session proof succeeds and exact host-provider append acknowledgments
commit the joint record. Shutdown drains the fixture. Peer/session controls are
explicit downstream doubles; they are not remote storage or cryptographic proof.

Both actual native TCP/TLS and QUIC administrative histories now verify readiness
through the real learner snapshot worker, retain the sent reply in leader ingress,
queue cancellation before delivery, and require unchanged durable state. Fresh
readiness then supports the existing promotion, lost-receipt resumption, checkpoint
and actual-file recovery history. Native cancellation stays on one live store
binding; changed-session promotion is checked in the owning host assembly.

The initial host schedule attempted a zero consensus-step budget, which the
existing bounded runtime correctly rejects. It now uses normal polling and checks
refusal across disconnect/reconnect. The initial native QUIC schedule stopped
peer polling, preventing transport acknowledgments and send completion. The
corrected schedule pauses only ingress (budget zero), continues transport progress
and asserts that the reply is retained in actual leader ingress before cancellation.
No production guard, budget rule or QUIC behavior was weakened for these tests.

Final verification:

- All-feature `effect_owner` and `native_member_startup` suites: 127 + 32 = 159 passed.
- Core-only `effect_owner delayed_readiness`: 1 passed; no native/TLS dependency required for the owning host schedule.
- All-feature/all-target Clippy with warnings denied, formatting, diff checks and inventory validation pass. Inventory remains 64 contracts; no production seam is added.

This is finite selected-schedule coverage. It closes the named held-readiness
assembly gap while preserving the separate native/provider evidence boundaries.
Combined promoted-leader failure with older views and unavailable/restored/compacted
witnesses, plus broader recursive partial joint/final delivery/restart, remain P4
release requirements. Public mutation ingress remains gated; P6 lifecycle, P7
measurements and macOS/separate-host operational evidence remain outstanding.
The full P0–P7 goal remains active; P8 remains deferred.

## Slice 82 — native promoted leader loss and witness outage

Four owning native TCP/QUIC histories reuse seeded weighted membership records.
The promoted final-view leader acknowledges value 7 and closes before the old
view learns the transition. With both peers unavailable, the old view campaigns
but rejects a client write at admission, preserving the original request allocation;
its query never grants authority or changes the durable data/configuration prefix.
Cancellation and a fresh context precede restoration of the actual witness store.

A retained witness authorizes catch-up from the reopened compacted leader; its
acknowledged write survives and snapshot recovery plus a subsequent write reaches
value 9 on all replicas. A compacted witness refuses ingress without a grant;
missing historical base means no authenticated negative reply is emitted. Explicit
host cancellation clears the query, and the old view remains at configuration 10,
commit 1/value 0 while the current weighted group reaches configuration 12/value 9.
Every native file store is reopened after coordinated drain/join, preserving its
expected application/membership state and no volatile authorization. The existing
actual-core witness-compaction test separately identifies the exact request's
WrongIdentity refusal; native steps observe ingress refusal without exposing RPCs.

Initial harness failures revealed admission-time NotLeader rejection, candidate
role preceding ballot persistence, frozen time preventing reconnect, and an
incorrect expectation of a negative reply when historical authentication is
impossible. The harness was corrected to the existing contracts; no production
checks or quorum requirements changed.

Final verification:

- All-feature native startup 36 + membership 32 + core library 57 + member recovery 33 = 158 passing tests.
- All-target/all-feature Clippy with warnings denied, formatting, diff checks and contract inventory validation pass (64 contracts).

Coverage is finite and weighted. Broader recursive partial joint/final delivery
and restart remain the P4 release gate; public mutation ingress remains closed.
P6 lifecycle, P7 measurement and macOS/separate-host validation remain outstanding.

## Slice 83 — recursive partial joint/final restart

Eight native TCP/QUIC schedules broaden the finite P4 fault ledger:

- Partial joint: keep the required old voter absent while the native candidate
  repairs the learner across multiple retained batches, with and without a
  divergent uncommitted command suffix. Observe durable accepted joint with no
  pending dependency, unchanged commit 1/application 0 and no leader. Drain/join
  both survivors, reopen their files and require the same pre-commit state before
  restoring the old voter. Ordinary recursive quorum commitment then reaches
  application 18, and final file recovery excludes abandoned learner commands.
- Partial final: seed committed joint on all three stores but final on only 3.
  Old policy requires 1; new policy requires both members of a nested Majority(2,3)
  child. Keep 1 offline. Persist 3's campaign promise and restart it before any
  vote reply; recover 2 from WAL or a verified compacted joint checkpoint. Require
  a new-policy election, committed final and acknowledged application 7. Voter 2
  retains joint ballot origin despite echoing final request scope and subsequently
  catching up. Reopen survivors and the unchanged offline store to check exact
  respective application/membership state.

The joint driver initially applied a pre-repair non-voter assertion after restart,
when accepted joint correctly made the learner a voter. The assertion is now at
initial opening; the restart boundary explicitly requires durable accepted joint,
no pending dependency, no leader and unchanged commitment/application. A missing
closure type annotation was also corrected. No production checks were changed.

Final validation: 57 core + 4 activation model + 33 member recovery + 32 membership
+ 44 native startup = 170 tests pass with all features. All-target/all-feature
Clippy denies warnings; formatting, diff and inventory validation pass. Inventory
remains 64 contracts, with the downstream recursive module recorded as evidence.

These selected native schedules cover the remaining named slice-77 fault rows;
they do not prove arbitrary distributed traces. Transition prefixes are fixtures,
and graceful drain/reopen is distinct from the existing storage-fault histories.
General public mutation endpoints remain gated. Work advances through the existing
trusted administration path to P6 durable split/merge; P7 and the full P0–P7 goal
remain active, with macOS/separate-host evidence outstanding and P8 deferred.

## Slice 84 — scope application data adapter

New ScopeStateMachine/ScopeImage public contract and native BucketCounter<P>
provide deterministic bounded data export/import. Per-bucket counters avoid
pretending that the original indivisible global counter is splittable. Canonical
requests include key/delta/outbox; exact operation content and original outcomes
survive subrange export, target rebasing, later writes and checkpoints. Outbox
instructions have stable operation IDs, are never externally delivered in replay,
and are retained under explicit finite lifetime capacity.

Eight all-feature downstream tests cover split data partitioning, compatible scope
combination, history longer than target log, original retry outcomes, overflow,
outbox preservation, content conflicts, complete disjoint coverage, collision and
capacity refusal, atomic failed batches/imports, pending reservations, every
command/checkpoint truncation, changed metadata/scope/schema/boundaries, returned
policy/buffer allocations, query spare-capacity bounds and a host adapter with its
own schema/format. The host adapter uses the native application primitive but
implements the public scope/checkpoint seam without private access. This is
substitutability evidence, not an independent application algorithm comparison.

The native snapshot test imports data from source applied 100 at target index 1,
seals but does not publish, and reopens with no published snapshot. A subsequent
published image reopens and restores values/retries/outbox at target boundary 1.
This exercises actual data/manifest persistence. It does not establish a committed
Import, quorum receipt, source fence, ownership activation or power-failure model.
The lifecycle layer must persist source lineage and keep imported data non-serving.

Final validation:

- All-feature scope 8 + core 57 + application 9 + directory 14 + routed 10 = 98 passing tests.
- Core-only library 48 and scope 7 pass without native/TLS dependencies.
- All-target/all-feature Clippy with warnings denied, formatting, diff checks and inventory validation pass; inventory now contains 65 implemented contracts.

No Raft/WAL/wire format, durability token, generation, consensus effect or dependency
changed. New application formats are VBBCMD01/VBBCP001. Initial private-field
compile failures were corrected to public image getters. Review also corrected
receipt-vector capacity accounting, original policy return on construction failure,
query spare-capacity limits and collision classification. P6 lifecycle and
no-dual-owner histories remain pending; P7 and full P0–P7 stay active, with macOS
and separate-host operational evidence outstanding and P8 deferred.

## Slice 85 — intent journal recovery

Seven new directory contract tests cover bounded checked top-level split/merge
intents, exact source/generation matching, unchanged ownership, conflicting local
target reservations, original retry outcomes, lock reconstruction, every command/
checkpoint truncation, atomic rejection, read bounds and pending-iterator ceilings.
The existing directory suite gains one native three-replica real-file history:
commit intent, lose observation, leave one replica behind, checkpoint/compact,
reopen, elect, snapshot-catch-up, retry, quorum-read and reopen again. Original
intent index and source manifest survive. Message delivery is host-driven in
process; this adds no TCP/QUIC or power-failure evidence.

All-feature library 57 + directory 22 + scopes 8 + routed 10 = 97 passing tests.
Core-only library 48 + directory 20 = 68 passing tests. All-target/all-feature
Clippy with warnings denied passes. Initial Clippy failures were confined to
bounded enum layout and repeat iterator style, corrected before the passing run.
Inventory records 66 contracts. Formatting, inventory shape/path and diff checks
are recorded separately from behavioral conformance.

An intent is not a source fence, target import receipt or ownership certificate.
No source data moves and no owner changes in these histories. Local target
reservations do not establish global placement authority. Delegated-parent epoch
coordination, lifecycle cancellation/resumption, data publication/activation and
full no-dual-owner histories remain pending under P6. Old implementations reject
the new application command; no mixed-version deployment is validated. Full P0–P7
remains active, including macOS/separate-host evidence; P8 is deferred.

## Slice 86 — source freeze recovery over native TCP and QUIC

Seven new source conformance tests validate exact intent binding, irreversible
fence, immutable F-state while the wrapper applied prefix advances, same-batch
ordering, original data/retry/outbox after adapter import, provider lifetime export
bounds, envelope/payload key matching, original construction resources, every
freeze/checkpoint truncation, atomic refusal, pending ceilings and status/read
capacity accounting. Source status and ordinary data rejection are distinct query
results; the latter never serves old-epoch data after the fence.

Four three-node native histories use actual TCP/TLS or QUIC and durable files,
with WAL-only or checkpoint/compaction recovery. They discard the initial success
observation, reopen, elect, retry the original operation, verify original fence
and byte-identical exports despite later log progress, perform quorum-backed
status/data queries, and reopen again. Exported target data retains original
counter result and outbox after local adapter import. This is selected source-side
network/recovery evidence. It does not establish committed target import/readiness,
metadata publication, activation, no-dual-owner transfer completion or power-loss
behavior. Existing storage-fault/core evidence is complementary.

All-feature core 57 + directory 22 + scopes 8 + routed 14 + source 7 = 108 tests
pass. Core-only library 48 + scopes 7 + source 7 = 62 pass. All-target/all-feature
Clippy with warnings denied, formatting, diff and inventory shape/path checks pass;
inventory now has 67 contracts. Compile/test-harness mistakes (range Result mapping
and core state accessor) and a test clone style warning were fixed before passing.
No new consensus effect, persistence token, transport/WAL format or dependency is
introduced. Scope capability version 2 adds an explicit provider lifetime bound;
new source application formats are separately documented. P6 target/import/
publication/activation, recursive lifecycle and full P0–P7 remain active; macOS/
separate-host operational evidence remains outstanding and P8 is deferred.

## Slice 87 — non-serving target import and source content binding

Nine target conformance tests exercise staging, original phase retries/status,
canonical full-data imports, SHA-256 standard vector, source-observed metadata/
content commitment matching, ordered complete multi-source coverage, duplicate
operation-ID refusal, returned inputs, provider capacity, atomic truncation/batch/
restore failures, WAL replay/checkpoints, command limits and read capacity. Source
status now derives bounded per-target image commitments from exact F-state; its
checkpoint format is unchanged. Hashing reuses pinned ring as a direct dependency.

Four actual native network histories use TCP/TLS or QUIC with three replicas per
source/target group. Both split targets stage before source fencing; one imports
against a quorum-backed source commitment while the other stays staged. WAL-only
or checkpoint/compaction recovery, lost application observation, re-election,
repeated reopen, original digest/index status, recovered values/outbox, quorum
NotActive reads and synchronous inactive-write refusal are checked. Import carries
actual image bytes through the existing durable WAL quorum; no digest-only data
availability claim is made. The source's F is retained separately from target I.

All-feature core 57 + directory 22 + scopes 8 + source 7 + target 9 + routed 18 =
121 tests pass. Core-only library 48 + scopes 7 + source 7 + target 9 = 71 pass.
Final target/routed suites and all-target/all-feature Clippy pass after envelope/
two-target-stage refinements; core-only target passes again. Formatting, diff and
inventory checks pass; inventory has 68 contracts. Initial harness failures assumed
asynchronous rejection instead of the facade's correct synchronous refusal; later
fixture-shadowing, module-sharing and helper-name errors were fixed before passing.

This is selected local phase/network/recovery evidence. It does not establish
metadata publication, activation, complete distributed merge, whole-transfer
no-dual-owner proof, power-loss behavior, or macOS/separate-host deployment.
Foreign provenance/configuration validation and group-creation/preflight orchestration
remain trusted-host duties and required work for the complete lifecycle. Inline
imports have explicit 8 MiB body ceilings and selected native envelopes may be
smaller. Further transfers/retirement, recursive parent coordination and full P0–P7
remain unfinished; P8 stays deferred.

## Slice 88 — checked durable transfer publication

Seven publication conformance tests cover complete ordered source/target evidence,
changed/missing facts, ordinary operation/byte exhaustion with reserved control
capacity, pending competitors, original retries, bounded quorum-read values,
atomic checkpoint recovery and every codec/checkpoint truncation. Maximum
256-route split and 256-source merge metadata encode to 63,624 and 61,584 bytes;
these synthetic envelope tests do not establish distributed merge behavior.

Four new native histories run three-replica metadata, source and two target groups
over TCP/TLS or QUIC. Targets stage/import actual per-bucket data; quorum-observed
fence and import commitments feed a checked publication. The fenced source stops,
ordinary directory operation slots are full, and the committed original decision
survives discarded observation, WAL/checkpoint reopen, retry and another reopen.
Targets continue to refuse data. No activation or complete no-dual-owner split
claim follows from this evidence. Foreign quorum provenance is trusted authenticated
host input, not a cryptographic certificate derived from the evidence structs.

Final affected all-feature suites pass: library 57, directory 22, routed 22,
scopes 8, source 7, target 9, publication 7 (132). Core-only library 48, directory
20, scopes 7, source 7, target 9, publication 7 pass (98). All-target/all-feature
Clippy with warnings denied, formatting, 69-contract inventory and diff checks pass.

Intermediate native failures exposed harness assumptions about persistent node-1
leadership, current-term read readiness, competing elections and cleared read
cancellations. Helpers now observe the actual current-term-ready leader. Publication
histories bound exact-ID/bytes retries and fresh quorum-read retries to four attempts
for leadership uncertainty only. ReadRequests already validates tracked StaleRead
cancellation after term changes; other unexpected step/application/owner errors fail.
Independent native histories are serialized within the binary to bound unrelated
worker/socket contention; each history retains concurrent replicas/groups and
unchanged production deadlines. The final full routed run passes all 22 tests.
These selected graceful-reopen Linux schedules are not power-cut, macOS,
separate-host, activation, distributed merge or complete protocol proofs. P0–P7
remains active and P8 deferred. CI remains background feedback.

## Slice 89 — durable target activation and independent service

Eight deterministic downstream activation tests cover exact local import/decision
matching, inactive/new-epoch refusal, reserved lifecycle identity, original
activation retries and conflicting bytes/configuration, preserved imported data
results/outbox, routes and payload/envelope keys, ordered pending phases, atomic
activation/checkpoint truncation refusal, WAL replay and old inactive checkpoint
compatibility. A downstream host provider has non-Copy receipts and nested read
results whose receipt bound changes after import; same-batch import/activate/data
accounting charges those buffers. Full 32-operation imported history permits
activation and an original retry while refusing new data IDs.

Four additional real TCP/TLS/QUIC histories activate one imported three-replica
target against the actual recovered metadata publication. Another target remains
inactive. Metadata stops before ordinary writes. WAL-only or checkpoint reopen
preserves first activation identity, values, imported/new operation retries and
outbox. The old source reopens from its WAL and refuses old-owner writes/quorum
reads while the activated target serves. Independent native histories use the
existing test mutex; replicas/groups remain concurrent within each history.

Final affected all-feature suites pass: library 57, directory 22, scopes 8,
source 7, target 9, publication 7, activation 8 and routed 26 (144). Core-only
library 48, directory 20, scopes 7, source 7, target 9, publication 7 and activation
8 pass (106). All-target/all-feature Clippy with warnings denied, formatting,
69-contract inventory and diff checks pass. A clarity rename typo was rejected
by compilation; focused target/activation/publication checks pass after correction.

Activation uses bounded VBTACT01 records and VBTRGT02 checkpoints, explicitly
reading old inactive VBTRGT01. There is no mixed-version deployment claim. Foreign
quorum provenance/configuration remains authenticated trusted host input; structural
evidence alone grants no authority. Only committed ordered local activation enables
service. Selected graceful Linux reopen schedules do not prove power-loss,
every interrupted split stage, distributed merge, recursive lifecycle/retirement,
macOS or separate-host operation. Those remain in the active P0–P7 scope; P8 is
deferred and CI remains background feedback.

## Slice 90 — every committed split phase reopened and resumed

Four new native histories compose actual three-replica metadata, source and two
target groups over TCP/TLS or QUIC. Each closes/joins and reopens all groups after
nine individual committed boundaries: intent, each target's stage, source fence,
each target's import, metadata publication and each target's activation. WAL-only
and checkpoint variants use the same public application/storage/read contracts.
The stateless trusted test host reconstructs one next action from fresh quorum
observations on each invocation. Action receipts are discarded; expected retained
statuses are comparison oracles only, not the input to resumption.

At every phase before/after reopen, source/target quorum reads check serving rights
and inactive/fenced data admission refuses. A partially activated split serves one
disjoint scope while the other remains inactive; source never thaws after fencing.
The original intent, fence/export commitments, import lineage, publication decision
and activation facts remain identical through restart. After both targets activate,
metadata/source workers stop before original-operation retries and new child
writes. Another reopen preserves values, retry behavior, outbox and original phase
identities; old source/routes stay fenced/rejected. The exact phase trace ends in
Done without executing any phase twice. See docs/SPLIT_RECOVERY.md.

Executed: all four `split_resumes` all-feature native tests pass (202.19 seconds
in the selected run); core-only routed tests pass 7/7; all-target/all-feature
Clippy with warnings denied, formatting, 69-contract inventory and diff checks
pass. Existing unaffected tests were not repeated. Phase diagnostic printing was
added afterward without changing assertions or behavior. No production protocol,
format, dependency, timer or quorum changed.

These finite histories interrupt after commitment and graceful owner drain. They
do not cut power, roll back uncommitted phase tails, drop partial replication,
partition a quorum or establish arbitrary-fault liveness. They do not implement
an autonomous resumer, delegated-parent lifecycle, later-transfer/retirement,
distributed merge or macOS/separate-host evidence. Existing storage/core fault
coverage remains separate. The selected first split recovery path is covered;
compatible merge is current, then reusable recursive ownership and retirement,
then P7 measured tuning. Full P0–P7 remains active and P8 deferred. CI is background
feedback, never a required merge gate.

## Slice 91 — compatible two-source merge recovery

Three downstream merge tests pass with all features and core-only. Actual source
images combine through target import, checked publication, activation and
checkpoint recovery with original results/outbox. Missing source coverage refuses;
conflicting source operation IDs reject atomically without target mutation or
readiness.

Six native TCP/TLS/QUIC histories pass (249.92 seconds): four complete WAL/checkpoint
variants reopen all four three-replica groups after seven committed phases, and
two conflicting-ID WAL variants remain safely inactive after refusal/reopen.
The complete histories stop the second source after the first fence, distinguish
Unavailable from an observed absent fence, preserve the first immutable export,
and include an old-epoch write before the recovered second source freezes.

Review found an unnecessary old-source availability dependency in the test host
after publication. The production activation guard already accepts the retained
decision and local import. After correcting host ordering, all four complete
histories pass again (184.73 seconds) with both sources stopped before activation.
Target reads/retries/new writes work with metadata/sources offline; another reopen
retains publication/import/activation and both source fences. Collision histories
were unchanged and not repeated. Three deterministic tests pass again in both
feature configurations. All-target/all-feature Clippy, formatting, 69-contract
inventory and diff checks pass. A test-only non-Copy array compilation error was
corrected with an iterator assertion. No production protocol/format/dependency
changed; unaffected suites were not rerun.

These selected graceful committed-boundary Linux histories use ordinary three-voter
quorums and trusted foreign provenance. They do not establish power-loss or
arbitrary-fault liveness, repeated movement of activated targets, recursive parent
coordination, retirement, macOS or separate-host behavior. See
docs/MERGE_RECOVERY.md. Reusable movement/retirement is current, delegated-parent
coordination next, then P7 measured tuning. Full P0–P7 stays active; P8 deferred.

## Slice 92 — activated targets reused as later sources

Target guards now commit a later checked source freeze through the same application
and authoritative log. The original import/activation remains immutable; the
provider stays at F while the wrapper applies later no-ops/refusals. Actual bounded
exports and quorum-readable source status feed the existing next import/publication/
activation contracts. Scope contract 3 exposes retained/imported operation presence
so control IDs cannot hide data retries. Import collisions refuse atomically.
New VBTRGT03/application schema 2 checkpoints retain both boundaries and the later
fence; schema-1 inactive VBTRGT01 and active VBTRGT02 remain readable, with explicit
schema/tag mismatch refusal. No mixed-version claim.

Five downstream tests pass: split -> merge -> split with original results/outbox,
exact freeze retry, pending/same-batch transitions, full-history control capacity,
ID/manifest/budget/binding refusal, all freeze/checkpoint truncations, corrupted
boundary/ID/bootstrap refusal and checkpoint/replay. Legacy active/inactive and
host-provider operation-presence/accounting tests also pass.

Four final native histories pass (271.63 seconds), using TCP/TLS or QUIC and WAL
or checkpoint recovery. After a real split and writes to both activated children,
the same guards/logs become merge sources. All five three-replica groups reopen
after each of seven later committed phases. Fresh quorum observations reconstruct
the next action; discarded action receipts do not lose original import/activation,
new F/export commitments, publication or activation. Frozen provider boundaries
remain F through subsequent wrapper progress. The merged owner serves original
retries and new writes with all old groups/metadata stopped; another reopen retains
values/outbox while every old source refuses. Native composition is split -> merge;
the further split is deterministic downstream evidence.

Affected all-feature suites pass: library 57, scopes 8, target 9, activation 9,
source 7, publication 7, merge 3, repeat 5. Core-only equivalents pass 48, 7, 9, 9,
7, 7, 3, 5. All-target/all-feature Clippy with warnings denied, formatting,
69-contract inventory and diff checks pass. The initial native matrix passed
before the schema-label refinement (273.24 seconds); final code ran all four again,
not eight unique histories. Stale schema/format assertions and test-only directory
imports were corrected. The library's existing QUIC socket test initially lacked
sandbox permission and passes with that permission. Unaffected native suites were
not rerun. No consensus timer/quorum, store binding or dependency changed.

Source data remains retained; retirement/reclamation and delegated-parent lifecycle
remain unimplemented. These finite Linux ordinary-majority committed-boundary
reopens are not power-loss, arbitrary-fault liveness, macOS or separate-host proof.
Foreign provenance/configuration remains trusted authenticated host input. Current
work is durable retirement, next recursive coordination, then measured P7 tuning.
Full P0–P7 remains active; P8 deferred. See docs/REPEATED_TRANSFERS.md.

## Slice 93 — old-owner retirement and recovery before reclamation

The new initial-construction RetirementGuard uses the same authoritative source
or target application/log. Complete matching publication, all target activation
observations and explicit trusted host release of external retention promises
gate committed retirement. It drops actual old provider/import buffers and retains
source fences/commitments, decision/release and original target activation lineage.
No external-pin discovery or migration of unwrapped deployments is implemented.

Seven all-feature retirement tests pass: five deterministic/host-provider cases
and two native-file histories. Both initial and later activated sources exercise
sealed-but-unpublished, pre-publication failure and post-publication lost completion
(six interruption histories). Recovery follows the old log-pinned snapshot plus
retirement tail; verified retired checkpoint publication/installation then permits
pin release and physical log reclamation. Fresh reopen needs no old provider or
retained command entries. These are real-file interrupted publication checks,
not whole-machine power-loss or secure-erasure evidence. The injected provider's
live-resource weak witness survives admission/failed batches and expires only
after actual retirement; retired checkpoint restore has no live owner.

Four native network histories pass (28.37 seconds): TCP/TLS and QUIC, each WAL and
checkpoint/reclaimed-log reopen. Four actual three-replica groups commit metadata
intent, source freeze, both imports, publication and both activations before
retiring the original source. Lost client completion is recovered via quorum
status; original fences and exact retirement retries survive reopen. A target
serves original data retries with the source offline. Later activated-source
retirement currently has deterministic/native-file coverage only.

Affected all-feature retirement/scopes/activation/publication/repeat/source/target/
log-reclamation suites pass 7/8/9/7/5/7/9/8. Core-only equivalents excluding native
log reclamation pass 5/7/9/7/5/7/9. All-target/all-feature Clippy with warnings denied
passes. Native routing caught an overallocated receipt vector; exact command-count
capacity and a direct capacity assertion fix the declared-bound violation. Test
setup/import/order/limit errors were corrected with focused reruns. Unaffected
native histories were not repeated. No storage/consensus protocol changed.

These selected Linux ordinary-majority histories do not establish arbitrary-fault
liveness, mixed-version deployment, macOS or separate-host support. Foreign
quorum provenance and retention promises remain authenticated host obligations.
Recursive lifecycle and broader P7 work remain; full P0–P7 stays active.
Formatting, diff checks and the 70-contract inventory validator pass.

## Slice 94 — reserved delegated-child epoch publication

The existing Directory journal now retains parent reservation and completion
records. A checked parent/child plan locks one parent generation and reserves
final publication history before source fencing. Contract-2 VBTINT02 binds exact
child manifests/operation to parent digest/generation and reservation provenance;
decode recomputes the 120-byte binding's digest. Top-level VBTINT01 is unchanged.
Source/target/import/publication operation gates and checkpoint restore enforce
the child binding. Parent completion requires the exact reservation and matching
child decision; same-group metadata checks its actual retained local decision.
Only the child route epoch and parent route generation change, keeping the parent
ownership epoch and grandparent locator stable. No provider/worker/store was added.

Five downstream deterministic tests pass in both core-only and all-feature builds.
They use actual provider exports, final imports, child publication and activation;
verify source refusal, original target retry and new warm writes without parent
access; restore parent/child checkpoints and replay the exact parent log; test
three-level cold routing, same-group metadata provenance, original outcomes,
stale/concurrent/wrong-context refusal, ordinary operation/byte exhaustion and
competing pending completions. Every new codec/checkpoint truncation refuses;
one low-bit flip at each byte position of a bound intent also refuses atomically.

Affected all-feature directory/retirement/routing/activation/publication/repeat/
source/target suites pass 22/7/11/9/7/5/7/9. Core-only equivalents pass
20/5/7/9/7/5/7/9. Existing TCP/QUIC original-source retirement WAL/checkpoint matrix
passes 4/4 in 27.15 seconds, validating enlarged query/accounting composition and
top-level compatibility. It does not test delegated network recovery. Focused
setup fixes corrected cache-limit construction and receipt outcome names; review
added the child-content digest before finalizing VBTINT02 and retained exact
top-level 64 KiB reservation accounting. No consensus/storage protocol changed.

Foreign reservation/configuration/child-decision provenance remains authenticated
host input. Native delegated phase/reopen, delegated merge/repeated movement,
abandoned pre-fence reservation cancellation/replanning and broader faults remain
unverified or unimplemented. Concurrent remote child metadata changes must be
detected before fencing; no timeout/unfreeze bypass exists. No mixed-version,
macOS or separate-host claim. Full P0–P7 remains active; P8 deferred.
All-target/all-feature Clippy with warnings denied, formatting, diff checks and
the 71-contract inventory validator pass.
## Slice 95 — native delegated split recovery and parent cache refresh

Native tests use separate three-replica grandparent, parent, child metadata,
source and two target groups. Targets bind the actual quorum-observed parent
reservation index/configuration. The resumer discards action completions and
reads committed phase status after reopening all established groups. It checks
11 transitions, immutable original decisions, source fencing, target refusal
before activation, unchanged grandparent locator and the temporary stale-parent
routing gap. Additional parent outages exercise source retries before fencing
and forward-only refusal after child publication. Finally, original target
retries and new writes succeed while all ancestor/source workers are stopped,
then their results survive another reopen.

The native cache needed a narrow compatibility fix: at unchanged parent ownership
epoch, accept an authenticated newer generation that changes only child locator
epochs, retaining ranges, child identities and groups. Child epochs cannot regress.
The public manifest predicate supports the same decision for host caches. Tests
check skipped intermediate epochs, changed ownership/ancestry/authority/state/
scope/shape/child bindings, and atomic native cache rejection. No durable format,
quorum policy, transport provider or production timer changed.

Focused validation: all-feature and native-only delegation 7/7, routing 11/11;
core-only delegation 6/6, routing 7/7. All-target/all-feature Clippy passes with
warnings denied. Formatting, diff checks and the 71-contract inventory pass.
`cargo +stable test --locked --offline --all-features --test routed native::delegation -- --nocapture`: 4/4 passed in 428.83 seconds. This covers TCP/TLS and QUIC, each with WAL-only and checkpoint recovery. The shared native-history guard serializes these cases. A preliminary TCP/WAL case also passed in 56.23 seconds; it is not an additional distinct history.

These are selected Linux ordinary-majority process-reopen histories, not arbitrary
power-loss/fault completeness, delegated merge/repeated movement, abandoned
reservation cancellation, macOS or separate-host validation. Those limitations
remain in the active baseline scope; P8 research remains deferred.

## Slice 96 — delegated merge and repeated movement composition

The downstream test in tests/delegation/repeat.rs performs an actual delegated split 20 -> 21/22, merge 21/22 -> 23, and split 23 -> 24/25. Later sources are the actual prior activated targets. Real exports/imports carry data, original retries and outbox history; child decisions and immediate-parent completions come from retained journal state. Parent/child journals recover after later actions; targets restore into fresh unstaged applications derived from original child intents at staging/import/activation/fencing and final writes. Exact freeze/completion retries retain outcomes, stale source intents and wrong bound operation IDs refuse, old owners stay fenced, final values are 10/16 with exactly three outbox records each. Parent generation reaches 4 with ownership epoch 1; grandparent locator remains unchanged. Cold recursive and child-only warm routing agree.

All-feature delegation 8/8 and repeat 5/5 pass; core-only 7/7 and 5/5 pass. This is deterministic public-contract application composition, not native delegated merge/repeated recovery. No production code, protocol, format or dependency changed. The four native split cases from slice 95 remain the network evidence; native repeated movement, abandoned reservation recovery and broader platform/fault/performance gaps remain active.
## Slice 97 — native delegated repeated movement

`tests/routed/delegation_repeat.rs` composes an actual split 20 -> 21/22,
merge 21/22 -> 23, and further split 23 -> 24/25 with distinct grandparent,
parent and child metadata groups. All groups have three ordinary-majority
replicas. The two later moves have 20 committed phases: target staging, each
source fence, each import, child/parent publication and target activation, plus
reservation/intent. Each action completion is discarded and every created
group is reopened before the next action is chosen from fresh quorum status.
Original retained intents reconstruct each later target's immutable bootstrap.

Checks include partial merge fencing, parent outage after each child publication,
unchanged grandparent locator/parent ownership epoch, refreshed immediate-parent
cache, target refusal before activation, permanently fenced old owners, and
child-only resolved final retries/new writes while all metadata and old owners
are stopped. Final values 10/16 survive another reopen. Metadata ordinary slots
increase only in this test assembly from 3 to 10 for three lifecycle operations;
no production protocol, timer, dependency or durable format changed.

TCP/TLS WAL case: 1/1 passed in 225.92 seconds. All-target/all-feature Clippy,
formatting, diff checks and the 71-contract inventory pass. The remaining TCP/TLS checkpoint and QUIC WAL/checkpoint cases passed 3/3 in 1106.42 seconds under the shared native-history serialization guard. Together with the separately run TCP/TLS WAL case, all four selected cases passed. No case was restarted on an observation timeout. These results predate slice-98 decline/cancellation protocol changes.
Selected Linux process-reopen histories do not establish arbitrary power-loss/
fault completeness, mixed-version behavior, macOS or separate-host operation.

### Slice 98 — permanent child refusal and parent cancellation

Six new deterministic public-contract tests in tests/delegation/cancellation.rs
exercise both ordered intent/refusal races (including refusal after completed
publication), same-group actual retained provenance and changed configuration/
operation/index rejection, original receipt recovery, full ordinary parent history
with reserved final credit and competing pending finals, child history exhaustion,
failed old intent, bounded query accounting and every new codec/checkpoint
truncation. A fresh compatible split 202 after cancelled 200 uses real source
images/import/publication/activation and preserves original data retries and new
writes. Cancellation never changes manifest generations or revokes a successful
intent; authenticated foreign commitment remains a host obligation.

Affected all-feature suites pass: delegation 14/14, directory 22/22, retirement
7/7 and transfer_repeat 5/5. Core-only counterparts pass 13/13, 20/20, 5/5 and
5/5. Native-only (without TLS/QUIC) counterparts also pass 14/22/7/5.
All-target/all-feature Clippy with warnings denied passes after adding the
native acceptance assembly. Inventory still contains 71 existing contracts; the
new recovery messages extend the delegation contract rather than adding a provider.
Native command:
`cargo +stable test --locked --offline --all-features --test routed cancellation -- --nocapture`
passes 2/2 in 227.64 seconds under the shared native-history guard: TCP/TLS with
WAL-only recovery and QUIC with checkpoint recovery. Each uses actual three-replica
parent/child/source/target groups, actual retained reservation/configuration facts,
lost-receipt restart after refusal/cancellation and every fresh transfer phase.
Parent outage before cancellation leaves the source serving; original intent 200
stays refused; committed fresh intent 202 refuses cancellation even before source
fencing. Parent outage after child publication keeps source fenced and targets
inactive. Final imported retries and values 9/13 survive further reopen.

This selected pair is not the full transport/storage cross-product, arbitrary
fault liveness, macOS/separate-host or mixed-version evidence. Earlier slice-97
native results used the preceding protocol revision; they were not rerun here.

### Slice 99 — native performance harness and explicit startup timing

Added release-mode examples/native_benchmark.rs through public native Node,
Counter, WAL and TCP/TLS/QUIC startup contracts. It retains raw dispatch/completion
CSV, useful-write receipts and summaries only after quorum read, all-replica
applied/value verification, full worker joins, actual WAL reopen and exact original
first/last retry outcomes with unchanged final data. Admission/unknown outcomes
inside warm-up or measurement invalidate a run. Recovery-only identical retries
are explicitly counted. Inputs/outstanding work/history are bounded.

Default-timer attempts exposed uncertainty: first QUIC window-32 attempt had an
insufficiently diagnosed non-applied outcome; the second completed measurement but
failed recovery retry with leadership change; longer TCP serial failed measured
operation 123 with leadership change. No result from those failed attempts is
counted. Two successful preliminary default-timer window-32 CSVs are retained,
with their distinct harness revisions identified, without controlled improvement
claims.

NativeStartup and NativeMemberStartup now expose open_with_protocol_and_timers
using the existing TimerConfig contract. Pure validity is shared with TimedShard;
initial election overflow rejects before binding/files. Existing wrappers keep
their original defaults. No quorum/authority/storage dependency or protocol changes.
The final four sequential equal-count runs declare 50 ms heartbeat, 1000–1999 ms
elections and default queue/poll budgets: TCP windows 1/32, QUIC windows 1/32,
64 warm-up plus 256 measured useful writes. All four verify recovered value 320,
original retries and worker joins, with zero recovery retries. Applied ops/s are
8.902/36.242/7.156/25.136 respectively; p99 milliseconds are
227.608/1558.050/546.170/1942.021. Raw samples, exact commands, source/binary hashes,
compiler/dependencies, machine, filesystem/device/firmware and limitations are in
validation/performance/slice99 and docs/PERFORMANCE.md. Independent CSV checks
confirm IDs, indices, values, latency arithmetic and summary agreement.

All-feature startup 10/10 and runtime 25/25 pass, including explicit TCP/QUIC
invalid timing/no-side-effect and host-relative deadline checks. Native member
learner/joint/final/checkpoint recovery with explicit timing passes 2/2. Final
all-target/all-feature Clippy with warnings denied passes. This is a finite
single-host closed-loop baseline, not sustainable throughput, fixed-p99 tuning,
maintenance/open-loop/multi-group scaling, macOS or separate-host evidence. Full
P0–P7 stays active; P8 remains deferred.

Core-only runtime tests also pass 18/18, and the benchmark compiles with TLS
without QUIC. Formatting, diff checks and the 71-contract inventory validator pass.

### Slice 100 — WAL barrier and native host attribution

Added examples/wal_benchmark.rs through the existing public JournalIo seam. Its
observer forwards real FileLogIo operations and times append, WAL synchronization
and manifest publication separately, alongside complete native append/barrier time.
Exclusive fresh roots, bounded inputs/history/samples, actual barrier tickets,
exact acknowledged GroupLog reopen, fresh Counter replay and historical first/last
retries gate every result. This is local B1 storage work, never quorum throughput.

Two 64-batch runs (eight additional warm-up batches) pass: one entry/batch yields
64 measured records, 1.950159 s and 32.818 local durable records/s; 32 entries/batch
yields 2048 records, 2.354276 s and 869.907 local records/s. Total barriers are
1948.316/2342.797 ms, WAL sync 692.377/726.713 ms, manifest publication
1255.062/1612.872 ms, p99 barriers 69.309/89.405 ms. Publication dominates these
local barrier costs, but remains required by the unchanged acknowledged-prefix
corruption/recovery contract. No synchronization/publication was removed.

Extended the existing native benchmark with measured-phase totals from NodeProgress:
persistence batches, worker events, apply deliveries and host poll wall durations.
Four sequential TCP/QUIC window-1/32 runs with 256 measured writes and unchanged
50 ms heartbeat/1000–1999 ms elections pass full reopen/value/retry/worker joins,
recovered value 320 and zero recovery retries. Applied ops/s are
7.106/26.125/7.498/27.581; p99 ms are 615.773/2369.064/513.127/1780.343.
Serial runs report 1534 persistence batches each, window-32 runs 350/353. Host
poll time is about 11–14% of elapsed, with maximum individual rounds below 8 ms.
These diagnostic totals support investigating durability batching; they neither
reconstruct a parallel critical path nor prove improvement. The predeclared
250 ms TCP serial next-tuning target is not met in this diagnostic run.

Raw local/replicated samples, summaries, source/executable hashes and environment
are in validation/performance/slice100. Independent CSV checks confirm per-stage
arithmetic, bytes, percentiles, unique IDs/indices/values, rate summaries and
host-count bounds. All-target/all-feature Clippy with warnings denied passes;
no production consensus, storage, runtime, transport or dependency changed.
Full P0–P7 remains active; sustainable/open-loop/multi-group/maintenance performance,
fixed-p99 improvement, macOS/separate-host and broader phase gaps remain outstanding.

Native-only WAL example and TLS-only replicated example compilation also pass.
Final formatting, diff checks and the unchanged 71-contract inventory pass.
