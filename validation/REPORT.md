# Validation report — slice 35

Current evidence index: [slice192 baseline review](baseline/slice192/README.md).
Latest focused implementation evidence: [slice194a transfer decisions](baseline/slice194a/README.md).
Entries below are historical and retain their original source/coverage limits.

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

## Slice 101 — actual shared native Multi-Raft benchmark

Added a benchmark-only Node::from_parts assembly with one authoritative WAL/worker,
one snapshot worker and one TCP/QUIC endpoint per replica for 1–32 groups. Optional
GROUPS CLI retains the one-group startup mode. Client ticket tracking is replica
and group scoped; all-replica/recovery positions remain per-group. A per-group
window ceil(global window/groups) prevents claiming one outstanding request/group
while accidentally accumulating several. Receipt histories, every group's local
value and quorum read, full worker joins/reopen and original retry outcomes gate
successful sample publication. No production protocol, format, provider, public
contract or default changed.

Four sequential release measurements (256 measured +64 warm-up writes, global
window 8; TCP/QUIC, 1/8 groups) pass with recovered aggregate 320, every expected
partition value and zero extra recovery retries. Both sides use explicit 50 ms
heartbeats and 10000–19999 ms elections. TCP rates are 21.141/9.204 applied ops/s,
p99 950.520/1308.700 ms; QUIC rates 15.807/7.786, p99 1769.466/2920.492 ms.
All groups begin measurement with node 1 leading. Eight groups are slower and
submit more persistence batches; no amortization/throughput improvement is claimed.
The unchanged 250 ms TCP serial p99 target remains unmet/unproven.

The preliminary 1000–1999 ms election TCP one-group run passes, but the eight-group
run reports Unknown(LeadershipChanged) for operation 92 during measurement and
is invalid (no successful summary). Longer experimental elections are disclosed,
not a fix or a service-default change. See validation/performance/slice101 for
raw samples, summaries, environment/source/executable hashes and scope limits.
Independent CSV checks verify complete unique IDs, group history/indices, bounded
windows, arithmetic, nearest-rank percentiles and rates for all controlled runs.
An extra eight-group/one-measured-operation/window-3 TCP correctness run also
passes complete recovery and groups with only warm-up history; its timing is not
used for performance claims. Full P0–P7 remains active, P8 deferred; sustained,
offered-load, maintenance, macOS/separate-host and broader fault evidence remain.

The original four-argument startup mode also passes a one-operation TCP
recovery/retry/join check. All seven published raw result sets (four controlled,
two correctness-only and one preliminary successful run) pass the independent
validator. All-target/all-feature Clippy with warnings denied, TLS-only example
compilation, formatting/diff checks and the 71-contract inventory pass. The Node
inventory records this evidence without adding a public contract.

## Slice 102 — actual shared WAL attribution through public providers

Benchmark-only LogStore/JournalIo wrappers observe the selected native file store;
benchmark helpers are generic over that same public store contract, retaining
plain NativeStartup mode. No production API/provider/protocol/format/default changed.
Observers forward original results/tickets/capabilities, use bounded aggregates
without holding locks across I/O and confer no durability/read authority. A
real-file unit test verifies non-durable append state, exact barrier completion,
empty/stale barrier rejection, maintenance capability/error forwarding and reopen;
it passes with all features and TLS-only.

Four sequential release measurements (TCP/QUIC × 1/8 groups, 256 measured +64
warm-up commands, window 8, explicit 50 ms heartbeat/10000–19999 ms elections) pass
full all-group/all-replica values/quorum reads, joins/reopen, original first/last
historical retry and unchanged final values/joins. All groups start measurement
on node 1. Aggregate recovered value is 320; extra recovery retries are zero.
TCP rates 15.635/7.276 applied ops/s, p99 779.263/2791.030 ms; QUIC rates
17.137/7.326, p99 1680.206/2873.822 ms. No speed improvement is claimed.

Observed means are 1.000/1.005 group units per TCP append and 1.000/1.119 per QUIC
append. Aggregate append processing totals 19.556–76.022 ms versus aggregate
barriers 26.056–105.132 seconds, dominated by native sync/manifest publication.
The latter remain required durability dependencies. The source performs one
append/barrier pair per worker request. This selects bounded ready-queued-request
shared barriers for the next experiment, not a claim of proven queue availability
or improvement. Raw data and independent arithmetic checks are recorded in
validation/performance/slice102, including cumulative joined exact counts and
explicit interval-boundary/parallel-duration limitations. Physical command appends
and diagnostic counters are not useful-operation counts or durable watermarks.

An extra plain four-argument startup one-operation TCP run passes full recovery/
retry/joins and raw checks. All-target/all-feature Clippy with warnings denied,
TLS-only observer test/compilation, formatting/diff checks and 71-contract inventory
pass. The LogStore/JournalIo inventory records the observer conformance evidence
without adding a public seam. Full P0–P7 remains active; the prior 250 ms TCP serial
p99 target is unmet/unproven. Sustained/offered-load, maintenance/recovery, broader
fault/macOS/separate-host and other recorded phase gaps remain; P8 stays deferred.

## Slice 103 — bounded ready-request shared native barriers

NativeLogWorker gathers only already-ready independent FIFO requests within
existing request/unit/retained-byte and store pending limits. Original appends
remain independent; one fully validated ticket-union barrier permits only original
request/visit/owner durable subsets. Original credits/control reserves and queued
reclamation/close ordering remain. No public API, format, timer-default or thread
change. The PersistenceWorker inventory records the new conformance evidence.

Eight new deterministic/native-crash tests pass with all features: reversed exact
union and mixed owners; independent append rejection; partial/duplicate/wrong-
session/wrong-generation/failed barrier and fatal append; FIFO unit/byte limits,
close/reclamation order and store dependency limits. They include 436 native
framing/failure schedules over either shared record (106 bytes each) and eight
actual-file append/sync/publication fault/reopen cases. Existing worker (9),
maintenance (5), log-store (9) and effect-owner (127) tests pass, including actual
three-node/100-group replication/restart/dedup paths. Final all-target/all-feature
Clippy with warnings denied passes. These finite checks are not a complete proof.

Baseline A reuses slice102 raw evidence; baseline B rebuilds 007dbde with the
same executable hash. Two candidate samples per TCP/QUIC × 1/8-group case use
identical workload/observer/timers/resources. All 16 shared runs pass full values,
reads, joins/reopen and original retries; recovered aggregate 320, extra retries
zero. Eight-group TCP rates rise from 7.276/10.612 to 20.108/30.614 ops/s; QUIC
7.326/9.687 to 22.746/27.861. Candidate mean units/barrier are roughly 3.0–3.1.
One-group controls vary with no reliable improvement; cross-sample p99 varies and
no general latency-budget or sustainable-capacity claim follows. Compilation was
excluded from timing; uncontrolled desktop activity remains a source of variation.

Both original startup-mode TCP serial runs also pass recovery/retry/joins. Baseline
p99 478.658 ms, candidate 311.801 ms; the predeclared 250 ms serial target remains
unmet. Full raw CSVs/summaries, storage observations, independent passing arithmetic,
hashes and reproduction/limitations are in validation/performance/slice103. P7 and
other recorded P0–P7 phase gaps stay active; broader sustained/offered-load,
maintenance, fault/macOS/separate-host evidence remains. P8 stays deferred and CI
remains background feedback.

Final native-only shared-barrier tests also pass (8/8), as do formatting/diff
checks and the unchanged 71-contract inventory.

## Slice 104 — bounded scheduled offered-load benchmark

Benchmark-only --offered RATE extends the existing shared Node assembly. Fixed
intended start times expose dispatch lateness; bounded global/per-group retained
clients cause explicit dropped offers rather than a delayed retry queue. Rows
include admissions/refusals, Applied, NotProposed and Unknown; unknown/pending/
invalid outcomes prevent successful summary publication. Known Applied histories
account for refused-ID holes. Complete all-replica values/quorum reads, joins,
reopen and original historical retry outcomes remain successful-publication gates.
No production API/provider/format/timer/resource change; the Node inventory records
selected evidence without adding a public contract.

Six new schedule/ledger/history/diagnostic tests plus the existing real-file
observer test pass with all features and TLS-only (7/7 each). Five independent
negative checker tests pass. All-target/all-feature Clippy with warnings denied
passes. Original startup TCP and shared QUIC closed-loop paths also pass complete
one-operation correctness/recovery checks; timing is not performance evidence.
The updated checker passes all 16 slice103 shared and both serial archived cases.

Four sequential single-host/eight-group release cases pass exact raw checks and
full values/reads/recovery/retry/joins, with zero extra retries and no uncertain
outcomes. TCP/QUIC at 4 offers/s over 30 s admit/apply all 120 and refuse none;
p99 1454.546/1091.054 ms. At 48 offers/s over 5 s, TCP applies 40/240 and refuses
200 (8 applied in drain); QUIC applies 121/240 and refuses 119 (3 in drain).
Horizon applied rates 6.400/23.600 ops/s, total rates 7.428/23.474; p99
1691.862/506.619 ms. All scheduled offers are retained in raw rows; no offered or
physically appended count is relabeled as useful Applied throughput. Raw and
independent arithmetic/histogram/ticket evidence are in validation/performance/slice104.

No concurrent compilation/bulk build work ran during performance measurements.
Current kernel 7.2.9-2-cachyos differs from slice103's recorded 7.2.9-1. These are
one-sample finite illustrations, with uncontrolled desktop/filesystem load and no
deliberate maintenance/fault injection: no sustainable-capacity, transport-speed,
fixed-p99, macOS or separate-host claim. The 250 ms TCP serial target remains
unmet; compatibility runs do not replace it. Full P0–P7 remains active, including
earlier phase gaps; P8/Windows are deferred and CI remains background feedback.

## Slice 105 — bounded maintenance and selected follower catch-up

Benchmark-only scheduled checkpoint/reclaim waves use the public Node controls,
same-leader/term/store durable snapshot boundaries and exact original full reclaim
tickets. Bounded skipped opportunities, full observed pause duration, raw
maintenance/offer outcomes, nonregressing reopen bases and explicit cleanup remain
visible. No production provider/protocol/format/resource/timer changes.

Thirteen benchmark tests pass with all features and TLS-only, including six new
maintenance state/scope/timing tests. All-target/all-feature Clippy with warnings
denied passes. Four independent maintenance negative checker tests and five
offered checker tests pass. The updated verifier passes all four archived slice104
and 18 slice103 histories. Contract inventory remains 71; it verifies metadata
shape/paths, not protocol correctness.

Four successful eight-group/three-replica cases offer 480 commands at 8/s over
60 seconds: TCP control and maintenance+pause apply 462/443, window-refuse 18/37,
p99 1171.573/2051.818 ms. QUIC control and maintenance without pause apply 456/424,
refuse 24/56, p99 1495.611/1956.002 ms. TCP maintenance completes 27 checkpoints,
81 exact reclaims and 2 skips; QUIC without pause completes 26/78/3. Freed bytes
are 352388/344004. Every successful case passes expected values, quorum reads,
full joins/reopen and original historical retries with zero extra retries. TCP's
five-second host-poll pause produces a post-pause-prefix checkpoint and actual
new follower snapshot installs, with recovered bases retained.

The matched QUIC pause experiment fails the final catch-up gate. Raw 411 Applied/
69 refused offers, maintenance rows and successful cleanup are archived, without
a successful summary/recovery claim. The original error lacks individual failed
predicate values; a focused diagnostic change retains those facts on future
failures while preserving acceptance. A no-pause QUIC result does not pass the
failed pause experiment. Preliminary old-executable control and concurrent-build
correctness smoke are retained separately. Raw evidence, independent arithmetic,
hashes and environment are in validation/performance/slice105.

These finite single-host cases establish selected maintenance behavior, not
sustainable capacity, a transport advantage, arbitrary-fault proof or macOS/
separate-host validation. No compilation/bulk build work overlaps long performance
runs. The original 250 ms serial p99 target and prior P0–P7 scope gaps remain open;
P8/Windows stay deferred, CI is background and the full goal remains active.

A short QUIC pause run under the diagnostic-only executable passes the unchanged
catch-up gate and independent arithmetic: 120 offers/15 seconds, 96 Applied,
24 window refusals, 5 checkpoints/15 reclaims/2 skips, 108348 bytes reclaimed;
actual pause 5.000185054 seconds, one new follower install at/after forced boundary
11 beyond prior accepted index 10. Full value/read/reopen/base/retry/join gates
pass with zero extra retries. This selected success does not diagnose or erase
the failed long run. Both are archived; repeatable 60-second QUIC catch-up remains
unverified. Final formatting/diff and unchanged 71-contract metadata checks pass.

## Slice 106 — buffered compaction schedules and selected-group repair evidence

One 60-second QUIC diagnostic repeat fails the unchanged catch-up gate with the
source still leader/term1/same binding, aggregate new follower installs1, but
selected group1 base0 < required11. All480 offers remain (416 Applied,64 refused).
Cancellation/reclaim cleanup/close succeed; no successful summary or full
value/read/reopen claim is made. Raw failed artifacts and exact diagnostic facts
are archived in validation/performance/slice106. No build/compilation overlaps
this diagnostic run. This identifies a failed predicate, not a packet trace or
production corruption.

Two new public-contract tests compare buffered Append/commit-retry delivery after
leader compaction against dropping those messages. Buffered delivery preserves
committed state at follower base0 without installation; dropped delivery requires
a snapshot at base2. Both check delayed old requests after newer commitment,
exact original duplicate outcomes, quorum reads and reconstruction. Native-file
cases close/reopen all WAL/snapshot stores, check recovered bases/values, elect
anew and return original retries without a second application effect.

Snapshot suite21/21 all-features and9/9 core/contracts-only pass, as does all-target/
all-feature Clippy with warnings denied. Five independent maintenance checker
tests pass; a new two-group case rejects falsely using another group's installed
base and a positive aggregate install count as selected-group repair. Inventory
remains71 contracts, with SnapshotRetention's finite conformance scope updated.
No production/benchmark behavior, protocol, format, provider or timer change.
These deterministic schedules do not reconstruct the live QUIC history or prove
arbitrary-fault safety. Long QUIC acceptance and earlier baseline/platform/p99
gaps remain open. FullP0–P7 stays active; P8/Windows deferred, CI background.

## Slice 107 — bounded public/native observability and service metrics

Public Observer receives fixed Copy post-poll NodeObservation and exports a Copy
CounterSnapshot. NativeCounterObserver uses bounded fixed-cardinality saturating
counters, exact owner/time validation and scoped close, with no I/O, clock reads,
locks, threads, event queue or persistent state. Node/core do not invoke it;
observer failure cannot substitute for the original poll result. The executable
uses the same public seam for normal/admin polling and local volatile metrics.

Executed checks:

- Observer conformance: 3/3 all-features and 2/2 core/contracts-only; independent host
  shared views, native fixed storage, binding/session/lane/generation/time/close
  rejection, saturation and failed-poll capture.
- Owning Node with overloaded/closed host diagnostics: 1/1 core/contracts-only;
  original write/read outcomes and graceful shutdown remain correct.
- Real three-process metrics/recovery/retry histories: 2/2 TCP/TLS and QUIC;
  local counters increase, full joins/reopen preserve values/original retries,
  and fresh no-quorum startup counters exclude replay delivery. TCP-only build
  independently passes its 1/1 history.
- Existing complete executable membership lifecycle: 2/2 TCP/TLS and QUIC;
  enrollment/promotion/retirement/restart preserve the updated admin poll branch.
- All-target/all-feature Clippy with warnings denied passes.

The first process attempts failed because the restarted listener was queried
before readiness. The fixture now awaits a real successful metrics response under
its deadline; production timers/semantics were unchanged. No performance tests
were rerun and no performance gain or zero-overhead claim follows. Baseline map
covers roadmap exits/catalogue pointers but is not a complete normative safety
or arbitrary-fault audit. Inventory adds the initial C19 seam (72 contracts),
corrects wire/scope design associations, and retains richer telemetry/missing
catalogue requirements. Full P0–P7 remains active; existing long QUIC catch-up,
fixed-p99, platform/lifecycle gaps remain. P8/Windows deferred, CI background.

## Slice 108 — public/native transport buffer provisioning

BufferPool/FrameBuffer expose fixed maximum-byte owned reservations, initial
length, bounded resize, diagnostics and scoped close. NativeBufferPool shares
finite atomic credits, allocates lazily and releases storage before credits.
NativePeerTransport uses leases for both encoded directions; a selectable shared
factory accepts an independent downstream pool. NativeWireCodec counts without
allocating a frame, then encodes into the selected lease. Wire/durable formats
are unchanged. Original outbound queue credits remain separate from frame credits.

Executed on Linux:

- All-features buffer 3/3, transport 18/18, wire 14/14 and QUIC 10/10 pass.
  Coverage includes downstream shared-view lifetime, maximum reservation versus
  initial allocation, native concurrent owners, delayed flush, exact original
  ticket/queue credit retention, pool exhaustion before read/admission, retry,
  malformed encoding/header, receive growth allocation failure, abort/drop and
  real TCP/TLS framing. QUIC includes three Raft replicas and exact log completions.
- Native without TLS: buffer 3/3, transport 17/17, wire 14/14 pass.
- Core/contracts-only: buffer 1/1, transport 1/1, wire 1/1 pass.
- Actual three-process service metrics/recovery/original-retry histories: TCP/TLS
  and QUIC 2/2 pass. Full worker joins/reopen preserve application values/retries.
- Actual executable add/enroll/promote/retire/restart histories: TCP/TLS and QUIC
  2/2 pass, exercising membership formats through the changed native encoding.
- All-target/all-feature Clippy with warnings denied passes; fmt/diff checks and
  inventory validation pass (73 contracts).

An initial TCP test needed the permitted local-socket execution environment.
The new receive-growth failure fixture initially assumed the remote send was
already terminal when the receiver refused allocation. It now explicitly aborts
that still-pending sender before collecting its failed completion. Production
pool exhaustion maps to the existing transport Overloaded result so PeerDriver
stages/retries original batches; allocation failure during resize is terminal,
avoiding a retry after a consumed header. No consensus algorithm/timer changes.

This is initial encoded-frame provisioning, not a universal allocator or pooled
WAL/snapshot/application storage. Existing host codecs' compatibility defaults
retain separately bounded temporary Vec encoding. Shared-pool fairness/control
reserve and connection admission remain explicit next work; undersized shared
budgets can stall idle receive reservations. No performance tests were run and
no speed/zero-overhead claim follows. macOS/separate-host, broader fault/lifecycle,
long QUIC selected-group repair and fixed-p99 gates remain open. Full P0–P7 stays
active, P8/Windows deferred, CI background.

## Slice 109 — constrained public/native outbound admission

AdmissionPolicy reserves bounded Copy owner/peer/class/cost/usage metadata after
mandatory native queue checks. An opaque owned AdmissionLease retains provider
credits through queued/dispatched work, original-batch completion and both
queue/batch destruction orders. NativeAdmissionPolicy supplies finite shareable
bulk batch/message/capacity-byte reservations with bounded atomic attempts,
rollback and scoped view close. Control bypasses optional policy and remains
subject to mandatory hard ceilings/reserves. Policy grants no consensus authority.

Executed Linux checks:

- All-features admission 8/8, outbound 4/4, transport 19/19 pass. Independent
  downstream policy injection checks original Vec pointer/capacity on refusal,
  exact owner/peer metadata, permissive/refusing policy versus node/peer/background/
  peer-count ceilings and control reserve, stale completion, delayed ownership,
  close/drop in both orders, failed construction, native partial rollback and
  concurrent views. Native transport holds a lease through delayed flush and a
  partial-send abort, retaining credits until the original batch is consumed.
- Native without TLS: admission 8/8, outbound 4/4, transport 18/18 pass.
- Core/contracts-only: admission 1/1, outbound 3/3, transport 1/1 pass without
  warnings after moving a test import into its native-only module.
- Real shared 100-group owner/native WAL/TLS history: 1/1 passes.
- Actual three-process service recovery/original-retry histories: TCP/TLS and
  QUIC 2/2 pass. Full worker joins/reopen preserve application values/retries.
- Actual executable add/enroll/promote/retire/restart histories: TCP/TLS and QUIC
  2/2 pass.
- All-target/all-feature Clippy with warnings denied passes (one fixture default
  initializer lint corrected); fmt/diff checks and inventory pass (74 contracts).

Outbound Rust contract2 adds the optional batch lease. Host constructors were
updated to set None when they supply no additional reservation; the original
native queue constructor remains available. The lease is shared by retained
queue bookkeeping and the batch, preventing early credit return when either
owner is destroyed first. Message storage drops before the batch's lease field.
No wire, persistent format, durability token, timer or Raft algorithm change.

This is initial outbound policy integration. Client/disk/connection/shared-frame
policy, shared-pool fairness/control storage and C21 authorization remain separate
work. One fixed opaque lease allocation per accepted bulk batch is bounded by
batch ceilings; no performance benchmark or zero-overhead claim was made. Previous
long QUIC repair, fixed-p99, platform and broader fault/lifecycle gaps remain.
Full P0–P7 stays active; P8/Windows deferred and CI background.

## Slice 110 — authenticated service principals and command scopes

Executed Linux checks:

- `cargo +stable test --locked --offline --all-features --test authorization`:
  4/4 pass. Independent host credentials/policy without native, invalid simulator/
  unready/changing channels, exact binding, generation, expiry, clock, scopes,
  native close/replacement snapshots and rejected-plan allocation ownership.
- The same target with `--no-default-features`: 2/2 pass; with
  `--no-default-features --features native`: 4/4 pass.
- `cargo +stable test --locked --offline --all-features --test counter_service`:
  25/25 pass. New TCP/QUIC peer histories enforce reader/writer/admin permissions
  on real mutual-TLS command connections, reject plaintext and same-CA wrong pins,
  checkpoint/recover original operation IDs, revoke writer on policy restart and
  join workers. Invalid access grants fail before store creation/listener ownership.
  Existing client routing/uncertainty/deadline, service recovery and executable
  learner/promote/retire/restart histories remain green.
- All-feature/all-target Clippy with `-D warnings`, fmt, diff and inventory pass
  (75 implemented contract records).

Initial authenticated process failures came from a helper's unrelated fixed retry
ID; its parameter now identifies the actual history operation and recorded result.
A full concurrent run then observed a valid QUIC UNKNOWN LeadershipChanged. The
new history explicitly retries only that result with identical operation ID and
payload under a finite caller deadline, retaining the production CLI's stop-on-
uncertainty semantics. The final full suite passed. A redundant non-Drop fixture
call was removed to satisfy Clippy. These failures are not protocol safety claims.

Public service authorization is separate from transport identity and consensus
membership/ownership. Native plans have explicit generations and finite validity;
new generations deny old credential snapshots. Executable reload occurs on restart,
not live refresh. Denied commands admit no work; later expiry cannot undo an
accepted command. Existing rustls authentication/pins are reused; no custom crypto
or Raft/wire/WAL format change. Unflagged loopback command mode remains trusted
plaintext. Live rotation, external issuers, durable principal audit and general
public configuration ingress remain incomplete. No benchmark, macOS, separate-
host, arbitrary-fault or complete P0–P7 claim. Goal stays active; P8/Windows deferred.

Final cleanup inspection moved pending-ticket cancellation into the common
connection-removal path for deadline and TLS/channel failures. The final full
service suite and Clippy were rerun after this edit; the specific post-admission
TLS-failure branch was inspected, not separately fault-injected.

## Slice 111 — bounded peer endpoint discovery through native connections

Executed Linux checks:

- Discovery conformance: all-features 4/4, core-only 3/3, native without TLS 4/4.
  Independent host resolver/connector exercise original refused request, resolved
  address substitution, retained slots, wrong identity/expiry, duplicate receipt,
  exact-generation invalidation, cancellation, rejected construction and shared
  resource views. Native tests cover capacity, conflict/stale generations,
  invalidation/expiry floors, monotonic clock and volatile restart.
- Connector suite 14/14: real native TCP/TLS timed-out obsolete endpoint,
  invalidation, externally published fresh hint and authenticated connection to
  its new address despite the caller supplying the obsolete address. Exact
  node/store/generation authentication and explicit close/drain/dial-worker join
  are checked; returned sessions remain live after connector cleanup.
- Full owning runtime suite 129/129: includes new driver backoff on four transient
  discovery errors and terminal wrong-scope rejection, plus existing real native
  hundred-group transport/checkpoint/recovery histories.
- Full executable service suite 25/25: existing TCP/QUIC principal, membership,
  automatic routing, quorum-loss/deadline, checkpoint/restart and retry histories.
- All-feature/all-target Clippy with warnings denied, fmt/diff and inventory pass
  (76 contract records). The shared-view construction/close test was added after
  the combined runtime/service run and subsequently passed all feature selections.

No failed acceptance check was observed. Source review added duplicate-completion
validation before removing any attempt, avoiding partial removal or a repeated
receipt panic. Accepted attempts retain original deadlines and completion lifetime;
only the selected endpoint is replaced. Discovery hints cannot provision pins,
activate voters, grant read authority or modify ownership. No wire/WAL/Raft change.

C17 remains partial: responsibility-authority resolution, dynamic executable
refresh, external protocols/refresh scheduling and real discovery-selected QUIC
execution are outstanding. QUIC service regressions do not establish that last
integration. Native hints/floors are volatile; reconstruction is explicit trusted
host input. The cache is not a durable authority journal. No performance, macOS,
separate-host, arbitrary-fault or complete-baseline claim. Full P0–P7 stays active,
P8/Windows deferred; CI background.

The new peer-driver transient discovery/backoff test also passed core-only (1/1),
without native/TLS features.

## Slice 112 — responsibility-authority discovery and cached child independence

Executed Linux checks:

- Routing all-features 13/13, core-only 9/9 and native without TLS 13/13 pass.
  Downstream manifest source/cache injection checks exact missing-path lookup,
  wrong source/manifest, expiry, lookup/cache budgets, minimum epoch/generation,
  fenced route refusal and independently closable shared source views. Warm child
  resolution bypasses the closed source after parent removal.
- Focused native TCP/TLS and QUIC parent-independence histories 2/2 pass, each
  exercising WAL and checkpoint variants. Actual Directory quorum-read tickets
  and outcomes populate NativeAuthorityDiscovery; the common discovery consumer
  supplies routes for real child proposals/reads, retries, checkpoint and restart.
  Source and parent roles are closed before child service continues. No ancestor
  commit is inserted in the child path.
- The final focused rerun also passes read-binding/group/request checks, live
  duplicate idempotency, invalidated/older read replay refusal, fresh observation
  replacement, stale-epoch refusal, exact old-observation invalidation, expiry
  without duplicate renewal and original rejected route-vector pointer retention.
- Directory suite 22/22 passes. The broader full routed lifecycle run remains
  live at this record; it is not counted as passed. Native histories serialize
  their socket/worker topologies. Preserve the running process and record its
  eventual terminal result separately; do not restart solely for output silence.
- Final all-feature/all-target Clippy with warnings denied, fmt/diff and inventory
  pass (77 contract records).

The schema was refined after real-path inspection: last-barrier equality alone
could not reject replay of a different older completed read at the same index/term.
The native provider now binds to an exact ReadInvocationBinding and consumes the
original ticket with a bounded highest sequence. Out-of-order older completed
reads can be rejected as stale hints and refreshed through a new read. A script
constant-reassignment error initially left test call sites using the old argument
shape; compilation identified missing ticket arguments. The script/call sites
were fixed directly before successful focused execution.

This is explicit refresh from trusted host-local completed Directory reads, not
an externally signed proof or automatic remote fetcher. ReadOutcome/tickets must
come from the original completed host invocation. Native observation IDs are
provider-local and discarded at replacement/restart; source reconstruction is
empty and explicit. Payloads use existing bounded cache/manifest admission;
observation metadata is bounded by retained manifest count. Fresh successful-None
source results invalidate a matching previous source observation, without proving
arbitrary namespace retirement; that branch was inspected, not independently
fault-injected in this slice. Existing target admission/apply/fencing remains the
authority. No wire/WAL/Raft change, macOS/separate-host, performance, arbitrary-
fault or complete P0–P7 claim. P8/Windows deferred; full goal remains active.

## Slice113 — initial constrained learner placement

Executed Linux checks in this goal turn:

- All-feature placement planning 5/5 and placement authorization 8/8 pass.
- Core-only planning 4/4 and native-without-TLS planning 5/5 pass.
- Actual executable TCP/TLS and QUIC add/enroll/promote/retire/restart histories
  2/2 pass (12.31 seconds). Both consume a planner-produced learner record from
  recovered membership, preserve ordinary readiness and joint/final admission,
  and retain configuration identities and Counter deduplication after reopen.
- All-feature/all-target Clippy with warnings denied passes. fmt/diff and inventory
  shape/conformance-path checks pass (78 records).

Planning tests exercise independent downstream providers, authorization denial,
malicious scope/sample/store/candidate output, stale/invalid samples, joint state,
operation-history bounds/reuse, configuration overflow, eligibility and stable
ranking across input order. Candidate capacity/domain inputs are fixtures, not
measured capacity or proof of physical independence. Planning is synchronous and
non-durable; only existing ordinary membership persistence grants authority.

The prior full routed lifecycle run (session21618) remains live at this record,
with another TCP checkpoint delegation history passed. It is not counted as a
terminal suite pass and must be observed through the same handle. No new slice113
check failed. Automatic voter replacement/global rebalancing, live capacity
collection/reservations, general public mutation ingress and the broader baseline
remain unfinished. No macOS/separate-host, performance or full-protocol proof
claim. Full P0–P7 remains active, P8/Windows deferred.

## Slice114 — generic enforced application envelopes

Linux executed: all-feature application 10/10, core-only application 10/10,
Node facade 35/35, readiness 10/10 (including native TCP/QUIC compacted learners),
and executable TCP/QUIC membership/enrollment/promotion/retirement/checkpoint/
restart 2/2 (12.26 seconds). All-feature/all-target Clippy with warnings denied,
fmt/diff and inventory78 pass.

New tests distinguish current checkpoint length from whole configured lifetime:
understated command/checkpoint or wrong-schema configuration fails before host
authorization and persistence, preserves storage/core and leaves Node Running;
readiness refuses a fitting current checkpoint with insufficient lifetime budget.
Downstream capability/default-none and compatible larger declarations are checked.
Seven native bounded applications report existing enforced envelopes. Providers
remain trusted to enforce their reports; no capacity reservation is established.

The initial non-escalated readiness run passed seven cases and failed two native
socket binds with PermissionDenied. The permission-enabled run passed those cases,
and the final run includes the new lifetime regression (10/10). An initial wrong
node test target/filter selected zero cases; that is excluded from evidence. The
correct effect_owner/node_facade selection ran 35 cases. No broad rewrite or
unchanged failing-check loop. The older full routed lifecycle run21618 remains
live and is not counted as terminal. Generic public configuration ingress, broader
lifecycle, macOS/separate-host/fault and performance evidence remain open. Full
P0–P7 active; P8/Windows deferred.

Continuation observation: the same live run21618 subsequently passed
native::delegation::repeat::quic_delegated_repeated_moves_recover_from_checkpoints.
The overall run still has no terminal result; preserve it.

## Slice115 — authenticated provisioned-intent configuration command

Linux executed checks:

- Remote configuration histories TCP/QUIC and missing-access preflight: 3/3.
  Dormant startup, native reader/writer denial, wrong-scope admin grants,
  nonleader refusal, missing operation, learner and joint/final commitments,
  checkpoint/restart and original-ID completed retries preserve Counter dedup.
- Isolated interrupted configuration CLI reply: 1/1; exact original bytes and
  UNKNOWN response, no automatic reroute. This uses a fake peer, not an arbitrary
  native configuration-phase disconnect campaign.
- Existing automatic executable membership add/enroll/promote/retire/restart:
  2/2 (12.50 seconds). Existing authenticated ordinary command/scope/recovery
  histories: 2/2 (2.74 seconds).
- All-feature/all-target Clippy with warnings denied, fmt/diff and inventory78 pass.

A first Clippy check reported a collapsible nested cleanup conditional. A focused
short-circuit-preserving fix passed the final check. Remote calls reuse existing
PrincipalCredentials/ServiceAuthorizer, exact NativeAdministrationPlan and Node
configuration admission; no new wire/WAL format or durability shortcut. Native
positive remote histories use unchanged voters for their joint/final phases;
new-voter readiness is exercised by existing automatic enrollment histories, not
claimed as independently fault-tested remote promotion.

The prior full routed lifecycle run21618 remains live; more delegation WAL,
QUIC merge collision and cancellation checkpoint cases passed on that same run.
It is not counted as a terminal full-suite pass. Client-supplied targets and broad
ingress interruption schedules, macOS/separate-host/fault and performance gates
remain open. Full P0–P7 stays active, P8/Windows deferred.

## Slice116 — client-supplied full configuration targets

Linux final executed checks: dynamic native TCP/QUIC histories2/2 (3.59s),
provisioned remote/preflight3/3 (2.85s), automatic executable enrollment/promotion/
retirement/restart2/2 (12.28s), interrupted configuration CLI1/1. All-feature/
all-target Clippy with warnings denied, fmt/diff and inventory78 pass.

New dynamic tests provision no operation records. They exercise reader/writer
denial, malformed/truncated/duplicate/unknown-store/trailing input, valid policy
with denied placement, learner and weighted same-electorate joint/final commits,
exact retained retries, conflicting reuse, checkpoint/restart, compacted payload
comparison refusal, historical operation identity status and a fresh post-restart
record preserving Counter retry state. Oversized commands are refused by the CLI
before exchange; no new raw authenticated oversize-frame claim. The parser grows
vectors from parsed children under node/depth limits rather than reserving an
untrusted branch count.

Initial compilation caught one obsolete Option-plan readiness accessor; a focused
selected-mode accessor fix passed. No persistent/wire/Raft change. Core/provider
admission remains required after target/session/placement checks. Compacted
operation identity alone does not compare payloads; dynamic retries then refuse
comparison rather than invent matching targets. Broad native ingress disconnect/
revocation, remote new-voter fault schedules, P4/P5/P6/P7 and platform/performance
release gates remain open. Full P0–P7 active; P8/Windows deferred. The older broad
routed lifecycle run21618 remains live with additional QUIC delegated checkpoint
recovery passed; no terminal suite result is inferred.

## Slice117 — interrupted administration and retained promotion repair

Linux local evidence: 51 core membership tests; all 33 tests/member_recovery.rs
histories (including native wire5/wire6, WAL/barrier failures, TCP/TLS and QUIC);
both new native counter-service close/deadline/unread-commit histories (13.29s
latest); focused core-only final-promotion regression; all-feature/all-target
Clippy with warnings denied; formatting and 78-contract inventory checks passed.
A negative control restored the pre-fix batched_repair source temporarily and the
corrected deterministic regression failed at missing historical repair, then
passed with the implementation restored. An initial fixture error used leader
append completion on a follower; setup now explicitly persists the committed
final boundary before exercising the real election.

The native QUIC failure identified a stable-final survivor and a learner missing
the committed promotion, unable to elect after leader loss. The fix reuses the
bounded LearnerRepair receiver contract with retained committed historical joint
and exact old/current stores. Repair never advances commitment or replaces the
ordinary durable ballot. Tests retain missing-compacted-history refusal, source
eligibility, restart/lost reply, stale context and identity checks, and higher-term
persistence. Clean TLS closure now cancels observation promptly while the TCP
socket remains open; deadline and lost client reply do not undo durable work.

Original long run21618 is now terminal successful: directory22, routed54,
routing13. Routed runtime was 2641.76s. It used the original older binary; these
are its broad lifecycle results, not retrospective new-repair validation.
No full proof, arbitrary fault/revocation, post-final historical snapshot repair,
macOS/separate-host or performance claim. Full P0–P7 remains unfinished and active;
P8 and Windows deferred. See docs/IMPLEMENTATION.md for linked current/next plans.

## Slice118 — explicit committed stable checkpoint learner recovery

Linux execution: 37 member_recovery tests (four new), 130 effect_owner tests,
60 all-feature library tests, 14 secure and 14 wire tests passed. Native WAL
binding every-byte append plus sync/publication faults now cover both joint and
committed-final checkpoint images, recovering the old or complete boundary with
matching application state; the snapshot provider in those fixtures is host-owned.
The new mode's host history compacts the final prefix, installs/restores before
reply/ballot, completes ordinary election, and separately recovers a lost install
completion. Unknown-as-old-voter promoted senders, missing prior operation IDs,
invalid identities/scope/boundaries and unselected mode are refused.

Both native counter-service interruption histories passed using TCP/QUIC wire7
peers (13.33s). This confirms startup/codec/session selection and those existing
histories; it does not force the new committed-final snapshot transfer. Secure
negotiation explicitly tests7 and6/7 mismatch in both directions; the new wire
RPC rejects every truncation, small size ceilings and format6 encoding/decoding.
Core-only/all-target compilation, core-only library tests, formatting,
all-feature/all-target Clippy and 78-contract inventory validation passed.

No persistent format changed. Native snapshots/WAL/application barriers remain
mandatory; no repair response counts as a ballot. This uses authenticated
non-Byzantine old-view-voter committed snapshot provenance, not a Byzantine or
transferable commitment certificate. An accepted-final head with an earlier
committed-joint checkpoint correctly refuses to export an uncommitted final;
bridging from that actual joint image and forced native-network checkpoint
histories are next. Missing trusted old-view source remains unavailable. Full
P0–P7 remains active with previous scope/fault/platform/performance gates, P8 and
Windows deferred, CI background.

## Slice119 — committed joint checkpoint bridge beneath accepted final

Linux: 60 all-feature library, 38 member_recovery and 48 native_member_startup
tests passed in the final completed run (0.07s, 0.50s and 3.38s). All-feature
all-target Clippy -D warnings, core-only all-target compilation, formatting and
78-contract inventory checks passed. Previous repair source used as a negative
control fails the bridge test at the required historical snapshot export.

Four native histories force wire7 TCP/QUIC pre-election recovery from committed
joint and final checkpoints, with original voter absent and the joint entry
compacted away. They check application restore before eligibility, candidate
status before observation, owner abort/reclaim/reopen, ordinary election, new
write, duplicate original receipt and final WAL/checkpoint/application reopen.
Owner abort permits accepted I/O to finish and discards observations; it is not
power-loss or process-kill evidence. Host image/WAL fault coverage remains
separate. Historical joint replies cannot certify accepted-final commitment.

The wider native run found delayed learner repair arriving after promotion,
first in an obsolete term and then retrying an older configuration. Temporary
message traces identified the ingress; exact current-voter authentication gates
stale-term discard and old-view WrongIdentity refusal. No term/log effect escapes.
A host regression covers all three repair envelopes, current/higher terms,
unknown sender and wrong group. The existing native strict error assertion was
not relaxed. Earlier intermediate runs failed; final suite results above cover
the focused correction and checkpoint changes together.

No new provider or persistent format. Older mismatching stable checkpoint,
broader membership/lifecycle/fault schedules, macOS/separate-host evidence and
performance remain incomplete. Full P0–P7 stays active; P8/Windows deferred.

## Slice120 — session-bound readiness after actual native learner restart

Linux final run: 50 native_member_startup tests passed (6.05s), including two new
TCP/QUIC histories. All-feature/all-target Clippy -D warnings, formatting, diff
whitespace and 78-contract inventory checks passed. No production API/provider,
wire or storage format changed; existing checks cover this boundary.

The test captures native readiness, closes/reclaims the learner actual workers
and stores, reopens the same WAL/checkpoint files/address/identity, and waits for
the leader to authenticate a different StoreSession. The original proof yields
NotProposed(AuthenticationRequired) for its exact admission/operation ticket;
leader log/state remain unchanged. Fresh readiness preserves the original
configuration target/ID, finishes joint/final, demotes former voter3 to learner,
and retains original Counter duplicate outcome and current value through native
checkpoint and file reopen. Existing cancellation/queued plan revocation checks
also run in these histories. Initial stable learner assignment remains seeded.

A negative control removing only Node's live peer binding comparison failed at
unauthorized leader membership publication (0.17s). Production source restored;
complete successful run above used the original gate. An initial fixture failure
on the expected refused admin step was corrected with exact-ticket assertions;
no broad driver error exemption was added.

This is selected graceful learner restart and embedding evidence, not process
kill/power loss, live remote credential rotation or universal membership safety.
Remote target interruption/revocation and broader P4/P6/P7, platform/fault and
performance gates remain. Full P0–P7 active, P8/Windows deferred.

## Slice121 — observe requesting channel before membership execution

Linux: full 33-test counter_service integration suite passed on the reordered
production owner loop (13.33s). The two subsequently extended TCP/QUIC interruption
histories passed separately (20.21s; 31 unchanged tests filtered). Final
all-feature/all-target Clippy -D warnings, format, diff whitespace and 78-contract
inventory checks passed. No new API/provider, protocol or persistent format.

The existing bounded command poll/cancellation block now precedes Node execution
authorization and prepared-target tick. Observed close/deadline clears the exact
pending channel/target before that iteration executes queued membership work;
late-arriving packets remain asynchronous. Accepted durable work is not undone.
The full suite includes native command authorization, partial I/O/deadlines,
remote provisioned/client targets, lifecycle/recovery/retry, TCP/QUIC close and
unread-commit failover, shutdown and actual file recovery. Existing finite tests
do not force every possible close-versus-execution timing; no universal race
proof is claimed from this ordering review.

The added histories force promotion preparation with the required learner
offline, kill/wait/reopen the command-serving voter at its actual native files,
and require local absence of the abandoned operation before/after learner return.
Explicit fresh target request completes; original unread committed record retry,
conflict refusal, Counter dedup and all-three-store membership reopen still pass.
Recovered operation sets exclude abandoned preparation17015 and closed17011.
This selected process loss occurs before admission, not during persistence; it
cannot certify cancellation or rollback of accepted records.

Broader remote revocation/partial-progress, arbitrary faults, macOS/separate-host
and performance gates remain. Full P0–P7 active; next implementation targets the
P5 metadata-authorized creation intent/bootstrap gap. P8/Windows deferred.

## Slice122 — opt-in metadata creation reservation journal

Linux: 60 all-feature library tests, 31 directory tests (nine new), and 27
core-only directory tests passed (0.06s/0.12s/0.04s). All-feature/all-target Clippy
-D warnings, formatting, diff whitespace and 78-contract inventory checks passed.
The existing library UDP bind first failed for sandbox PermissionDenied; the same
suite with native socket permission passed. Earlier version-width/schema-fixture
and Clippy test-style errors were corrected before final validation.

Public bounded creation intents bind exact metadata parent/generation, fresh
responsibility/group identity, initial recursive voter/store configuration,
application adapter and empty/staging mode. Schema2/init2/checkpoint2 is explicitly
selected before apply; default schema1 remains unchanged, refuses new commands,
and cannot downgrade/upgrade through recovery. Cross-mode application/checkpoint
failures leave destination state unchanged. Ordinary history budgets, identity
reservation/refusal, original retries/conflicts, parent transfer lock, atomic
invalid batch and checkpoint/truncation replay are directly tested.

Native three-core Raft with actual file WAL/checkpoints commits the reservation,
loses the observation, compacts, reopens, elects and snapshot-catches-up the lagging
replica while preserving original status. Delivery in this fixture is modeled,
not native TCP/QUIC. Separate native ModelIo storage histories cut every creation
append frame byte and fail sync/publication before/after; after modeled power loss,
recovery sees the old committed metadata state or the complete intent and no new
manifest/serving authority. This is finite fault evidence, not arbitrary crash or
formal protocol proof.

No assigned-node bootstrap, namespace activation, remote commitment certificate,
live schema upgrade or mixed-version deployment is released. Local creation status
requires separately established authenticated metadata quorum-read authority for
foreign use. Full P0–P7 remains active with previous ledger gaps; next is native
exact-intent bootstrap before election. P8/Windows remain deferred.

## Slice123 — committed creation to assigned durable bootstrap

Linux all-features library61/directory31/group_creation4 passed (0.06/0.13/0.02s),
core-only directory27/group_creation3 passed (0.06/0.00s). Host authority/log/binding
replacements check exact assignment/application before mutation, uncertain binding
publication, exact retry, conflicts, existing-unbound refusal and preservation of
progressed logs. Native ModelIo covers every torn bootstrap frame plus sync and
publication faults with modeled unsynced loss/reopen and durable campaign ordering.
Native immutable binding file unit test reopens at five publication observation
boundaries, preserves original bytes on conflict and checks exclusive lock ownership.
It does not simulate hardware power failure or arbitrary filesystem corruption.

The actual three-core metadata WAL/checkpoint history now rejects lagging/altered
source status and authorizes a new assigned target WAL; target file reopen/exact
retry preserve identity and campaign persists its ballot before vote sends. This
history uses in-memory metadata delivery, not TCP/QUIC created-service startup.
All-feature/all-target Clippy -D warnings passed. Namespace activation, remote
creation authority protocol, owning-service integration and multi-creation shared
WAL binding remain outside this evidence. Full P0–P7 remains active.
Core-only group_creation Clippy -D warnings, format/whitespace and 79-contract
inventory validation also passed. No macOS/separate-host execution claim.

## Slice124 — actual TCP/QUIC created service from committed reservation

Final selected native histories2 passed in1.35s, build8.78s. Both native protocols
commit schema2 metadata intent, provision two assigned replicas, close/reopen
metadata, recover exact original reservation, preserve those two WALs and create
the remaining assigned replica. A recursive-quorum three-node Counter service
then elects, writes, reads, checkpoints, closes and reopens with metadata offline.
Exact creation retries preserve each progressed target WAL. Original application
operation/outcome/dedup survive; changed command bytes conflict and new writes work.
Each corresponding metadata replica WAL is exactly unchanged during child operation.
All-feature/all-target Clippy -D warnings passed. Initial index assertion failure
was a test misunderstanding of Counter's current-applied-entry receipt contract;
production Counter semantics were not changed to make it pass.

These are direct-group owning-service embedding histories, not namespace activation,
a creation RPC/CLI, arbitrary interruption or hardware power-loss coverage. Explicit
snapshot create/recover is caller-owned; partial initialization remains a separate
recovery case. Linux loopback does not establish macOS or separate-host operation.
Final format, whitespace and 79-contract inventory checks passed.

## Slice125 — fresh independent namespace creation

All-feature directory31/namespace3 passed0.13/0.06s; core-only directory27/namespace3
passed0.06/0.06s. Following focused receipt/index fixes, core-only namespace3 passed
0.04s and all-feature namespace4 passed0.15s. Host cases cover canonical/truncated
codecs, full ordinary directory budget with reserved successful control, mismatch,
retry/schema/checkpoint refusal, atomic restore/control indices, non-serving data
admission/apply/read, exact activation and original data retries. A receipt capacity
regression checks retained allocation against the declared bound.

Actual native TCP/QUIC creation124 regression histories2 passed1.41s. New namespace
histories2 passed1.82s, checkpoint/reopen ready, publish and activation state, obtain
target readiness through original quorum reads, preserve publication/data retry
outcomes and keep exact per-node metadata WALs unchanged during offline operation.
Native ModelIo covers every activation frame append cut and sync/publication faults
with unsynced loss/recovery: actual Raft replay either retains non-serving readiness
or exact activation; original retry converges. No arbitrary power-failure or full
protocol proof claim. Fresh independent root namespaces only; source export/freeze,
recursive insertion, deletion/reparenting and directory authority movement remain.

Initial native ProviderViolation was a new guard receipt Vec capacity exceeding its
bound; exact command-count allocation fixes it without weakening validation. First
fixture fields and two Clippy findings were corrected. A filtered test invocation
compiled directory/namespace but ran zero tests for them; subsequent unfiltered
runs above provide actual evidence. All-feature/all-target Clippy and core-only new
target Clippy -D warnings passed before the final envelope regression addition;
final outcomes follow below. Foreign authentication/commitment is a trusted host
obligation, never proved by constructible digests/status. No live schema upgrade,
creation endpoint, macOS or separate-host execution claim.
Final all-feature namespace5 passed0.15s, including a valid4096-voter policy/store
map whose creation command exceeds the old control-command envelope. Final
all-feature/all-target Clippy -D warnings, format/whitespace and80-contract inventory
passed. Final core-only result follows below.
Final core-only namespace4 passed0.06s. Native WAL fault test is feature-gated and
covered by the all-feature namespace5 run. No remote CI gate was used.
Final TCP/QUIC namespace histories2 passed1.88s, build5.13s, including a quorum-read
of the published manifest and exact client cache resolution before target activation.
All-feature/all-target Clippy -D warnings, format/whitespace and80-contract inventory
passed after this addition. Cached routing remains a hint; target guard enforces
its own committed activation/ownership state.

## Slice126 — partial namespace publication/activation with owner abort

Final interrupted TCP/QUIC histories2 passed1.75s (build5.32s). Six linked native
created-service/namespace histories passed4.49s before final strengthened WAL boundary
assertions. Original initialization/publication/activation client tickets stay unread.
Publication/activation commit on two replicas with a demonstrably lagging third;
owner abort stops core polling, discards terminal worker observations and reclaims
actual selected file stores before reopen. WAL base_index0 confirms non-compacted
replay at interruption boundaries. Reclaimed store commit prefixes retain the phase
on the quorum and remain below it on the laggard. Original retries repair the missing
phase and preserve exact status/indices on all replicas. The activation laggard stays
NotActive; post-recovery original data retries and unchanged offline metadata WALs
remain covered. All-feature/all-target Clippy -D warnings passed5.37s.

No production schema/provider/gate changed. Owner abort permits accepted native I/O
to finish; this is not hardware power loss, arbitrary distributed schedules or a
claim of rollback. Source freeze/export for fresh namespaces, recursive selector
insertion and broader original P0–P7 gaps remain open. P8/Windows deferred; Linux
loopback execution does not establish macOS or separate-host behavior.
Final format, whitespace and80-contract inventory validation passed.

## Slice127 — schema4 created namespace transfer metadata

Executed Linux, cargo +stable with --locked --offline. All-feature directory31,
namespace8 and transfer-publication7 passed (0.11s,0.16s,0.04s; build7.56s).
Core-only namespace7 passed0.07s, build4.11s. All-feature/all-target Clippy with
-D warnings passed6.63s. Initial new fixture compilation failed on private range
fields; use of existing public start()/end() accessors resolved it. Early tests
before schema binding passed but were superseded by schema4 histories after
inspection found command-history replay compatibility must be preserved.

Three new constructed-fact metadata histories cover publication-before-transfer,
unknown/incarnation/authority/current-generation rejection, retained failed retry,
reserved control at ordinary exhaustion, pending locks/checkpoint recovery, exact
original decisions, repeated split/merge after restore, unchanged anchor/initial
namespace publication, schema3 historical refusal replay, live/cross-schema refusal
and all truncated schema4 checkpoint inputs with atomic restore. No actual native
schema4 deployment, created-owner freeze/export/import, hardware fault, macOS or
separate-host claim. Source-capable created namespace composition remains next.

## Slice128 — source-capable created namespace guard

Linux cargo +stable --locked --offline: final all-feature namespace12/source7/
target9/publication7/activation9 passed (0.39/0.40/0.20/0.03/0.12s), build2.12s.
Core-only namespace10 passed0.44s, build8.44s. Existing actual TCP/QUIC created
service/namespace histories6 passed4.81s, build12.18s after generic owner assembly.
Clippy all-features/all-targets -D warnings passed6.08s before final target read
assertion; final check recorded below. Initial checks failed for missing owner
module imports/receipt bound and a fixture TargetStatus field/unused import; focused
fixes compiled. No runtime test failed; earlier namespace12 passed0.50s.

New deterministic histories use actual existing TransferSource, BucketCounter and
TransferTarget: namespace initialization/publication/activation, source data/freeze,
exact-F exports, two target imports remaining non-serving, checked metadata transfer
publication and activation, duplicate/outbox lineage and fenced-source reopen.
Control/bootstrap/fence bypass, key mismatch, preactivation freeze/read/write,
reserved IDs, pending order/budgets, source export budget and owner/checkpoint
provenance refusal are checked. Every source checkpoint truncation restores
atomically. A NativeLogStore/ModelIo fence-frame history exercises all append cuts,
sync and publish-before/after faults, actual Raft committed replay, active-old or
fenced-complete observations and exact original retry/export recovery. These are
selected deterministic/storage-model histories; new source-capable actual network
phase/reopen validation is next. Existing fixed-owner network histories remain
regression evidence. No complete lifecycle-proof/hardware/macOS/separate-host claim.
Final slice128 Clippy all-features/all-targets -D warnings passed3.04s; format,
whitespace and80-contract inventory passed. All checks terminal; CI not a gate.

## Slice129 — native created-source split phase recovery

Linux cargo +stable --locked --offline --all-features --test routed. Initial
standalone TCP WAL history passed52.08s (build14.13s); unused test Done variant warning
removed. Four-case run: TCP and QUIC WAL cases passed, both checkpoint cases timed
out after Ready in graceful close at native.rs420 (aggregate145.65s/build8.62s).
Runtime Node Quiescing requires clients.is_drained(), so this was incompatible with
intentionally unread phase results. Focused correction uses existing owner-abort/
reclaim after checkpoint compaction, preserving unread-result checks and original
timeouts. Focused TCP/QUIC checkpoint rerun2 passed113.26s, build7.12s. All four
selected combinations have passing evidence across these runs; the failed combined
run remains recorded. No production fix or protocol redesign was needed.

Each combination uses actual committed creation authorization and assigned native
source WAL/snapshot establishment, immutable creation-record checks, twelve all-
replica phase reopen boundaries (ready, namespace publication/activation, two writes,
intent, two stages, fence, two imports, publication), unread original tickets and
exact retries with unchanged phase indices/digests. Quorum reads verify guard service
state, targets remain non-serving until activation, and source stays fenced. Original
activations then run with metadata/source offline, each target aborts/reopens/retries,
one target serves while the other remains NotActive, imported retries/outbox and new
writes continue independently. Exact offline metadata file bytes and recovered
GroupLogs remain unchanged. Final all-group recovery retains original phases plus
new target data/outbox and stale-route/fenced-source refusal. Actual reclaimed WAL
bases are zero; checkpoint bases are positive after selected prefix compaction.
This is selected Linux loopback/owner-abort evidence, not process/hardware power loss,
arbitrary schedules, recursive insertion, macOS or separate-host validation.
Final slice129 all-feature/all-target Clippy -D warnings passed3.16s; final format,
whitespace/inventory recorded below on completion. No remote CI gate was used.
Final slice129 format, whitespace and80-contract inventory passed; all recorded
validation handles terminal.

## Slice130 — checked responsibility insertion

83 related tests passed (directory31, delegation14, namespace_creation12,
transfer_source7, transfer_target9, transfer_publication7, insertion3). Insertion
covers actual deterministic fenced-parent handoff to two fresh children, exact
reservation/configuration checks, imported retries/outbox, child identity service
and later delegated freeze, full publication checkpoint truncations, atomic batch
rejection and reserved publication after ordinary-history exhaustion. Native
intent recovery covers every frame append cut plus Sync/PublishBefore/PublishAfter
in ModelIo, observing only old or exact recorded intent and no early children.
This does not validate native assigned insertion publication/activation phases,
arbitrary faults, power loss on real hardware, macOS or separate hosts.

Failed initial checks: missing production/test imports, fixture used RoutedOutcome
for TargetOutcome and nonexistent WrongResponsibility instead of WrongIdentity;
activation used a wrong operation ID; exhausted history prevented a later fresh
reservation; self-parent mutation refused earlier by manifest Cycle; Clippy Copy
hint clone. Corrected fixtures/imports and separated exhaustion/later-lifecycle
branches without weakening production checks. Final extra checks recorded below.
Additional slice130 activation9/repeat5/retirement7 regressions passed (104 unique
related tests total). Insertion3 rerun also verifies exact child reads. Core-only
insertion2 and all-feature/all-target Clippy passed before this read assertion;
final rerun/checks recorded below.
Final core-only insertion2 passed0.12s; all-feature/all-target Clippy -D warnings
passed1.31s. Format, whitespace and80-contract inventory passed. All validation
handles terminal; no remote CI gate used.

## Slice131 — native assigned responsibility insertion

Four selected actual native histories passed: TCP/TLS WAL41.20s; QUIC checkpoint,
TCP/TLS checkpoint and QUIC WAL together143.56s (topologies serialized by the
native-history mutex). Tests/routed/insertion.rs uses verified metadata Staging
assignments for all six child stores; first only two child21 stores are provisioned
before metadata recovery/original reservation retries and completion. No route
or serving owner is implied by creation. Exact binding bytes survive phase recovery.

Each case leaves results unread at two data writes, intent, two stages, fence, two
imports and publication; quorum facts then survive abort/reclaim/reopen and exact
retry. Parent and child manifests publish together and resolve to actual child
identities. Original activations run with metadata/source stopped, each target
individually reopens and retries while the first serves with its sibling NotActive.
Imported retries/outbox and new writes survive final recovery; stale parent hints
and source writes refuse. Every stopped metadata file byte and recovered GroupLog
remains unchanged during child-only activity. Checkpoint cases assert reclaimed
positive floors; WAL cases assert zero floors.

This is finite Linux loopback/owner-abort evidence, not arbitrary schedules, real
hardware power loss, macOS, separate hosts or deeper insertion. No production
protocol/store/runtime changed. Initial compile found NativeStartup non-Clone;
owned vectors are moved into open. Clippy flagged needless_range_loop; changed
to equivalent iterator/enumerate. Final all-feature/all-target Clippy -D warnings
passed1.42s; acceptance cases did not fail. Final metadata checks recorded below.
Final slice131 format, whitespace and80-contract inventory passed. All recorded
validation handles terminal; no CI gate used.

## Slice132 — checked same-authority nested insertion

107 related tests passed: insertion6, directory31, delegation14, namespace12,
source7, target9, publication7, activation9, repeat5, retirement7. Focused insertion6
passed2.29s; core-only insertion5 passed2.29s. Three new nested tests cover actual
root insertion then child-to-grandchild handoff, full-map parent reservation/ref/
configuration binding, all codec truncations/tampering, source fence/reopen, atomic
child publication/parent locator refresh, reserved completion after ordinary-history
exhaustion, target activation and retry/outbox/fresh-write preservation. New schema4
targets refuse schema3 restore; schema5 metadata refuses new plan/schema6 restore.
Checkpoint all truncations and failed-batch checks preserve prior state. Dynamic
parent later reservation/grandchild freeze works;31-node ancestry accepts a new
child while32-node ancestry refuses. Cancellation and nested read budgets checked.

All-feature/all-target Clippy -D warnings passed0.88s. Initial fixture missing
ScopeImage import and Copy query clone were corrected without relaxing acceptance.
Selected native root regression is pending its terminal result. No native nested
phase, arbitrary-fault, cross-authority/retained-scope or macOS/separate-host claim.
Full P0–P7 remains active. Final metadata checks recorded below.
Selected native TCP/TLS root-insertion WAL regression passed46.08s under the new
production code, preserving schema5/root-target compatibility. Other prior native
cases were not rerun without a changed path/failure; this is not nested native
acceptance. Added successful nested-intent decline refusal assertion; final focused
result follows.
Final focused insertion6 passed2.95s; core-only insertion5 passed2.57s;
all-feature/all-target Clippy -D warnings passed1.20s. Format, whitespace and
80-contract inventory passed; all handles terminal. No CI gate used.

## Slice133 — native nested insertion phase recovery

Test-only native composition uses schema6 metadata, the original activated schema3
child21 as source and verified assigned schema4 Staging grandchildren31/32. The
fixture derives the binding only from an actual committed full-map parent
reservation; pre-reservation stores do not invent application owners. Ten selected
preactivation phases leave accepted results unread, quorum-observe exact facts,
abort/reclaim, reopen original files and retry exact operations. Each phase checks
source/target service gates and unchanged original source import/activation lineage.
Publication and parent locator refresh are distinct: stale root routing refuses
before refresh and returns exact grandchild grants afterward. Original activations
then recover independently with metadata/source stopped; the first grandchild
serves while its sibling remains inactive. Imported retries do not repeat effects,
new writes/outbox survive final reopen, bindings stay byte-exact, and all stopped
metadata/source files plus recovered metadata logs remain unchanged during activity.

All four selected nested histories passed: TCP/TLS WAL83.99s; TCP/TLS checkpoint
and both QUIC WAL/checkpoint cases passed together265.64s (serialized history
fixture). Existing root TCP/TLS WAL regression passed44.06s after fixture extraction. All-feature/all-target Clippy -D warnings passed1.56s; format,
whitespace and80-contract inventory checks passed before final documentation edits.
Pre-run inspection caught missing election before reopened activation retry and
added the existing campaign helper. No executed acceptance failed.

This is finite Linux loopback owner-abort/selected-boundary evidence, not arbitrary
power loss, cross-authority insertion, retained local scope, macOS/separate-host
execution, mixed-version operation or complete lifecycle proof. No production
protocol, storage or runtime changes. The original pack stays untouched, RPL-1.5
retained, full P0–P7 active and P8/Windows deferred. Current native recovery advances
P1/P5/P6; next is subsequent nested split/merge/retirement continuity, followed by
the full baseline gap audit and next required complete path. CI is not a gate.

Final slice133 format, whitespace and80-contract inventory passed; all validation
handles are terminal. No CI gate used. Full goal remains active.

## Slice134 — inserted-grandchild later split/merge and retirement

Test-only guarded owner composition starts before bootstrap and performs actual
root insertion then nested insertion. It moves responsibility31 from original
schema4 grandchild31 into ordinary targets41/42, then merges their compatible
scopes into43 using exact current parent reservations and locator completions.
Accepted phase commands restore fresh schema-bound checkpoints and retry original
bytes, comparing retained lifecycle outcomes rather than the new receipt index.
Partial merge fencing leaves the other source readable. Publication alone leaves
targets NotActive; incomplete activation evidence refuses source retirement.

Original child21 and grandchild31 retire under scoped explicit host retention
releases and complete retained activation evidence, including retained historical
evidence after later movement. Ordinary split owners retire after merge. Exact
retirement retries and checkpoint restoration keep fences/original activation
lineage while provider payload disappears. Wrong source/fence/partial-target proof
and cross-owner tombstone restore refuse without mutation. Imported operations1/81
and later82 survive the next move with exact original outcomes; new83 adds data,
with four outbox records in merged43. Sibling32 retains80; root manifest stays
unchanged and final root-to-grandchild routing resolves exact merged/sibling grants.

Shared native retirement helper is extracted from the existing tests; fresh-owner
closure and target identity are generalized without production changes. Actual
schema4 nested-source command trace exercises sealed-unpublished, before-manifest
publication and lost-completion cuts using FileLogIo/FileSnapshotIo. Recovery uses
the log-authoritative previous snapshot plus retirement replay, then publishes a
retired checkpoint, reclaims WAL and reopens without provider data. Original
source/ordinary-target native-file regression tests pass after helper extraction.

33 related all-feature tests passed: delegation14 (0.07s), insertion7 (3.02s),
retirement7 (0.12s), repeat5 (0.36s). Core-only insertion6 passed2.90s. Initial
fixture compile found a temporary route borrow, missing scope trait and incorrect
source-status accessor; each was corrected locally. First acceptance exposed
comparison of outer receipt indices on retry: changed to exact underlying retained
outcome, explicitly checking the retry receipt advances. Focused continuation
then passed0.07s; final strengthened payload-removal check result follows.
All-feature/all-target Clippy -D warnings passed1.21s before that final assertion.

This proves finite deterministic/checkpoint composition and selected single-replica
native-file persistence cuts; it does not validate networked later movement,
networked retirement, arbitrary faults/power loss, cross-authority/retained-scope
insertion, macOS or separate-host deployment. General retention remains an explicit
host contract. No production protocol/store/runtime/API change. Full P0–P7 stays
active, P8/Windows deferred, original pack immutable and RPL-1.5 retained. Macro
review: P5/P6 gain subsequent nested lifecycle evidence; all original gaps remain.
Next native later-movement acceptance, then full-gap audit/next required path.
Final strengthened continuation passed0.07s; final all-feature/all-target Clippy
-D warnings passed1.73s. Format, whitespace and80-contract inventory passed.
All validation handles terminal; no CI gate used. Full goal remains active.

## Slice135 — native inserted-grandchild later split/merge recovery

A test-only native history finishes actual root/nested setup with consumed results,
then moves assigned schema4 grandchild31 through ordinary split41/42 and merge43.
Later targets use explicit trusted three-replica bootstrap; application bindings
are reconstructed from quorum-observed original parent reservations. All20 selected
later phase cuts leave accepted results unread, observe exact quorum facts, abort/
reclaim, reopen the same files and retry original operations/bytes. Source and
target status/indices/digests plus actual configuration contexts are checked.

Current dynamic parent21 reservations and completions refresh responsibility31
locators while parent21's epoch and root locator remain unchanged. Root routing
refuses stale child locators between publication and completion, then resolves
exact current grants. Partial merge fencing leaves the other source readable;
non-activated imports refuse data and each target activates independently. Original
assigned source creation bindings remain byte-exact at every reopen.

Stop metadata, child21, grandchild31 and split41/42 before merged43 imported
retries/new writes. All stopped file bytes and recovered metadata GroupLogs stay
unchanged, including root20/sibling22 files stopped throughout. Merged43 retains
original1/81 and later82 outcomes without repeated effects, new83 survives final
reopen, four outbox records remain and unrelated sibling32 keeps80. Final reopen
retains all original and later source fences and the exact completed phase facts.

All four selected TCP/TLS and QUIC later WAL/checkpoint histories passed
together659.04s (serialized history fixture). Original nested TCP/TLS WAL regression passed84.96s after
consumed setup extraction. All-feature/all-target Clippy -D warnings passed2.21s;
format/whitespace and80-contract inventory passed before final evidence edits.
Initial compile needed an existing CheckpointStateMachine bound. Two setup attempts
exposed duplicate=true from the retrying helper for an uncertain response; durable
value3/outbox2 now verify the once-only logical effect. First full TCP attempt
recovered all phase facts but an idle sibling write failed NotLeader; write
preflight now uses the existing campaign helper. No serving gate, timeout, storage
format, production contract or consensus rule was relaxed.

This is selected Linux loopback owner-abort/WAL/checkpoint evidence. It does not
validate networked retirement, arbitrary faults/power loss, cross-authority or
retained-scope insertion, macOS, separate hosts or mixed-version deployment.134
retirement evidence remains deterministic/native-file scope. Full P0–P7 remains
active, P8/Windows deferred, RPL-1.5 retained and original pack unchanged. Macro
P1/P5/P6 gains later native movement evidence without dropping prior requirements.
Next: full baseline gap audit and next required path; following: implement that
audited path with its own schema/contracts/acceptance. CI remains background.

Final slice135 format, whitespace and80-contract inventory passed; all validation
handles are terminal. No CI gate used. Full goal remains active.

## Slice136 — baseline audit and fixed serial performance gate

Production/benchmark revision b2a367e48e045c1776f38aafb4826d4d50462b24 unchanged.
Read roadmap, component contracts, invariant/scenario/release gates and existing
source/evidence; docs/BASELINE_AUDIT.md records missing and unvalidated scope.
Historical tests are not claimed as newly executed. Full P0–P7 remains active.

Actual release example all-feature build succeeded in17.53s. Sequential reference
runs used TCP/TLS,three replicas,one group,64 warmup,256 measured8-byte commands,
window1,50ms heartbeat and1000–1999ms election range. Initial /tmp root was tmpfs;
its successful3.988ms p99 smoke/recovery result is retained but excluded from disk
acceptance. Fresh Btrfs A stopped at operation250 Unknown(LeadershipChanged),
exit1,no complete summary and only a CSV header. Fresh B completed44.532562s,
5.749ops/s,p99432.366616ms,recovered320,original retries verified,workers joined.
Raw B checker passes256 receipts and routing/history/window/timing arithmetic.
Fixed250ms gate correctly fails (exit1); A missing-summary and smoke-context
checks also correctly reject (exit1). Logs, samples, contexts and captured
source/binary/toolchain/kernel/device provenance: performance/slice136. This is
finite Linux loopback evidence, not sustainable throughput, macOS, real power
loss or measured attribution of the leadership-change/storage cause.

Added validation-only fixed-workload/context/numeric checker. It requires the
existing raw checker first and binds context to summary/sample hashes. Captured
host metadata remains trusted input, not proof of device durability. Five direct
Node unit tests passed; final rerun10.80ms. Actual CLI checks ran with approved
escalated read-only execution after sandbox child-spawn EPERM. No automatic
approval rejection or protocol change. Final cargo fmt check, git whitespace
check and80-contract inventory check pass. No Rust suite rerun claimed because
no Rust source changed; the executed complete native benchmark supplies B's
fresh recovery/retry/join evidence. No CI gate used.

Next slice must preserve partial outcomes and measure the startup critical path
before choosing a safe fix. Same250ms gate and durability remain. P8/Windows
stay deferred; original pack and RPL-1.5 retained.

Public-artifact privacy revision: automatic approval review rejected the initial
push due to local paths and captured machine/environment metadata. Detailed
capture remains local; public provenance omits hostname/kernel/device/memory
inventory, archived checker logs redact the workspace prefix, and B context names
the actual workspace-relative benchmark root. Summary/sample/binary hashes and
measurements are unchanged. Independent hardware reproduction is limited by this
redaction. Six final direct Node tests pass10.83ms; B actual raw validator passes
and fixed numeric gate still correctly exits1 after context revision.

## Slice137 — retained native benchmark failures

No production Rust change. Benchmark drive/workload now retains partial valid
receipts, unresolved tickets, original errors, poll totals and replica state on
warmup/measurement failure, followed by explicit owned cleanup. Diagnostic I/O
occurs after measurement stops; incomplete runs cannot produce a success summary.
Complete samples precede verification/recovery. This does not infer applied state
from unknown outcomes or add timing/durability relaxation.

Actual tests: example all-features16/16 pass, final0.02s after3.50s build. Three
new tests validate synthetic unknown history/no summary/exclusive files, injected
loop failure totals, and real three-node TCP/TLS applied-prefix plus DedupCapacity
refusal with exact original error and all workers joined. Initial compile used
.get() on u64 term; focused correction made. Earlier15-test run also passed;
no claim of complete production fault coverage. Release build success6.16s.

One fresh Btrfs Linux startup TCP/TLS reference (same3 replicas,64 warmup,256
8-byte commands,window1,timers/durability): exit0,52.394331s,4.886ops/s,
p99854.999704ms,recovered320,retries verified,workers joined. Independent raw
checker passes256 receipts/window/history/timing. Fixed250ms numerical gate
correctly returns failure exit1. No tuning gain, sustainable-capacity or cause
claim. Raw/source/binary evidence in performance/slice137, private workspace path
redacted in archived checker stdout. No overlapping build during measurement.
Six gate/context tests pass10.82ms; format and80-contract inventory pass. Next
startup critical-path attribution then a cause-supported safe fix. P0–P7 active;
P8/Windows deferred, RPL-1.5 retained, original pack preserved, CI background.

Final example all-feature Clippy -D warnings passed1.12s. All handles terminal.

## Slice138 — optional native journal timings and startup attribution

Actual source changes introduce fixed-size optional native FileLogIo timing
readers and explicit static startup selection, retaining JournalIo results,
durability dependencies, formats and default construction. No callbacks or
persistent metric authority. Default/member/legacy connector paths remain untimed.

Executed2 public-interface timing/failure tests plus9 log-store conformance/crash
tests pass, store execution0.02s after1.74s compile. Native library filter5 tests
pass0.06s after8.08s compile including replacement interruption and saturating
metrics. Example16 tests pass0.03s after7.62s build. All-feature/all-target
check6.69s,Clippy -D warnings8.34s,core-only check3.28s pass. Native-only timing
checks2/2 pass after8.45s build. Release example build14.31s. Initial compile
missed the connector builder's None tuple option; fixed locally. Initial fault
assertion wrongly rejected recovery of complete synced bytes after publication
failure; corrected to the existing unknown-outcome contract. Failed barrier
releases no durability receipt and leaves in-process durable state unchanged.

Sequential unchanged TCP/TLS startup reference workloads on Linux/Btrfs:

- Marked journal diagnostic:46.337179s,5.525ops/s,p99756.731330ms; actual native
  measurement-stage sync means19.64/21.37/22.04ms and full manifest means
  37.53/39.96/39.31ms,512/511/511 calls each. Stable join snapshots retained.
- Separate uninstrumented reference:41.720369s,6.136ops/s,p99734.953838ms.

Both exit0,recover320,verify original retries and join all workers. Both raw
256-receipt history/window/timing checks pass. Journal validator accepts complete
stage arithmetic; fixed acceptance checker refuses diagnostic flag (exit1),
then correctly fails uninstrumented250ms numeric gate (exit1). Empty diagnostic
gate JSON is refusal output. No build overlapped measurement. No preferred rerun,
throughput improvement, sustainable capacity or controlled overhead claim.
Neither run lost leadership; publication is a measured cost, not proof of all
critical-path or earlier leadership-change causes. Timed QUIC and macOS were not
newly executed. Power loss is not inferred from process/file tests.

Three journal-validator tests pass12.85ms; six fixed-gate tests pass11.05ms.
Format/whitespace and80-contract inventory pass. Performance/slice138 retains raw
samples, journal snapshots, source/binary hashes and contexts; public host paths
are redacted and hardware reproduction is limited by omitted private inventory.
Next investigate recoverable bounded manifest publication without dropping required
sync/selection dependencies. Full P0–P7 active, RPL-1.5 retained, pack preserved,
P8/Windows deferred, CI background.

## Slice139 — publication interruption evidence and rejected data-sync experiment

New private native publication helper retains original create/write/full sync/
rename/directory-sync dependencies and adds primitive fault injection for tests.
15 cuts cover initial,legacy and reclaimed manifests, exact selection, unchanged
acknowledged hard state, exclusive lock and session recovery. Missing initial
selection fails closed. No new protocol/provider/format or durability downgrade.

The staging sync_data experiment preserved data/essential-metadata synchronization
and directory/log barriers, but was reverted after an incomplete candidate run.
Primary rationale: Rust File::sync_data and Linux fsync/fdatasync documentation
linked in performance/slice139/README.md. Final code retains sync_all. Historical
experiment.patch and source/binary hashes describe the tested candidate, not the
final source. Control binary hash matched138's executable before copying.

Actual experimental checks: native library7 tests pass0.06s after5.43s build;
store9 and timing2 pass0.01s after8.23s build; Clippy all-feature/all-target
-D warnings6.63s;release build15.93s. After restoration native library7 pass0.05s
after4.04s build; real three-process TCP/TLS leader replacement/original retry/
native-file recovery1/1 passes0.96s after6.96s build. Final store/Clippy checks
recorded below. No claim of physical power-loss, macOS or new QUIC execution.

Sequential marked diagnostic control:256 measured receipts,49.777019s,
5.143ops/s,p991030.629025ms,recovered320,retries verified,workers joined. Raw
receipt and journal-stage validators pass. Candidate exits1 at operation229
Unknown(LeadershipChanged):164 retained receipt rows IDs65–228 independently
pass identity/value/group/index/latency arithmetic; original error, replica states,
journal failure snapshot and workers_joined=true cleanup retained. No complete
candidate summary/performance/recovery result; raw checker correctly rejects it.
Neither election causation nor sync_data unsafety is established. No preferred
rerun or optimization acceptance. Marked diagnostics do not satisfy the original
uninstrumented250ms gate; it and sustainable capacity remain open.

Performance/slice139 retains privacy-safe raw evidence and experiment provenance.
Next actual networked nested retirement closes another P6 gap; P7 remains active
with the entire P0–P7 scope. RPL-1.5 and pack retained; P8/Windows deferred, CI
background. None of these finite checks certifies the full baseline.

Final restored store9/timing2 pass0.02s after4.64s build; final all-feature/all-target
Clippy -D warnings7.96s passes. All handles terminal.

## Slice140 — guarded profile selected before nested network bootstrap

The actual recursive insertion fixture now supports raw and initially guarded
grandchildren31/32 through a test-only profile. No production protocol, provider,
format or implicit migration was added. Exact owner receipt/read extraction
refuses retired/fenced variants. Existing assigned creation bindings and the
complete lifecycle history are shared between profiles.

Executed cargo +stable test --all-features --test routed guarded_nested_insertion
--locked --offline -- --test-threads=1 --nocapture:4/4 pass349.62s after10.70s
compile. TCP/TLS and QUIC, each WAL/checkpoint, cover ten unread lifecycle and
two unread activation abort/reopen/retry cuts per history, quorum fact equality,
no-dual-owner observations, imported operation IDs/outbox, original activation,
immutable creation bindings and successor writes while ancestor files remain
unchanged. Executed the existing raw tcp_nested_insertion_recovers_unread_phases_
from_wal history:1/1 pass84.91s after1.47s compile. Other raw profiles not rerun.

All-feature/all-target Clippy -D warnings passes2.42s; cargo fmt check, git diff
--check and node validation/check-inventory.mjs pass (80 contracts). Initial
compile failures were limited to fixture visibility/query bounds/type inference
and fixed directly. All handles terminal. This is Linux pre-retirement live-guard
evidence, not networked nested retirement/reclamation, macOS, power-loss or broad
protocol proof. Next test actual later transfer and retirement of assigned31;
full P0–P7 and prior gaps remain active, P8/Windows deferred, CI background.

## Slice141 — assigned nested31 network retirement, replay and reclamation

Executed cargo +stable test --all-features --test routed assigned_nested_retirement
--locked --offline -- --test-threads=1 --nocapture:4/4 pass267.38s after14.09s
compile. TCP/TLS and QUIC each cover WAL and older-live-checkpoint retirement-tail
replay for actual assigned grandchild31 after501 split into41/42. Shared guarded
factory is selected before bootstrap; no hot wrapper installation.

Checks include complete quorum publication/activation, missing-target/wrong-freeze
proof refusal, accepted client request still charged/unread during native owner
abort/join, exact retirement R/freeze/activation lineage on all replicas and quorum
reads/retry, absent owner/refused exports/reads/data, immutable creation binding,
pinned retired checkpoint and exact reclaim tickets/reduced WAL bytes, no retained
application commands, and retired checkpoint reopen. Metadata/ancestors/31 stop
while41/42 and sibling32 serve imported retries/new writes with exact values/outbox
and unchanged stopped files. Final31 reopen preserves the retirement after writes.

Existing raw tcp_nested_later_moves_recover_unread_phases_from_wal:1/1 passes
163.33s after1.13s compile, covering split/merge20-phase abort/reopen/retry through
the shared fixture. Other raw combinations not rerun. All-feature/all-target
Clippy -D warnings passes5.06s; cargo fmt check, git diff --check and80-contract
inventory pass. One compile-only generic restart factory mismatch was fixed at
its cause. All handles terminal. Selected Linux histories do not establish both
merged-source cleanup, general retention registry, physical power loss, macOS,
separate-host or broad protocol proof. Full P0–P7 and previous gaps remain active;
P8/Windows deferred; CI background; no performance claim.

## Slice142 — independent retirement of nested merge sources

Executed cargo +stable test --all-features --test routed nested_merge_retirement
--locked --offline -- --test-threads=1 --nocapture:4/4 pass403.86s after9.20s
compile. TCP/TLS and QUIC, WAL/checkpoint, follow actual assigned ancestry through
501 split/601 merge and retire41/42 independently into43. Source-swapped release
refusal, complete quorum publication/activation and distinct release IDs are
checked. Each accepted retirement stays unread during native abort/join; recovery
preserves exact R/freeze/original activation lineage and original retirement retry.

Full restart after only41 retirement retains41's tombstone and42's live fenced
owner/activation. Fresh quorum facts rebuild42 proof;43 serves imported original
retries and a new write before42 retirement. Both old live checkpoint/tail replays
and retired checkpoint/reclaim reopen are exercised. Exact Node requests report
reduced WAL bytes; retained logs have no application commands after retired bases.
With ancestors and both old sources stopped,43/sibling32 retain imported IDs,
values/outbox and serve new writes with unchanged stopped files. Final old-source
reopen remains retired. Selected41-first order only; broader schedules remain.

Shared141 helpers now use exact source IDs and explicit immutable startup templates,
with mandatory assigned31 binding checks. Executed tcp_assigned_nested_retirement
filter:2/2 WAL/checkpoint regressions pass130.27s after4.30s compile. Other141/raw135
combinations not rerun. Initial Clippy all-feature/all-target -D warnings passes
1.98s; final after strict binding assertion passes3.98s. Format/whitespace and
80-contract inventory pass. All handles terminal. Production unchanged; no general
retention/migration, physical power-loss, macOS, separate-host, arbitrary-fault or
performance proof. Full P0–P7 and previous gaps active; P8/Windows deferred, CI
background. BASELINE_ACCEPTANCE.md updated to distinguish these selected native
cleanup histories from remaining requirements.

## Slice143 — protected shared encoded frame control reserve

Production BufferPool contract2/native pool/native transport now support explicit
control byte/lease headroom. Shared bulk accounting cannot consume it; actual
validated batches select Control, mixed/data/background and unclassified receives
use Bulk. No reserve is silently inferred or enabled by service startup. Legacy
native pools retain total-only counter operations; unsupported declared reserve
implementations fail closed. Constructor validates full-duplex bulk capacity plus
one protected max-send. Wire/persistent formats and consensus rules unchanged.

Executed all-feature buffer7/transport22/wire14 tests:0.03/0.04/0.60s after10.66s
build. Public independent host/native cases cover bytes/slots saturation, legacy
fallback/declaration refusal, close/resize/drop, exact partial-total/overflow
rollback, eight concurrent held bulk owners, mixed-batch classification, blocked
unclassified receive, control send under saturation, short writes/delayed flush,
abort and separate queue/frame credit return. Existing real TCP/TLS transport
regression is included; shared-reserve sessions are trusted host test attestations,
not cryptographic evidence. Added malformed host declaration test1/1 passes0.00s
after7.42s build, before any lease/plaintext and with consumed-session drop. Real
QUIC default framed transport credit test1/1 passes0.02s after6.19s build.

Core-only buffer3/transport1/wire1 pass after5.24s build. All-feature/all-target
check7.77s, initial Clippy -D warnings11.33s and final5.26s pass. Format/whitespace
and updated80-contract inventory pass. Unsupported two-filter Cargo command did
not execute tests; corrected separate commands above provide the actual evidence.
Inventory array lookup correction preceded its successful write/check. All handles
terminal. No new startup/CLI reserve option or selected-reserve Node load test, encrypted
shared load/performance, full per-owner/receive/connection fairness, macOS or general
fault proof. Full P0–P7 active; broader ledger and fixed250ms/sustainable-capacity
P7 gaps retained; P8/Windows deferred, CI background.

## Slice144 — owner-aware encoded-frame budgets

C14 contract3 selected quotas are non-overbooked and bounded to1024 live node/store
owner registrations. Authenticated native transport binds before frame admission;
reconnects share existing held quotas independent of connection generation. Native
and independent downstream providers retain registration through every accepted
frame and charge only Bulk against owner counters. Global reserve behavior and
legacy defaults remain. No wire/persistence/core change or new durability fact.

Actual all-feature buffer13 pass0.01s; wire14 pass0.44s. Transport initial25 had
24 passing host/native tests and one sandbox PermissionDenied loopback bind;
unchanged real TCP/TLS test separately passes0.02s with socket permission. Selected
owner test histories show unrelated-peer Data delivery under held hot-peer quota,
short I/O/delayed flush, reconnect, Control reservation, refused original batch
and wrong-owner constructor/session-drop cleanup. A final added full-duplex owner
quota refusal test is separately recorded below. Default QUIC frame regression1
passes0.01s. Buffer tests include held-registration/control lifetimes, byte/slot
pressure, overflow/non-overbooking, allocation-failure rollback, exact cleanup and
eight concurrent same-owner holders alongside unrelated owner progress.

Core-only buffer6/transport1/wire1 pass0.00s. All-feature/all-target Clippy -D
warnings8.30s and final0.52s pass; format/diff and80-contract inventory pass.
Initial nested StoreBinding field access and fixture integer-type compile errors
were corrected locally before successful tests. No selected encrypted Node load,
startup policy option, per-group/receive/connection fairness, performance, macOS or
universal failure proof. All prior phase gaps remain and the full P0–P7 goal is
active; P8/Windows deferred, CI background feedback.
Final selected owner full-duplex byte/slot constructor refusal1/1 passes0.00s
(after1.38s build), no frame leases. All execution handles are terminal.

## Slice145 — real encrypted Node shared-quota history

Actual native Node/TCP/TLS100-group assembly selects finite per-node shared pools:
5 max-frame byte reservations/5 slots, one Control reserve, two owners each with
2-frame/2-slot Bulk ceilings. A full host-held peer quota survives explicit
node1↔node3 disconnect and real TLS reconnect with a different session generation.
Eight groups commit writes10 then14 and perform quorum-backed reads through the
other peer; the pressured follower is explicitly still7. Staged outbound work and
sampled bounded byte/slot counts are checked. Release permits actual catch-up14.
Explicit Node shutdown/dial/WAL/snapshot worker joins return every frame credit;
a surviving host view remains usable. Native durable reopen changes StoreSession,
original operation retries return duplicate receipts without changing14, and a
new write/read reaches15 before the second zero-frame joined shutdown.

The first attempt failed its stalled-follower assertion in5.21s: half a quota
plus a temporary idle receive is not sustained saturation because decoded receive
returns credit. Corrected full-quota setup is recorded before the focused edit;
production budgeting/consensus/storage/timers were unchanged. Corrected test1/1
passes12.18s after2.66s build. Existing default hundred-group owning-facade
checkpoint/maintenance/restart/retry regression1/1 passes23.22s. All-feature/
all-target Clippy -D warnings passes3.67s. Native alias factory selection is
backward compatible with existing default Node/startup assemblies.

One finite Linux TCP/TLS schedule, not selected QUIC pressure, arbitrary resource
fairness/fault proof, a performance result, macOS or separate-host execution.
No automatic CLI quota selection, new durability token, protocol generation,
watermark, wire/persistent format or consensus effect. Full P0–P7 remains active
with all prior unresolved acceptance requirements; P8/Windows deferred.
Final cargo formatting, explicit included-fixture rustfmt, whitespace and
80-contract inventory checks pass. All execution handles terminal.

## Slice146 — discovered QUIC dial endpoint selection

NativeQuicConnector::new_with_discovered_dials uses validated selected Dial
addresses through existing public DiscoveryConnector/PeerDiscovery. Exact pinned
certificate/store, ticket generation and deadline gates remain. Static new and
Accept selection remain provisioned. Native socket mailboxes enforce one live
lease per provisioned peer as well as endpoint, preventing changed addresses from
accumulating retained sessions. Accepted sessions are not migrated or preempted.

Actual QUIC connector7/7 pass0.06s after1.72s build: new stale-hint timeout/refresh,
authenticated bidirectional data despite stale original input, changed-address
held-session overload with original request, unconsumed-generation retry after
release, wrong certificate rejection, invalid endpoint/self/family refusal and
existing static/cancellation/version/retained-session checks. Added independent
host PeerDiscovery→native QUIC test1/1 passes0.01s after2.14s build. Transferred
sessions survive connector close/drop and final session drop frees bound ports.
Mailbox ownership/routed packet bounds/generation-cleanup unit1/1 passes0.00s
(after11.47s build including concurrent Cargo locks). Existing real three-replica
QUIC framing/exact log completion regression1/1 passes0.01s after5.65s build.
Clippy all-feature/all-target -D warnings passes3.78s; final check below covers
last host test. Initial fixture-only ConnectDirection equality compile failure
was fixed using matches; no enum/API change or production test failure. A failed
documentation patch context produced no write and was corrected locally.

Selected real Linux connections, not automatic manifest fetching, executable
hot endpoint refresh, Accept-address refresh, live migration, arbitrary-fault or
performance evidence. No new core/storage/crypto protocol or durability fact.
Full P0–P7 and earlier missing scope remain active; RPL-1.5 retained, P8/Windows
 deferred and CI background feedback.
Final Clippy all-feature/all-target -D warnings passes1.08s after the host test.
Formatting/whitespace and80-contract inventory pass. All execution handles terminal.

## Slice147 — bounded automatic original Directory reads

ManifestReadSource version1 and NativeManifestLookup use exact original Node
read ownership with one pending lookup, bounded observation cache and one retry
slot. Explicit Node/driver polls replace manual observe; no hidden runtime or
remote credential is created. Deadline/cancel/close retain accepted work and
suppress late publication; recovery returns the original unresolved ticket/source.

Final real TCP/TLS and QUIC automatic lookup tests2/2 pass0.70s after6.11s build:
missing recursive route resolution, no duplicate accepted read, other-query
overload, expired unchanged metadata/new observation, demanded epoch refusal,
real host-retained positive completion after cancellation, retry without cached
cancelled result, original read-credit recovery and joined native cleanup.
Independent host lifecycle3/3 and all-feature routing13/13 pass0.00s after6.09s
build; core-only source1/1 and routing9/9 pass0.00s after5.13s build. Lifecycle
checks cover deadline retention, negative retry, closed-view terminal drain,
nonquiescent constructor refusal, time/binding refusal and exact recovery handoff.
All-feature/all-target check passes6.12s, initial library Clippy -D warnings3.74s,
final all-feature/all-target Clippy -D warnings3.70s. Fixture compile errors used
wrong existing protocol variant/observation field names; corrected locally with
no production redesign. Initial real tests2/2 passed0.61s before expiry addition.
80-contract inventory passes; final formatting/whitespace checks recorded below.

Finite Linux native and independent host histories, not arbitrary-fault proof,
external remote fetch transport, signed read authority, executable endpoint
refresh, macOS execution or performance evidence. Cached child routes retain
parent independence; hints do not activate ownership. Full P0–P7 stays active,
P8/Windows deferred, pack preserved and RPL-1.5 retained.
Final cargo formatting, included-fixture rustfmt and whitespace checks pass.
All test/check handles are terminal; no remote CI gate was used.

## Slice148 — automatic lookup/routed service composition

Real native TCP/TLS and QUIC tests2/2, comprising four WAL-only/checkpoint histories,
pass3.08s after4.98s build. Empty-cache original Directory reads resolve recursive
child routes. Exact source observation/cache manifest invalidation requires a
fresh actual read/new observation and preserves the unchanged checked route.
After all metadata Nodes close/drain/join, a zero-budget cached child resolution
uses an independent provider that panics on any source call; actual child write7,
original duplicate7 and quorum read7 succeed. Child checkpoint/WAL reopen/retry
preserves that value. Exact parent GroupLogs remain unchanged during outage.

Directory restart supplies a fresh original read session matched against the same
store identity, not merely a different leader. A newly empty route cache resolves
through original read polling again; exact grant/hint survive. Reopened child
returns original duplicate7, then new+3 commits10 and quorum read10. All native
owners drain/join before inspection/removal. These tests use actual WAL sync,
application receipts and read barriers, not local send completion as evidence.

Initial composed tests2/2 pass2.80s after4.55s build. Added explicit invalidation
checks2/2 pass2.98s after3.56s build. Final all-feature/all-target Clippy -D warnings
passes5.26s including lock wait; earlier3.09s. No production/test failures. Inventory
80 and whitespace checks pass; final formatting checks recorded below.
No new production provider/API, protocol token/generation, persisted format,
external remote lookup endpoint, arbitrary-fault or performance claim. This is
finite Linux service composition, not macOS/separate-host execution or complete
recursive lifecycle acceptance. Full P0–P7 and prior unresolved work stay active.
Final cargo formatting, included-fixture rustfmt, whitespace and80-contract
inventory pass. All execution handles terminal; no remote CI gate used.

## Slice149 — schema7 cross-authority nested insertion

Explicit public cross-authority plan, contract5 intent and opt-in Directory schema7
preserve older schema/tag admission and replay. Separate parent and child metadata
application states exercise local route/reservation and child creation checks.
Foreign original observations remain authenticated trusted host provenance,
not transferable certificates. This does not move a metadata authority.

Deterministic new2/2 pass0.01s after1.62s build. Relevant all-feature delegation14/14
pass0.07s and insertion10/10 pass3.22s after2.86s build, including new native ModelIo
intent-frame byte cuts and sync/manifest publication faults. Recovery proves old
or exact complete intent/local creation state; intent cannot publish descendants.
Added source/non-serving import checkpoint, cancellation and new successor effects
are covered by new focused4/4 pass0.39s after1.43s build, then successor-write1/1
pass0.01s after2.17s build. Source stays fenced; targets refuse before activation,
preserve duplicates7/11 and original outbox items after activation/checkpoint,
and new writes read8/12. Parent/child decision and cancellation locks reconstruct
exactly through schema7 checkpoints. Truncation/tag downgrade, unknown local
creation/mismatching references, legacy schema refusal and live upgrades refuse.

Initial fixture compilation used wrong public receipt/read shapes and a copied
path attribute; fixed against actual contracts. The added cancellation test first
failed because it used the child operation's ID for a separate decline command;
existing decline rules correctly rejected it. The focused distinct-ID correction
passes without production change; later successful intent declines stay refused.
All-feature/all-target check4.94s, final Clippy -D warnings1.66s, core-only library
check3.89s pass. Final formatting/inventory/whitespace checks recorded below.

Application-level deterministic/checkpoint and native I/O-model fault evidence,
not actual networked foreign quorum observations or physical power-loss proof.
Native TCP/QUIC separate-authority phase/reopen histories are next150. No global
foreign-ancestry proof, mixed-binary compatibility, retained-scope insertion,
macOS execution, separate hosts or performance result claimed. Full P0–P7 stays
active with earlier ledgers, P8/Windows deferred, RPL-1.5 and original pack retained.
Final cargo formatting, included-fixture rustfmt, whitespace and80-contract
inventory pass. All execution handles terminal; no remote CI gate used.

## Slice150 — real cross-authority insertion phase histories

Linux loopback native TCP/TLS and QUIC with WAL and checkpoint recovery exercise
separate metadata quorums, original schema7 local creation provenance, partially
established Staging assignments and exact binding reuse. Each accepted reserve,
intent, stage, source fence, import, child publication and parent refresh leaves
its original application completion unconsumed until owner abort. Exact original
quorum statuses and command retries survive reopen. No terminal-envelope polling
is confused with application result consumption or cancellation rollback.

Stale parent locator and unactivated targets refuse service. With metadata/source
owners stopped, both targets activate, lose original result observation, reopen
and exactly retry activation. Imported operations return duplicate7/11 with one
outbox item; fresh writes/quorum reads yield9/13. Original assignments stay exact.
Stopped metadata/source files, including logs, remain byte-for-byte unchanged;
recovered source still refuses the stale grant. Owners are joined before cleanup.

Initial fixture compile mismatches were corrected against existing contracts.
The first native run refused creation admission as NotLeader: campaign the actual
child before original/retry submissions. The next compared omitted dormant target
owners with correctly reopened assigned owners: expose the same uninitialized
owners after original reservation. Exact equality and consensus admission remain.
Corrected initial TCP/WAL1/1 passes57.88s after6.01s build. Final explicit original
phase retry matrix4/4 passes306.38s after7.70s build. Existing default TCP delegated
split WAL1/1 passes57.34s. Final all-feature/all-target Clippy -D warnings7.55s,
cargo formatting, included-fixture rustfmt, whitespace and80-contract inventory
pass. The matrix serializes socket histories; observation timeouts did not restart
live test processes. All execution handles are terminal.

Finite selected owner-abort/checkpoint histories, not arbitrary faults, physical
power-loss proof or global authenticated ancestry certificates. No production
protocol/provider changes, retained-scope feature, macOS execution, separate-host
or performance claim. Full P0–P7 and prior open gates remain active; current151
implements retained scope, next152 native composition and following153 deletion.
Windows/P8 remain deferred, CI background, RPL-1.5 and original pack preserved.

## Slice151a — scoped fencing with continuing retained service

Opt-in pristine RoutedApplication schema2 binds scoped history capacity in
VBROWN02. VBRSCF01 fences one exact locally owned range forward-only and emits its
original scoped operation/index/epoch fact only through applied receipts. VBROUT02
reconstructs exact facts without live/profile upgrade. Default schema1 stays
separate; existing whole-source TransferSource refuses the scoped profile.

Native BucketCounter data/retry/outbox histories preserve transferred7 while
retained11 becomes13 then16, surviving replay and checkpoints. Transferred reads/
writes, overlaps, wrong owners/epochs, control/data ID conflicts and contradictory
snapshot facts refuse. Pending admission orders actual scoped state transitions,
checks host admission and bounds, and reserves separate control capacity even
when ordinary dedup history is full. A later full fence stops the remainder.
Independent host heap receipts/queries/read results retain nested byte accounting.
Native ModelIo cuts every scoped-fence frame byte and injects sync and manifest
publication failures. Recovery is old state or exact complete fence; retained
writes remain available in both. This is finite native I/O-model fault evidence.

Initial constructor trait access/exhaustive-arm compile mistakes and a missing
fixture identity import were corrected directly against existing contracts.
Initial focused3/3 pass0.33s after14.11s build. Added native faults/local range
checks5/5 pass0.22s after24.78s build including lock wait. Core-only routed11/11
and transfer_source7/7 pass0.28s/0.45s after5.18s build. Clippy flagged duplicate
fixture/support loads and redundant allowance: share original root modules,
no suppressions. Native without TLS5/5 pass0.35s after8.93s build. Clippy7.80s
then passes. Added host check initially11/12 passes with one fixture error: original
grant0..128 was entirely fenced and key200 lay outside it. Freeze0..64 and retain
key100 within that grant; no production owner check changes. Final core-only
routed12/12 and source7/7 pass0.35s/0.56s after1.15s build. Final Clippy and metadata
checks follow below. No unchanged broad network/performance run is added.

Partial151 ownership guard, not complete insertion: immutable original-F exports,
checked mixed retained/delegated intent/publication and target activation remain
current151 work. Next152 is real native partial-transfer composition; following153
is bounded namespace deletion. Scoped facts are neither whole-source fence receipts
nor external certificates. Full P0–P7 remains active with original gaps, macOS
execution pending, Windows/P8 deferred, CI background, RPL-1.5 and pack preserved.
Final151a all-feature/all-target Clippy -D warnings passes1.31s. Cargo formatting,
included-fixture rustfmt, whitespace and80-contract inventory pass. All handles
terminal; no CI gate used. These checks do not complete retained-scope insertion.

## Slice151b — immutable original-F scoped source exports

Public ScopedTransferSource uses151a selected RoutedApplication and existing
ScopeStateMachine with explicit aggregate capacity and canonical VBSCOWN1/VBSCCHK1
bindings. Capture schema/scheme/range/F-correct immutable provider output exactly
at each scoped fence before publishing the owned candidate. Later retained
writes/retries/noops cannot change that image/digest. Per-image configured lifetime
reservations survive checkpoint/clone capacity shrink and preserve admission
headroom. Fixed original status uses the existing Node quorum-read seam; export
returns local data, not a transferable certificate or target authority.

Native BucketCounter F4 exports value7 while retained11 becomes13 then16; snapshot
and replay retain original image/digest, capacity reservation and fenced refusal.
Staging provider import preserves original duplicate operation1/outbox. Padded
independent host images keep stable headroom after recovery; wrong-F host output
fails atomically. Pending aggregate capacity, exact control retry, data/provider
key/ID conflicts, every checkpoint truncation and altered image/profile/op/F/
reservation/digest refuse appropriately without publishing a malformed fact.
Native ModelIo scoped-fence frame byte cuts plus sync/manifest publication faults
recover old state or exact complete scoped fence and image together, with retained
service. No source retirement/automatic image cleanup or new storage owner exists.

Initial all-feature check6.77s passes. Reservation refactor scope-variable mistake
and missing restored cached-digest initializer were corrected locally. A const
reassignment stopped one file-edit script after its source save; finish the test
edit with a targeted patch. Initial4/4 pass0.30s after8.92s build; later5/5 including
host provider pass0.24s after12.05s build/lock wait. Core-only new4/4, scope adapter7/7
and whole-source7/7 pass0.27/0.09/0.57s after4.50s build. Clippy all-feature/all-target
-D warnings10.40s passes. Added inflated-reservation refusal1/1 passes0.22s after
0.95s build; subsequent Clippy0.96s passes. Final whole-fence export-retention and
metadata checks follow below. No unchanged broad socket/performance suite added.

Finite deterministic/provider/native-I/O-model evidence, not complete retained
insertion or arbitrary faults/physical power loss. Checked mixed intent/publication,
retained-grant adoption and target activation are current151;152 native networked
composition and153 deletion follow. Full P0–P7 and prior gaps remain active; macOS
execution pending, Windows/P8 deferred, CI background, RPL-1.5/pack preserved.
Final151b whole-fence export retention/snapshot recovery1/1 passes0.00s after2.17s
build. Final all-feature/all-target Clippy -D warnings0.69s, formatting, whitespace
and81-contract inventory pass. All execution handles terminal; no CI gate used.

## Slice151c — retained-child metadata publication and target activation

Schema8/VBTINT06/VBDPLAN4/VBTPUB02 bind an explicit one-fresh-child subrange
transition; old directory schemas/formats remain selected separately. Actual local
Staging creation and original scoped source image/fence/import evidence precede
atomic mixed manifest publication. Deterministic root and foreign-parent nested
handoffs preserve real BucketCounter values/retries/outbox through checkpoint/
replay and activation. Fresh targets never serve before activation; retained source
still serves its E1 grant and rejects fresh E2 hints until next151d checked source
binding/adoption. No complete partial service or foreign certificate is claimed.

Every new codec truncation and old-tag downgrade refuses. Schemas1–7 reject plain
and wrapped partial commands without checkpoint changes. Unknown actual creation,
changed residual owner and full-source evidence substitution refuse. Existing
manifest validation rejects duplicate fragmented child selectors; an initial test
incorrectly unwrapped that known rejection and was corrected at the fixture only.
Pre-intent decline/cancel and exact recovery/retry pass; accepted intent remains
forward-only. Native ModelIo every intent-frame byte and sync/manifest faults
recover original creation plus no intent or its exact durable record, never child
publication; subsequent exact original retry establishes/returns that record.

Existing delegation14/14 and insertion11/11 pass0.06s/3.19s after8.48s build.
Post-bootstrap-reader correction new2/2 and publication7/7 pass0.00s/0.04s after
3.37s build. Expanded namespace12/12, retained4/4 and publication7/7 pass
0.57s/0.25s/0.04s after3.91s build. Final added cancellation retained5/5 pass
0.16s after1.11s build; core-only final4/4 pass0.02s after1.16s build.
Final Clippy all-feature/all-target -D warnings passes1.54s (previous10.34s).
Formatting, whitespace and81-contract inventory pass. All process handles terminal.
Finite local application/I/O-model evidence; native network composition152,
retained source binding/adoption151d and original broader P0–P7 gaps remain open.
No unchanged socket/performance loop or remote CI gate was added. Linux checks;
macOS execution pending, Windows/P8 deferred, RPL-1.5/ignored pack preserved.

## Slice151d1 — original retained intent bound at scoped source freeze

Opt-in ScopedTransferSource schema2 selects canonical VBTINT06 freeze and explicit
bootstrap/checkpoint tags2. Exact source grant/group/range/control and provider ID
checks precede atomic original intent/digest/fence/image publication. Raw scoped
fences/bootstrap bypasses and old profiles refuse. Fixed original quorum status
includes an optional intent digest; publication checks any present digest. Default
raw schema1 remains unbound, separately selected, and supplies no adoption authority.
Independent cached intent digest and original scope/F/image metadata validate
checkpoint links. Maximum configured intent/digest storage participates in the
64MiB combined deployment ceiling. No new runtime/persistence provider is added.

Actual root/foreign nested data handoffs now use bound original source facts.
Changed original retry, every bound snapshot truncation, changed creation reference,
old profile, live selection, combined capacity overflow and creation/control/data
ID collisions fail closed. Ordered pending collision refuses without effect; actual
committed collision cannot freeze/export. Future data/full-fence commands cannot
reuse original creation IDs. Native ModelIo cuts every bound intent-frame byte and
injects sync/manifest failure: committed-prefix recovery yields no binding/fence/
image or their exact original combined fact, while retained work continues.
Existing raw source and independent padded/wrong-boundary host provider checks pass.

An initial check called a nonexistent routed method; use its existing internal data
history/initialization/fence checks. Review added independent cached intent digest
before accepting a valid-shaped changed creation reference on checkpoint restore.
Expanded all-feature retained7/7 and raw source5/5 pass0.31s/0.18s after4.14s build.
Core-only retained5/5 and source4/4 pass0.04s/0.27s after4.72s build. Added bounded
budget/pending/profile assertions targeted1/1 pass0.03s after0.65s build. Final
all-feature/all-target Clippy -D warnings passes1.49s (earlier7.81s). Formatting,
whitespace and81-contract inventory pass; all handles terminal. No unchanged socket/
performance suite or remote CI gate is added.

This completes the bounded source-binding step, not retained-grant adoption or a
complete partial service. Current151d2 must retain frozen authority/history/images
while accepting the new grant epoch; next152 native TCP/QUIC composition, following
153 deletion and full original P0–P7 scope remain active. Linux local evidence,
macOS execution pending, Windows/P8 deferred, RPL-1.5/ignored pack preserved.

## Slice151d2 — checked retained owner grant adoption

Opt-in source schema3 retains an active grant and bounded original adoption ledger
alongside the existing original routed history guard, over one authoritative ordered
application prefix. Data/read contexts pass active ownership first; historical
projection then preserves original provider retry state and permanent scoped fences.
VBSADP01 checks actual supplied metadata decision/configuration against original
locally bound intent/digest/fence/image and current before-grant. Authenticated
metadata quorum provenance remains a host obligation, as for target activation.
Only committed application execution permits the grant change/GrantAdopted receipt;
provider noop advances at that same index. No ancestor dependency, new persistence
owner, clock or runtime is introduced.

VBSCOWN3/VBSCCHK3 preserve separate profiles1/2 and reserve maximum original decision/
record digest storage per fence within64MiB. Independent cached digests bind original
operation/index/command; restore validates source facts, ordered grant chain,
original grant at each fence, known semantic/index/control collisions and capacity.
Frozen facts keep original intent epochs; full fences report their active original
epoch. Original images cannot be cleared/thawed by adoption. Source grant/context
are active; routed() remains the original bootstrap/history diagnostic.

Actual root and foreign nested paths adopt E2 and retained data13→16; stale and
transferred contexts refuse. Source imageF4/value7 and original freeze/adoption
retries survive checkpoints. Actual second Staging child22 moves128..192, preserving
child21 and the old export; provider value5/retry/outbox import, metadata publication,
activation and E3 adoption precede retained service and original operation2 retry.
A subsequent full fence/checkpoint preserves both original epochs/exports.
New codec/snapshot truncation, changed ledger/index, old profile, missing original
fact, live selection, combined budget, ID collisions and post-full-fence adoption
refuse. Pending adoption admits fresh E2 and refuses E1 without changing source.
Native ModelIo every adoption-frame byte and sync/manifest faults recover E1 or
exact E2 grant/status with original F4 image; retained writes, original receipt-loss
retry and another checkpoint preserve exact facts. Finite model/application evidence;
complete networked partial service remains152, broader lifecycle/platform/fault
and original P7 gates remain open.

Initial check assembly errors (misplaced match return/private selector/new query
arms) were fixed directly; all-feature/all-target check passes5.91s. First7/7 and
repeated8/8 pass0.51s/0.47s after8.61s/6.26s builds. Adoption fault test miscounted
new adoption after prefix5 plus one write as8; it is7. Correct that fixture only.
A command already running before correction repeated that failure. Expanded10/10
then pass1.60s after0.89s build. An inadvertent full routed invocation without
socket escalation passed13 application checks but failed89 native TCP/UDP endpoint
setups. Narrow actual required verification: application13/13 pass0.35s after0.02s
build; relevant legacy TCP source recovery with socket permission1/1 passes0.23s
after0.74s build. No unchanged full socket suite is rerun or claimed passed.
Core-only retained7/7, routed12/12, raw source4/4 pass0.13/0.34/0.27s after5.07s build.
Final changed budget/pending/full-fence assertions: all-feature retained10/10 and
raw source5/5 pass1.64/0.28s after6.70s build; core-only targeted1/1 passes0.12s after
4.14s build. Final Clippy all-feature/all-target -D warnings passes6.83s (earlier
10.29s). Formatting, whitespace and81-contract inventory pass. All process handles
terminal; no CI gate used. Linux evidence, macOS execution pending, Windows/P8
research deferred, RPL-1.5/ignored original pack preserved.

Checked bounded application path151 implemented; current152 is TCP/QUIC partial
phase recovery/offline service, next153 deletion, following154 reparenting. Full
P0–P7 stays active, including target-backed partial sources/general mappings/
retention and wider membership/metadata movement/platform/fault/P7 requirements.

## Slice152a — native retained root service phase recovery

Directory8, ScopedTransferSource3 and assigned TransferTarget3 compose over actual
three-replica TCP/TLS and QUIC nodes. Original metadata quorum creation authorizes
two-store partial Staging assignment, metadata WAL/checkpoint reopen, exact original
creation retry and final assignment. Original immutable binding bytes persist.
Intent/stage/freeze/import/publication/adoption/activation each leave the original
application completion unconsumed with its client credit owned until explicit abort.
Native workers join before original stores/factories reopen. Actual quorum-read
manifest/intent/Frozen/Grant/target facts determine progress, compare exactly after
recovery, and survive original command retries. No production protocol/provider
change or invented commitment/fence/import certificate is introduced.

Before activation child reads refuse; source freezes only the moved range and
retained key200 still serves11. E2 adoption permits retained service and refuses
stale E1 and moved-range requests without credit leaks. Metadata shutdown/join
precedes target activation retry, transferred original retry7/outbox and fresh
write9, source retained original retry11 and fresh write14. Independent source/
target recovery with metadata still stopped preserves14/9, original frozen image/
digest and original adoption status. Exact stopped metadata files and recovered
GroupLogs remain unchanged. All owners abort/join before temporary cleanup.

Fixture compilation corrected CreationBindings trait and native Node/Core config
API names. Source review corrected activation encoding to use the actual imported
target rather than a fresh factory. First selected TCP WAL1/1 passes12.17s after
8.76s build. Added explicit native moved-scope query/proposal refusal; final selected
TCP/QUIC x WAL/checkpoint4/4 passes53.62s after4.60s build with required loopback
permissions. All-feature/all-target Clippy -D warnings passes2.16s. Formatting,
whitespace and81-contract inventory pass. All handles terminal; no CI gate or
unchanged broad network/performance suite used.

Selected native root evidence, not a complete recursive lifecycle/platform/fault
proof. Foreign-parent152b remains current within152; next153 deletion, following
154 reparenting and original full P0–P7 scope remain active. Broader target-backed
partial sources/mappings/retention, membership/metadata movement/platform/fault/P7
requirements stay open. Linux local evidence, macOS execution pending, Windows/P8
deferred, RPL-1.5 and ignored original design pack preserved.

## Slice152b — native foreign-parent retained service recovery

Extended the existing retained fixture through explicit original factory selection:
parent100 owns an existing locator, child metadata1 reserves/assigns fresh target21,
and source20 preserves its retained range. Actual reservation400 produces canonical
bound intent200 and is WAL/checkpoint reopened/retried before target construction.
The unread intent/stage/freeze/import/publication/completion/adoption/activation
histories compare exact original quorum observations after joined reopen and exact
command retries. Parent completion401 updates the child locator epoch and parent route generation,
checked against the complete expected parent manifest; own parent/epoch stay intact.
Original creation records/assignment binding bytes remain unchanged through phases.

With child and parent metadata both stopped/joined, retained and target data/retries
continue and independently recover values14/9, original frozen export/digest and
adoption. Original target transferred retry/outbox semantics remain checked.
Stopped metadata files and recovered GroupLogs compare unchanged for both groups.
Root selection remains separate, and no new production/provider/persistence owner
was introduced. No fake commitment or normalized facts replace actual observations.

Actual local Linux checks: compile-only all-feature routed target passes6.75s;
initial foreign TCP/WAL1/1 passes33.03s; final foreign TCP/QUIC x WAL/checkpoint4/4
passes135.83s; root TCP/WAL regression1/1 passes11.71s. Native execution used required
loopback permissions. All-feature/all-target Clippy -D warnings passes1.90s;
formatting/diff and81 implemented contract inventory pass. All process handles
terminal. No failed test or broad unchanged network/performance rerun this slice.

Selected direct-parent retained path, not universal recursive lifecycle/fault proof.
Current153 deletion, next154 reparenting, following155 target-backed partial sources;
general mappings/retention, metadata authority movement, wider membership, macOS and
original platform/fault/P7 requirements stay open. Full P0–P7 remains active; Windows
and P8 deferred, RPL-1.5 and ignored original pack preserved.

## Slice153a — checked recursive deletion and retained tombstones

Pristine Directory schema9 binds VBDINIT9/VBDIR009 and adds VBDDEL01 original
manifest reservation plus VBDDCM01 checked completion. Existing log/checkpoint
history owns deletion locks, original intent/status and tombstones. Original full
owner fence observations and fixed published child tombstone projections cover
exact direct owners/selectors. Same-authority child facts require actual local
records; foreign facts still require authenticated original quorum/configuration
provenance from the host. These types/digests are content bindings, not certificates.

Deletion reserves bounded control capacity before owner fencing, retains Fenced
manifests at the original ownership epoch and next generation, preserves IDs and
original retries, and blocks incompatible publication/transfer/delegation/creation.
Unresolved creation or lifecycle reservations refuse deletion. Ordinary capacity
exhaustion cannot prevent an otherwise valid reserved completion. No physical
reclamation/expiry, cancellation, replacement log or native provider is introduced.

Actual conformance: original routed-owner full fences, retained data/outbox and
checkpoint fence recovery, leaf deletion, same/foreign child deletion, a three-
authority bottom-up chain, partitioned scopes with repeated direct groups,
delegation/deletion ordering and failed-batch atomicity. Missing/wrong/duplicate
coverage, wrong original operations/epochs, profile/count/capacity/pending/boundary
and all-cut codec/checkpoint refusals are exercised. Native ModelIo tests every
byte append interruption plus synchronization and before/after manifest publication
faults separately at intent and tombstone journal frames. Recovered committed
prefix is either the original state or the exact complete next transition;
checkpoint and exact original retry then preserve facts/outcomes.

Final all-feature deletion10/10 passes0.18s; core-only9/9 passes0.01s after1.77s
build. Existing relevant application suites all pass: directory31, delegation14,
insertion11, namespace_creation12 and retained_insertion10. Final all-feature/
all-target Clippy -D warnings passes2.64s. Formatting, whitespace and81 implemented
contract inventory pass. Clippy initially caught a fixture Vec used only as an
array; changed only that allocation. Added ordering fixture initially attempted
unsupported Single-to-Single transfer, which the existing contract correctly
refused in both feature runs. Inspected TransferIntent::validate_shape and changed
that fixture to the accepted two-target split. Focused ordering1/1 and final full
feature/core deletion suites pass; production safety checks were not weakened.
A final manifest-cache source review confirms same-epoch Active-to-Fenced newer
routing generations are accepted and Fenced routing refuses service.

This is finite application/native-journal evidence, not actual networked recursive
deletion or an arbitrary-fault proof. Current153b supplies TCP/QUIC WAL/checkpoint
unread-phase resumption and actual quorum owner facts. Wider owner families,
retention/cancellation, metadata movement, reparenting154, partial sources155,
original membership/platform/fault/P7 gates remain active. Linux local evidence;
macOS execution pending, Windows/P8 deferred, RPL-1.5 and ignored original pack
preserved. No CI gate or unchanged benchmark suite blocked this feature work.

## Slice153b — native recursive deletion and original full-fence quorum reads

RoutedControlReads adds Data/Fence queries over the existing checked routed owner.
State/admission/deployment/receipt/checkpoint paths delegate to the original owner;
commands, initialization, application schema and checkpoint bytes stay identical.
Fixed fence results are charged inline with no nested allocation, preserve original
operation/index/epoch and use the existing Node quorum read contract. Local
read_at/fence diagnostics alone establish no foreign commitment. A scoped fence
never becomes full-owner evidence. Host nested query/result capacity and admission,
NotApplied/group bounds and plain/view checkpoint compatibility are exercised.
The first compile exposed a missing underlying host ProposalAdmission bound; added
that required bound to the delegating implementation, without weakening admission.

Actual native histories use four distinct three-replica groups: parent/child
metadata1/2 and routed data owners20/21. Original parent/child reservations, child
fence/tombstone and parent fence/tombstone each leave the original result unread,
obtain actual quorum facts, abort/join, reopen original factories and retry exact
commands. The publication facts and child projections come from actual original
Directory reads; full F facts come from actual original read-view quorum results
and configured membership identities. No invented native index or normalized
status determines progress. Retained parent service continues after child F;
missing-child completion cannot encode/publish before original child status.

Both metadata authorities stop/join before owners independently recover and keep
refusing old-context reads/writes with no client-credit leak. Original metadata
files and recovered GroupLogs remain unchanged. Original stored7/11, retry content
and outbox survive: isolated cloned-provider diagnostics replay the original data
operation without modifying or serving through the fenced native owner. Late
metadata recovery preserves all original manifests/intents/tombstones/fences and
cannot thaw service. All owners abort/join before temporary store cleanup.

Actual Linux validation: initial selected TCP/WAL1/1 passes15.01s; final TCP/QUIC
x WAL/checkpoint4/4 passes65.11s after3.51s build. Loopback execution uses required
permissions. Final all-feature deletion/read-view12/12 passes0.10s; routed app/host
checks14/14 pass0.22s (native network suite excluded). Core-only deletion11/11 and
routed13/13 pass0.01s/0.21s. Final all-feature/all-target Clippy -D warnings passes
0.11s. Formatting/whitespace and81-contract inventory pass. All process handles
terminal; no unchanged broad network/performance rerun or CI gate used.

Selected two-authority native and broader application/journal evidence, not a
universal recursive lifecycle/platform/fault proof. Current154 reparenting, next155
activated partial sources, following156 metadata authority movement; wider owner
families, retention/cancellation/general mappings and original membership/platform/
fault/P7 gates remain open. Full P0–P7 goal stays active. macOS execution pending,
Windows/P8 deferred, RPL-1.5 and ignored original pack preserved.

## Slice154a — checked retirement of already-deleted child slots

Directory schema10 (VBDINI10/VBDIR010) adds bounded VBRSLOT1 retirement of one
exact published-deleted child selector. Parent generation advances once; identity,
epoch and all other selectors remain exact. Same-authority facts must match the
actual local tombstone. Foreign quorum provenance remains a trusted-host duty.
Original operation/index/status, requests and retired identities survive replay.
Pending parent deletion/transfer/delegation or unconsumed creation refuses the
transition. The latter guard preserves creation's original generation dependency.

Delegated vacancies preserve full coverage, return RoutingError::Vacant before
child lookup, and add no fictitious obligations to later parent deletion. Retained
owner service remains usable at the original epoch; stale child hints remain
fenced. The native cache admits trusted child retirement without admitting
concrete-owner removal, restored vacancies or older generations. VBMAN002 is
required exactly when vacancies exist; old non-vacant bytes are unchanged, old
profiles and downgraded/noncanonical tags refuse.

Actual Linux checks:

- All-feature child_slots10/10 (0.09s), deletion12/12 (0.10s), routing13/13 (0.00s),
  directory31/31 (0.07s), delegation14/14 (0.05s), insertion11/11 (2.09s), and
  retained_insertion10/10 (0.99s), after a shared1.40s build. New tests include
  local/foreign actual child deletion, unchanged retained data, all-vacant parent
  deletion, successive retirement, exact status/retry replay, profile/codec/cache
  refusal, capacity/pending work and failed-batch atomicity. Native ModelIo cuts
  every byte of the retirement journal frame and fails sync/publication, then
  reopens and checkpoints: only the complete old or complete new route survives.
- Core-only child_slots8/8, deletion11/11, routing9/9 and routed13/13 pass after
  2.98s build (0.00s/0.01s/0.00s/0.22s).
- All-feature routed application/host14/14 passes0.22s after8.62s build;
  101 native network tests deliberately filtered as unchanged by this operation.
- All-feature/all-target Clippy with -D warnings passes5.22s. Formatting,
  git whitespace and81-entry component inventory checks pass.

Initial test compile referenced nonexistent Value/HistoryFull variants; adjusted
to the existing Served and DedupCapacity results. A profile test initially built
a generation2 bootstrap grant, rejected by the existing generation1 contract;
corrected that fixture. Production invariants were not relaxed to pass tests.

Finite application/storage evidence only: this step makes deleted-slot cleanup
usable and explicit vacancies representable. It is not live-child reparenting,
a new distributed proof, native network feature evidence or macOS validation.
Current154b live reparenting, next155 activated partial sources and following156
metadata authority movement remain; full goal stays active. RPL-1.5, Linux/macOS
targets, deferred Windows/P8 and background-only CI policy remain unchanged.

## Slice154b — atomic local metadata reparenting

Schema11's bounded ReparentPlan changes old parent, destination parent and child
in one ordered metadata command. Exact active same-authority views, matching
application/scheme and an exact destination vacancy are required. Local ancestry
and subtree traversal checks reject cycles, excessive resulting depth, missing
or foreign segments and pending lifecycle/creation work. Scope, ownership epochs,
physical data owners, explicit placement and all descendant manifests are retained.
The original local status/read view and immutable operation history recover exact
publication/index across retry and checkpoint. It creates no second metadata owner.

The native cache requires explicit empty-cache with_local_reparenting selection
for vacancy filling and parent rebinding. Old defaults retain their refusal
contract; partial refresh returns WrongParent until the child is refreshed. New
hints grant no serving authority. Actual child data7, retained parent data11 and
nested descendant data13 survive the move in application histories; the original
semantic retry produces no duplicate outbox effect. A move back uses current
published manifests. Concrete data-owner bindings remain original and require a
follow-on adoption step for later ownership transfers.

Linux validation:

- All-feature reparenting9/9 (final0.20s after0.67s build), covering live/nested
  continuity, exact repeated move/retry, current cycle/depth/foreign/path refusal,
  lifecycle and pending-creation conflicts, capacity/failed-batch atomicity,
  old profile/checkpoint truncation, original status/bounds and explicit native
  cache selection. Every native reparent journal-frame cut and failed sync/
  publication reopens to all-old or all-new bindings and preserves original index5.
- The focused all-feature regression run passes110 tests: child_slots10/10,
  deletion12/12, routing13/13, directory31/31, delegation14/14, insertion11/11,
  retained_insertion10/10 and reparenting9/9. Shared build4.85s; individual suite
  times0.14/0.17/0.00/0.12/0.06/3.09/1.38/0.30s respectively. Two additional
  delegation-reservation/default-cache cases in the existing tests pass in the
  final reparenting run.
- Core-only reparenting7/7, child_slots8/8 and routed13/13 pass after4.56s build,
  with0.07/0.00/0.35s suite times. All-feature routed application/host14/14 passes;
  101 native network tests remain filtered as unchanged by this local transition.
- All-feature/all-target Clippy with -D warnings passes5.16s. Formatting, git
  whitespace and81-contract inventory validation pass.

The first destination test constructed an invalid Delegated all-Group map;
corrected the fixture to isolate the occupied slot. The first regression run
exposed globally widened native-cache admission. The fix was explicit pristine
selection, retaining the old default rather than weakening its existing test.

This evidence validates the local metadata branch only. Original owner grant
adoption, guarded cross-authority reparenting and their native service histories
remain required. The full P0–P7 objective is active; macOS, broader faults and
remaining roadmap scope are not inferred from these Linux finite checks.

## Slice154c — original full-owner parent adoption

Opt-in RoutedApplication schema3 binds the original startup grant and a lifetime
limit of up to64 parent changes. VBRPAD01 carries the original local ReparentStatus
and metadata configuration. Exact before/after derivation preserves concrete
owner, epoch, application data and retry/outbox history. Fixed ParentGrantStatus
and quorum-read projections retain the original operation/index after subsequent
changes or full fencing. Commands are host-authenticated observations, not
commitment certificates. Separate control capacity survives ordinary exhaustion.

TransferSource composes the selected inner schema. Recovery restores the inner
grant before checking its frozen intent; validating against the startup grant
first would reject a valid later split. An original adoption retry after F advances
only the outer applied prefix and cannot change the frozen images.

Actual checks on Linux:

- 72 focused all-feature tests pass: reparenting14, routed14, scoped_source5,
  transfer_source7, transfer_target9, transfer_activation9 and delegation14.
  The routed run excludes101 unchanged native socket histories. Shared build8.08s;
  suite times0.07/0.66/0.24/0.27/0.17/0.51/0.28s in execution order.
- Core-only reparenting11, routed13 and transfer_source7 pass after3.53s build;
  suite times0.23/0.33/0.50s.
- Actual application history uses committed metadata reparenting, owner adoption,
  fresh-original-factory checkpoint restore, reserved split, F, two imports,
  metadata/parent publication and both target activations. Both original retries
  remain duplicates with original outbox entries. Source adoption retries after F
  preserve original status and export digests across another checkpoint restore.
- Native ModelIo cuts every byte of adoption and subsequent freeze journal frames
  and fails sync/publication. Recovery sees the old or complete state, original
  data/outbox and original adoption index; exports stay at F. These are storage
  model/application histories, not new socket or arbitrary-power-loss evidence.
- Strict command truncation, full ordinary-capacity control admission, pending
  order, conflicts, stale grants, atomic batch failures, checkpoint truncations,
  corrupted record indexes/operations/counts, profile and immutable-binding
  mismatch, and repeated restore into already-adopted state pass.
- All-feature/all-target Clippy -D warnings passes. It first found a fixed test
  Vec that should be an array; that fixture was corrected. Formatting, whitespace
  and81-contract inventory checks pass.

This completes selected original full-owner adoption. Scoped retained and
activated/imported owner families, guarded cross-authority movement and native
network reparenting remain. The full P0–P7 objective, macOS and broader scope/gates
stay active; this evidence does not close full reparenting154.

## Slice154d1 — cross-authority reparent guards and cancellation

Directory schema12 explicitly binds preparation/cancellation. Canonical
CrossReparentPlan closes the full new ancestry and moved subtree under128 manifest
records, with exact local guards at each participating authority. The first sorted
authority records the initial guard; later prepares bind its original authenticated
quorum observation. No routes or data ownership change during preparation.
Only that coordinator records the original cancellation decision. Participant
release verifies its digest/identity and local facts; an early cancellation
creates a tombstone that rejects delayed acquisition. Guards never time out.

Actual Linux checks:

- All-feature reparent_guards8/8 pass. Histories cover three-authority preparation,
  original status/retry/checkpoint recovery, reserved cancellation under ordinary
  exhaustion, cancellation-before-prepare, reciprocal-move exclusion, pending
  ordering, exact closure/depth, codec/profile/atomicity and quorum read budgets.
- Original data writes/retries/outbox and disjoint metadata changes proceed while
  affected publication, deletion, creation and local reparenting refuse.
- The native ModelIo history cuts every byte of coordinator guard/cancellation
  and participant guard/release journal frames, plus sync and publication failures.
  Fresh replay/checkpoint reopen preserves old-or-complete locks/tombstones,
  unchanged manifests and original retry indexes. This is finite journal-model
  evidence, not networked complete-move or arbitrary-power-loss evidence.
- 100 distinct focused all-feature tests pass across directory31, delegation14,
  deletion12, insertion11, retained_insertion10, reparenting14 and new guards8.
  Directory tests were rerun after the creation fix. Core-only directory27,
  reparenting11 and guards7 pass (45 tests).
- All-feature/all-target Clippy -D warnings passes; formatting, whitespace and
  the81-contract inventory checks pass.

A new negative test found that the post-command guard check could miss a fresh
creation: its lookup history had not yet been inserted. Creation admission now
checks the protected parent directly, and both prepare/creation orderings are
covered. A test's deliberately changed cancellation initially violated its own
index ordering; it now keeps a valid shape while testing refusal against actual
local history. Clippy removed an unnecessary clone of a Copy query.

This closes preparation/cancellation only. Cross-authority commit/publication,
owner-family adoption and native complete-move resumption remain154d. Full P0–P7,
macOS and the other declared roadmap gates remain active.

## Slice154d2 — committed cross-authority metadata movement

Explicit Directory schema13 adds commit, local participant publication, all-party
completion and successful release over the original prepared plan. Coordinator
commit is exclusive with cancellation; original local evidence is checked rather
than accepted as a foreign assertion. Guards stay held until all original
participant publications are bound to the same decision. Each of the three
changed manifests advances one generation; epochs, physical owners, descendants
and application state remain unchanged.

Actual Linux validation:

- 106 focused all-feature tests pass: guards14, reparenting14, directory31,
  delegation14, deletion12, insertion11 and retained_insertion10. Shared build2.80s;
  suites finish in0.06/0.16/0.09/2.34/1.16/0.59/1.05s in execution order.
- Core-only guards12/reparenting11/directory27 pass (50 tests), after4.53s build.
- Three-authority movement checks both partial publication orders, bounded
  old/new routing refusals, unchanged physical owner, original data retries/new
  writes and unchanged metadata while writing. A subsequent reverse move can
  prepare and commit after all original guards release.
- The successful path exhausts ordinary history before commit, publication,
  completion and release. All phases use their reserved control capacity.
- Missing/changed original guards, coordinator-fact mismatch, early release,
  missing/changed publication evidence, commit/cancel exclusion, strict codecs,
  old profiles, atomic failed batches and every snapshot truncation are checked.
- Native ModelIo cuts every byte of coordinator decision/completion and
  participant publication/release frames, plus sync/publication failures. Fresh
  replay and checkpoint restore retain old or complete local phases, original
  observations/indexes and exact retries. This does not establish arbitrary crash
  or native network complete-move coverage.
- All-feature/all-target Clippy -D warnings passes. Constant envelope assertions
  were moved into compile-time checks as requested by Clippy. Formatting,
  whitespace and81-contract inventory checks pass.

This completes the metadata commit protocol. Native cache refresh and original
owner adoption across authorities remain next154d3, together with a complete
native TCP/QUIC resumption history. Wider owner families, macOS and full P0–P7
scope remain open; no complete reparenting-service claim is made here.


### Slice154d3 — full-owner cross-authority adoption and native cache

Linux validation: 121 focused all-feature tests pass across directory28,
guards18, reparenting14, delegation14, deletion12, insertion11,
retained_insertion10 and routed14. The shared --skip native:: filter excludes
three directory and 101 routed native-network cases; these are not newly claimed.
65 core-only tests pass (directory27, guards14, reparenting11, routed13).
All-target/all-feature Clippy -D warnings, formatting, whitespace and the
81-contract inventory pass.

Four new cases cover original completed three-authority metadata observations,
schema4 owner adoption, full ordinary-history control reserve, unchanged data
retry/outbox and original publication status, strict malformed/partial/profile
and atomic-checkpoint rejection, both partial native-cache refresh orders,
subsequent reserved split/fence/import/publication/activation, and every byte of
native owner adoption and later freeze frames plus sync/publication failures.
Exact original retries and exported-image digests survive restart. Journal
recovery sees old or complete states. Prior schema1–3 tests continue to pass.
No new native TCP/QUIC complete-move history or macOS execution is claimed;
those and other owner-family integrations remain outstanding.


### Slice154d4 — native complete cross-authority move/restart histories

All four TCP/TLS and QUIC WAL-only/checkpoint cases pass in46.59s after1.45s
rebuild (115 unrelated routed tests filtered). Three independent three-replica
metadata groups and one three-replica original data owner execute real proposals
and original quorum reads. Each metadata phase loses the first result, joins
workers, reopens actual files, compares original statuses/indexes and retries the
same successful operation. Cache refresh comes from the original metadata read;
partial publication refuses inconsistent routing.

The data owner adopts the completed move, loses/reconstructs its original result,
and later restarts/writes/reads/retries with every metadata authority offline.
All replicas retain the new parent, original retry/outbox entries and later writes.
Stopped metadata files and recovered GroupLogs remain unchanged. Late metadata
recovery retains all original guard/decision/publication/completion observations.
All-target/all-feature Clippy -D warnings, formatting/whitespace and the81-contract
inventory pass. Production code is unchanged in this slice; earlier application
and byte-cut checks were not rerun. These finite joined-abort histories do not
claim hardware power-loss, arbitrary faults, macOS, or other owner-family support.


### Slice154e1 — retained/scoped parent adoption

Actual Linux validation: 65 focused all-feature checks pass across retained
insertion14, scoped source5, routed14 (105 native tests excluded by the shared
filter), reparenting14 and reparent guards18. The final retained/scoped/routed
run passes33 tests after7.68s build; suites finish in3.36/0.37/0.28s. Core-only
retained10/scoped4 pass after0.34s rebuild. All four existing foreign retained-source
TCP/TLS and QUIC WAL/checkpoint regressions pass in35.19s after5.01s build
(115 other routed tests filtered). All-target/all-feature Clippy -D warnings,
formatting, whitespace and81-contract inventory pass.

Four new checks cover completed retained handoff -> guarded cross-authority
parent move -> schema4 source adoption/reopen -> another parent-reserved retained
handoff with target activation and parent completion; unchanged original exports,
retry/outbox and full-fence behavior; local metadata parent movement with full
ordinary history and independent capacity; strict profile/limit/collision/pending
and mixed-ledger checkpoint rejection; every native parent-adoption/full-fence
frame byte, sync and publication fault with exact old or complete recovery.

Two failing test fixtures were corrected: parent metadata must be published before
its dependent child, and a parent retry must use its original bytes rather than
a later retained-adoption command. Those refusals were correct; no production
safety contract was weakened. Existing schemas1–3 retain their old encoding,
while schema4 selects a tagged bounded history and advertises its larger reserve.
This is selected application/checkpoint/native-journal evidence. Imported owners
and native parent-move service for these families remain open; the four network
checks above are legacy-profile regressions, not proof of new-profile movement.

### Slice154e2 — imported owner parent adoption

Actual Linux validation:85 distinct focused all-feature tests pass: imported
parent4, transfer target9, activation9, repeated transfer5, retirement7, retained
insertion14, scoped source5, reparenting14 and reparent guards18. Final imported
suite4 finishes in1.67s. Core-only imported3 and retirement5 pass. Four existing
TCP/TLS and QUIC retirement WAL/checkpoint regressions pass in29.33s after8.85s
build (115 other routed tests filtered). All-target/all-feature Clippy -D warnings,
formatting/whitespace and81-contract inventory checks pass.

New checks cover real source data import and activation, supplied canonical
local/cross metadata observations, independent parent history despite exhausted
ordinary operation capacity, original retry/outbox preservation, repeated moves,
later two-source merge/activation, retirement/reopen and exact original facts.
Every checkpoint truncation, bad record bytes and semantic changes with recomputed
digests refuse atomically. Final-grant lineage must match the exact retirement
source, including round trips to the original parent. Native ModelIo tests cut
every parent-adoption and later freeze frame byte and inject sync/publication
faults; recovery yields only complete original or new state and retains exports.

This slice does not add a new TCP/QUIC parent-move integration history: the four
network runs above are old-profile regressions. Initial fixtures tried unreserved
nested intents/later-generation directory bootstraps; corrected fixtures use the
public host-observation seam without weakening those production checks. Actual
metadata protocol/recovery tests remain separately covered by reparent_guards.
New-profile native family service composition, macOS and the remaining roadmap
are still open.

### Slice154e3 — native retained/imported parent moves

Eight distinct new native cases pass across focused runs: retained and imported
owners over TCP/TLS and QUIC, each with WAL and checkpoint reopen. Original
source transfer, assigned target import and activation run through the real
three-replica service. Original metadata quorum observations supply the closed
plan, guards, commit, publications and completion. Unread results are followed
by joined worker abort, exact-file reopen and original-result retry. Partial
cache refresh refuses routing. With all metadata stopped, moved-owner writes,
reads, imported/retained retries and another owner reopen succeed. Frozen
exports, activation, creation bindings and original metadata observations remain
exact. Stopped metadata file bytes and recovered GroupLogs do not change.

Initial TCP/WAL cases pass in44.73s and40.93s. The remaining six-case run produced
four passes and two startup failures: AddrInUse after the free-port probe, then
AlreadyExists from the failed fixture's shared directory. Fixes are test-only:
probe listener candidates below this host's32768–60999 automatic ephemeral
range, and give owner families separate directories. The two affected
QUIC/checkpoint cases pass on rerun in91.61s. All eight existing root/foreign
retained-transfer TCP/QUIC WAL/checkpoint regressions pass in177.43s (119 other
routed tests filtered). All-target/all-feature Clippy -D warnings, formatting,
whitespace and81-contract inventory checks pass. No production protocol or
provider changed; earlier deterministic/journal tests were not redundantly run.

The imported-child case also verifies old-parent retained writes with refreshed
routing, while explicitly recording that the source's stored grant predates the
child-slot removal. Parent-side grant refresh before its next transfer is154f,
not a completed capability. These are Linux loopback joined-abort histories;
arbitrary faults, macOS and the full roadmap remain separate requirements.

## Slice154f — parent child-slot grant continuity

Explicit scoped-source schema5 adds checked local and completed-cross parent
slot adoption, retaining the bounded original grant ledger and historical export
bindings. The cross command carries the selected parent's original publication
and configuration, not just the child's observation. Both parent roles complete
a later retained transfer through target import/publication/activation. Original
exports, data retries and retained-publication statuses survive restart. Pending
order, bounded capacity, profile mismatch, conflicting IDs, malformed provenance
and truncated/corrupt checkpoints refuse without partial adoption. Every byte of
the new native adoption frame plus sync/publication faults recovers either the
old or complete grant; later full fencing preserves exact adoption retries.

Five new conformance cases pass with the existing suite:19 retained/scoped tests,
48 related all-feature lifecycle tests and51 core-only tests. All-target/all-feature
Clippy with warnings denied, formatting/whitespace and81-contract inventory pass.
The shared codecs were subsequently relocated beside routed control formats to
keep module dependencies one-way; their public exports and encodings are unchanged,
and final Clippy/format checks pass after the relocation.

All eight selected native parent-move cases pass. TCP imported/WAL passes in38.51s;
the other seven TCP/QUIC WAL/checkpoint cases pass in300.26s. Imported-child cases
now recover an unread old-parent grant refresh, repeat its original result, keep
remaining writes working with all metadata stopped, and independently reopen the
parent source. Original export/creation/activation facts and stopped metadata
bytes/GroupLogs remain exact. Retained-owner cases validate the same selected
source5 profile. These are Linux loopback joined-owner-abort histories, not
hardware power-loss, arbitrary-fault, macOS or full-roadmap proof.

During development, the new profile exposed schema4 hard-coding in two shared
test reopen factories; the factories now follow the selected profile. Production
continued to reject the mismatched checkpoint. An initial native filter selected
zero tests and is not counted among these results. No performance claim is made.

## Slice155a — imported partial-source embedding

Five new cases exercise an activated imported owner delegating a subrange while
retaining service. Actual Directory creation/reservation, checked intent, source
fence/image, target import, publication/parent completion and activation are run
twice. Original import/activation and transferred data retry/outbox remain intact;
older immutable exports survive subsequent operations and checkpoint/replay.
Mixed local parent/slot history uses supplied original metadata observations.
Bounded lifetime/export capacity, full application-history control reserve,
pre-activation refusal, pending ordering, ID conflicts, atomic bad batches and
truncated/corrupt checkpoint rejection are exercised.

NativeLogStore tests cut every partial-freeze/adoption frame byte and inject
sync/publication faults. Recovery has the original or complete state; remaining
data still serves and retried fences/adoptions reproduce original images/status.
These are native journal fault models, not TCP/QUIC or hardware power-loss tests.

All58 selected all-feature cases pass: retained insertion24, imported parent4,
retirement7, activation9, repeated transfer5, target9. Core-only regressions and
partial-profile checks also pass. All-target/all-feature Clippy, formatting and
inventory checks accompany the checkpoint. The initial test routing hint was
stale after publication and was corrected to the actual route; capacity refusal
now uses ReceiptBudget. One incorrectly named test-target invocation executed
nothing and is not counted. No performance result is claimed.

New-profile full-transfer retirement/reclamation and native network composition
remain155b. The existing retirement-lineage validator has not been widened for
partial delegation. The full goal and outstanding macOS/general-fault work remain
active; this is selected embedding-path evidence only.


## Slice155b1 — remaining data transfer after imported partial delegation

Three added cases cover actual two-child partial delegation followed by complete
remaining-range relocation or split, unchanged earlier children, original retry
and new successor writes, source/target/Directory checkpoint recovery, malformed
maps, old profiles, truncated codecs/checkpoints and every final-freeze native
journal frame byte plus sync/publication failures. Native tests use ModelIo, not
live sockets or actual hardware power cuts. Root construction and parent-reserved
same-authority execution are covered; foreign-authority execution is not claimed.

All123 cases pass in the eleven relevant all-feature targets: delegation14,
Directory31, imported parent4, retained insertion27, retirement7, activation9,
merge3, publication7, repeated transfer5, source7 and target9. The same eleven
core-only targets pass. No unrun tests or unfinished processes count as evidence.
Initial fixture compilation mistakes and a negative case's incorrect metadata
group were fixed; the latter had correctly accepted an ordinary fresh group.
Retirement support for this partial lineage and native network composition remain
pending. No P5/P6 completion or performance claim follows from these results.

Final all-target/all-feature Clippy with warnings denied passes. Formatting,
whitespace and the81-contract inventory check also pass. The inventory check
validates metadata shape and paths; executed conformance is the test evidence
above. All test and check sessions are terminal.


## Slice155b2 — retirement of partially delegated imported owners

Three new tests compose actual imported data, two partial delegations and a
remaining relocation/split through successor activation, explicit retention
release and retirement. They check exact original activation/compact lineage,
retired retries/non-service, missing successor evidence, incompatible profiles,
truncated checkpoints/lineage and rehashed index/operation discontinuities. The
existing supplied parent/slot history additionally validates its compact lineage.

The native-file case uses the generic retirement/storage conformance helper to
interrupt retired snapshot publication before/after the manifest or abandon a
sealed image. It recovers from the original pinned live snapshot plus committed
retirement tail, then publishes/pins a verified retired base, reclaims old WAL
and reopens without retained application commands. No hardware power-loss or
new TCP/QUIC service execution is claimed.

All64 cases pass in the six relevant all-feature targets: retained insertion30,
retirement7, imported parent4, target9, repeated transfer5 and activation9. The
same core-only targets pass. An initial test query named a nonexistent variant
and was corrected to Status; no production contract was relaxed. The full goal
stays active, with native service composition155b3 and metadata movement156 next.


Final validation: all53 matching core-only cases pass. All-target/all-feature
Clippy with warnings denied, formatting, whitespace and the81-contract inventory
check pass. All test/check sessions are terminal; no remote CI result is required.

## Slice155b3 — native partial-import lifecycle

`cargo +stable test --all-features --locked --offline --test routed partial_import`
passes all four new cases: TCP/TLS and QUIC, each with WAL and checkpoint recovery.
The final run includes nonempty data in both delegated ranges and the remaining
range. Each lifecycle command is committed by an actual three-replica group,
loses its original client observation, reopens its selected native provider and
retries after checkpoint/state equality checks. Original import, two subsequent
scoped handoffs, parent completion, remaining relocation and final activation
all use native committed observations. Staged targets do not serve; frozen ranges
refuse, while the retained range remains usable before the full fence.

Retirement first recovers from an older live checkpoint/WAL tail, preserves the
exact compact lineage/freeze/result and releases live owner service. The two
checkpoint cases then publish a retired snapshot, physically reclaim through
exact Node request completions, and reopen with no application commands in the
retained WAL. Original Staging creation bindings survive. Both children retain
nonempty imported data and original operation results; the successor also accepts
a new write and recovers it. These operations run with metadata and old owners
stopped, and their files remain unchanged. All owners are joined before cleanup.

`cargo +stable test --no-default-features --locked --offline --test retained_insertion --test retirement`
passes27 cases (22 retained insertion,5 retirement). All-target/all-feature Clippy
with warnings denied, `cargo +stable fmt --all -- --check`, `git diff --check` and
the81-contract inventory check pass. No new production schema/provider is added.

These are selected Linux loopback/native-file histories with one destination for
the remaining range. They supplement the embedding split-destination and byte-
cut fault tests; they do not establish arbitrary faults, hardware power loss,
macOS, separate hosts or metadata-authority movement. The full goal remains active.

## Slice156a — metadata authority source half

All seven `metadata_transfer` tests pass with all features; the six non-native
cases pass core-only. The new source profile binds its bootstrap and export
budget, validates complete settled manifest views and rejects live lifecycle
reservations, stale/pending changes and reused operation IDs. It freezes at F,
retains the original directory checkpoint and provenance, refuses subsequent
service and returns the exact original fence on retry. Exported directory replay
retains original publication outcomes. Truncated/corrupt checkpoints, mismatched
profiles, missing bootstrap and failed batches refuse atomically.

The native ModelIo case injects every append-byte cut plus synchronization and
publication failures into the actual native log codec/store fence record. Both
old-unfrozen and complete-frozen recovery occur; exact retry yields the same
image. This is modeled journal evidence, not hardware power loss or a networked
metadata move. No destination authority is activated by156a.

Related checks:31 all-feature directory tests,30 all-feature retained-insertion
tests and27 core-only directory tests pass. All-target/all-feature Clippy with
warnings denied passes. Formatting, whitespace and the updated contract inventory
are checked separately. Initial fixture failures from the lifecycle wrapper's
absent optional deployment envelope were corrected by using its underlying
Directory's existing declared bound; no old profile behavior was relaxed.

Final rejection-history checks preserve failed control IDs through snapshot/export,
prevent ID reuse and retain successful freeze capacity after the failure ledger
is full. The ledger digest is included in the source status. Final all-feature
and core-only tests plus Clippy pass; all sessions are terminal.

## Slice156b1 — non-serving metadata target import

`cargo +stable test --all-features --locked --offline --test metadata_transfer`
passes12 cases; the matching `--no-default-features` run passes10. Five new target
cases cover staged/imported status and exact retry, source F12 versus destination
I3, original source publication outcomes, constructor/configuration identity,
pending order, truncations/corruption and atomic checkpoint restore. Rehashed
rejection records and a forged fence overlapping a directory command refuse.
The source restore applies the same command-index separation check.

The new native case injects every append-byte cut plus synchronization and
publication failures through NativeLogCodec/NativeLogStore with ModelIo. Recovery
reaches staged-only and complete-import states, then exact retry/checkpoint keeps
the original history. This is modeled native journal evidence, not socket service
or hardware power loss. No destination activation is claimed.

All-target/all-feature Clippy with `-D warnings` passes. Publication, a writable
destination base, owner/locator refresh and end-to-end native composition remain.
Formatting, whitespace and the83-contract inventory/path check also pass.

## Slice156b2 — published metadata activation

The all-feature `metadata_transfer` target passes17 tests; core-only passes14.
Five added tests cover source publication and target activation with distinct
original/local indices, pre-activation refusal, source fencing, original history
and failure IDs, exact retries and a later metadata write. An inherited creation
remains reserved across the move; new namespace creation performs actual target
initialization/readiness and metadata publication, followed by checkpoint/retry.

Wrong configurations/profiles, changed publications, truncated commands and
checkpoints, inherited-ID replay collisions and partial application batches
refuse. The namespace fixture initially used a mismatched initialization ID;
correcting it to the original creation ID respected the existing contract.

The new native ModelIo test cuts every append byte plus sync/publication
boundaries separately for source publication, destination activation and a later
destination metadata write. Old and complete outcomes both occur; exact retry
resumes, source images stay immutable and activation survives checkpoint recovery.
This is actual native codec/store execution with modelled I/O, not a native socket
deployment or hardware power-loss experiment.

All-target/all-feature Clippy with warnings denied passes. Full live-owner and
locator adoption, repeated metadata moves, TCP/QUIC composition, macOS and wider
fault coverage remain; these tests do not complete P5/P6 or the overall goal.
Formatting, whitespace and the85-contract inventory/path check pass.

## Slice156c1 — full-owner metadata adoption and cache refresh

All68 cases pass across all-feature metadata_transfer23, reparenting14,
reparent_guards18 and routing13. The same core-only targets pass52 cases
(18/11/14/9). All-target/all-feature Clippy with warnings denied passes.

Six new adoption cases exercise actual source/import/publication/activation
observations, original full-owner adoption with exhausted data capacity, stable
data groups/epochs, preserved retries/outbox and exact bootstrap, mixed local
parent -> metadata -> local parent changes, and checkpoint replay. The full
source performs a later split through the migrated directory, imports both
targets and activates them with original data retries. Metadata-adoption reads
retain both the local owner result and original activation provenance.

Wrong domains/plan hashes/index relationships, old profiles, insufficient
capacity, truncated codecs/checkpoints and partial batches refuse. Opaque remote
digests/configurations require host authentication; a byte mutation of an opaque
profile is not locally detectable without that evidence. Initial fixture issues
were corrected to use the existing transfer APIs and distinguish those claims.

Native cache cases check exact ownership-preserving metadata changes, partial
parent/child refresh refusal and unchanged state on bad admission. Native ModelIo
cuts every adoption-frame byte plus synchronization/publication boundaries;
both old and complete grants recover, preserving data/outbox and exact retry.
These are selected modelled journal and embedding cases, not TCP/QUIC deployment
or power-loss proof. Retained/imported families, foreign locators and repeated
metadata moves remain, together with the full goal's other open requirements.
Formatting, whitespace and the86-contract inventory/path check pass.

## Slice156c2a — retained-owner metadata adoption

All-feature metadata_transfer23, reparent_guards18, reparenting14 and
retained_insertion33 pass (88 cases). Three new retained metadata cases cover
actual source fence/import/publication/activation, earlier immutable exports,
retained -> metadata -> retained grant replay, later transfer through the new
authority and original source/target data retries. Profile capacity rejection,
old-profile refusal, pending control/data admission, stale/conflicting adoption,
partial batches, checkpoint truncations and post-fence retry behavior are checked.
Native ModelIo cuts every adoption-frame byte and synchronization/publication
boundaries, recovering only old or complete grants and preserving exports/outbox.

Initial test calls used a private status helper and a nonexistent query variant;
they were corrected to existing public quorum-read APIs. No production API was
weakened for the fixtures. This is embedding and modelled native-journal evidence,
not physical power loss, native TCP/QUIC relocation or macOS validation.

The matching core-only targets pass67 cases (18/14/11/24). The three new
all-feature cases were rerun after adding an explicit imported-value and duplicate
retry check on the second target. All-target/all-feature Clippy with warnings
denied, formatting, whitespace and the86-contract inventory/path check pass.

## Slice156c2b — imported metadata adoption and retirement

All-feature local tests pass across metadata_transfer23, reparent_guards18,
reparenting14, retained_insertion38, retirement7, transfer_target9 and
imported_parent4 (113 cases). Core-only runs of the first six targets pass85
cases (18/14/11/28/5/9). All-target/all-feature Clippy with warnings denied passes.

Five new imported-owner tests cover both full and partial profiles: actual initial
import/activation, complete metadata relocation/adoption, later split or
retained-then-remaining handoff, retirement and exact restart/retry. They preserve
original imports, activation, data retries/outbox and partial exports. The retired
lineage reconstructs the exact final grant without application payloads. Missing,
reordered, rehashed-invalid and truncated histories, stale/wrong profiles, partial
batches and checkpoint truncations refuse. Old imported-parent retirement tests
also pass against unchanged profile encodings.

Native ModelIo cuts every adoption and retirement frame byte plus sync/publication
boundaries for both profiles. Recovery reaches only the old or complete state,
then resumes exact original control outcomes. These are modeled native-journal
and embedding cases, not TCP/QUIC deployment or physical power-loss validation.

Test corrections used existing public status/query variants, a supported split
shape, separate metadata/data configurations, and preserved original semantic
receipts while retry log indices advance. Partial pre-activation refusal retains
its existing NotApplied contract. Production checks were not relaxed to make
these fixtures pass. Foreign locators, repeat metadata movement and native socket
composition remain active work under the unchanged full P0–P7 goal.

Core-only imported_parent adds3 passing cases (88 total across all seven targets).
Both all-feature and core-only all-target Clippy pass with warnings denied. Final
metadata tests pass8 cases after the fixture cleanup; formatting, whitespace and
the86-contract inventory/path check pass. README remains unchanged.

## Slice156c2c — foreign metadata locator refresh, 10 October 2026

Seven new downstream tests cover actual metadata move phases, foreign parent and
child locators, a bridge manifest with both roles, bounded reserved admission,
original outcomes, profile/provenance/truncation/pending/batch refusals and cache
refresh order. The bridge test requires all references to one moved authority to
change together. Oversized complete plans refuse without projection. Native
ModelIo cuts every update-frame byte and sync/publication boundary for both
foreign authorities; each recovery has only the old or complete manifest and
resumes the original operation/status. Open creation keeps its completion reserve.

Executed locally:
- All-feature tests: directory31, group_creation4, metadata_transfer30,
  reparent_guards18, reparenting14, retained_insertion38, routing13:148 pass.
- Core-only same targets:27/3/23/14/11/28/9:115 pass.
- All-feature and core-only all-target Clippy, warnings denied: pass.
- cargo +stable fmt --all -- --check; git diff --check; node
  validation/check-inventory.mjs: pass (87 contract records/paths).

Commands used --locked --offline and the stable toolchain. No native socket,
physical power-loss or macOS result is claimed. Data-owner locator adoption,
repeated metadata exports, native move composition and remaining P0–P7 requirements
remain open. The full goal is active.

## Slice156c2d — owner adoption of foreign locators, 10 October 2026

Five new downstream cases cover routed6, retained-source7 and imported-full10 /
imported-partial11 profiles. Actual deterministic metadata move phases and a
foreign directory update precede owner adoption. Later split, retained handoff,
imported writes/merge and retirement preserve data, retries, original activation,
export bytes and metadata index domains. Empty-but-well-formed grant lineage
cannot substitute for the original locator adoption during retirement. New
status reads have fixed bounded results. Old profiles, bad observations, stale
and pending updates, checkpoint corruption and partial batches refuse.

Native ModelIo cuts every adoption-frame byte for all four owner families and
every retirement-frame byte for the full/partial imported owners, plus sync and
publication boundaries. Recovery reaches only the old or complete checkpoint
state, then an exact original retry. No socket or hardware-power-loss claim.

Executed with cargo +stable and --locked --offline:
- all-feature tests: imported_parent4, metadata_transfer35, reparent_guards18,
  reparenting14, retained_insertion38, retirement7, transfer_target9:125 pass;
- core-only same targets:3/27/14/11/28/5/9:97 pass;
- final changed owner-locator subset:5 all-feature and4 core-only pass;
- all-feature/core-only all-target Clippy with -D warnings: pass;
- formatting, git diff --check, inventory shape/conformance paths: pass,88 records.

Fixture repairs retained original schema/configuration/operation-ID contracts.
Merge continued to refuse conflicting data-operation histories. Repeated
metadata exports and native TCP/QUIC authority-move composition remain open,
alongside the other documented P0–P7 work. README is unchanged.

## Slice156c3 — repeated metadata moves, 10 October 2026

Five downstream tests exercise A->B->C and A->B->C->D with intermediate writes,
original receipt/creation identities, old rejected/control IDs, distinct log
indices, fixed profiles, bounded nesting and export declarations, stale/pending
freeze rejection, malformed import and checkpoint recovery. B's source fence at
index7 coexists with an A-domain creation command at7. ModelIo cuts every repeat
freeze/publication/import/activation frame byte, sync and publication boundary;
old-or-complete recovery resumes the original operation.

Executed locally with cargo +stable --locked --offline:
- Related all-feature suites:4/40/18/14/38/7/9,130 pass.
- Final core-only same suites:3/31/14/11/28/5/9,101 pass.
- Final changed repeated subset:5 all-feature tests pass.
- All-feature/core-only all-target Clippy with -D warnings: pass.
- Formatting, diff whitespace and inventory shape/conformance paths: pass,88.

An initial test incorrectly expected a committed conflicting retry not to advance
the log prefix. It now mutates the first staged import to test atomic rejection.
Earlier schemas and first-move tests remain exercised. These results do not cover
native socket composition, hardware power-loss or macOS; the next planned step
is the TCP/QUIC service path. The broader P0–P7 goal remains active.

Final admission checks include a wrong repeated-profile bootstrap operation ID;
outer source/publication wrappers reject nested target conflicts/non-serving
outcomes. Final metadata suite reruns cover all40/all31 cases after this change.

## Slice156c4 — native metadata moves, 10 October 2026

Four selected Linux native TCP/TLS/QUIC x WAL/checkpoint histories pass. Separate
three-voter groups execute A->B->C, foreign-parent locator refresh and original
full-owner adoption. Every selected control completion remains unread before
worker abort/join, durable reopen and exact retry, with identical quorum status.
Source export matches its quorum observation, frozen sources refuse reads and
unactivated imports refuse service. Partial cache refresh refuses resolution.
The owner serves after all metadata stops, recovers and retries four writes
without duplicate data/outbox effects. Stopped metadata files/recovered logs and
original receipt/history authority domains are checked.

A new retained-receipt capacity test covers four wrappers and repeated profiles,
including no commands and noop/command mixtures. The native router caught excess
Vec capacity; exact command-count allocation fixes the cause without relaxing
the published bound. Plain and frozen sources refuse the new borrowed target
accessor. Local metadata suites pass41 all-feature and32 core-only tests; strict
all-feature and core-only all-target Clippy pass with warnings denied. Two TCP-only
cases also pass with --no-default-features --features native,tls. A bare native
feature run selected zero socket tests and is not counted. Formatting, whitespace
and the88-contract inventory/path check pass. Commands use cargo +stable with
--locked --offline.

These are selected process/worker-abort histories, not physical power-loss or
arbitrary-fault proof. Retained/imported owner-family native composition and
other documented P0–P7 work remain. Linux/macOS GitHub CI has been activated and
was observed running; no remote success is inferred from activation.

## Maintainability enforcement and refactors — 10 October 2026

Earlier strict Clippy runs covered its default group. The separate function-size
and cognitive-complexity audit was warning-only; those passes did not establish
compliance with the requested limits. Both lints are now deny-level in Cargo.toml,
and the CI warning-only override has been removed. The lint job is expected to
fail while the remaining violations are repaired.

The first directory cleanup removes seven diagnostics with195 all-feature and
158 core-only tests passing. The next routed/log/snapshot refactor removes six
more production diagnostics. Its149 routed/lifecycle,14 routed contract,91
storage/snapshot and four native TCP/QUIC phase-recovery checks pass; combined
core-only coverage passes168 cases. Formatting, whitespace and inventory pass.
Strict library Clippy still fails with35 production findings; core all-target
Clippy fails with25 production and9 inline-test findings. Other integration
fixture findings remain from the audit. These results are not a clean lint or
full-roadmap completion claim.

## Current strict baseline and native recovery — slice158, 10 October 2026

The earlier maintainability entry above describes its historical failing audit,
not the current status. Both current complete all-target strict Clippy profiles
(all features and no default features) pass with warnings denied and zero
function-size/cognitive-complexity diagnostics. Existing limits remain enforced.

New actual native TCP/TLS and QUIC shared-assembly cases force all eight stale
follower groups to install snapshots after all transports are closed and their
peers compact beyond the old prefixes. Per-group durable bases, restored
application values/indices, old receipts, quorum reads and a second full restart
pass. Full native_benchmark suite18/18 passes in6.20s; independent maintenance
checker5/5 passes. The first fixture's premature loaded-count assertion failed;
it now waits for the separate application restoration boundary as well as the
WAL base. Both initial and final logs are retained.

One original60-second QUIC paused-follower workload passes the unchanged selected
group1 snapshot gate and full recovery/retry/join verification. The archived raw
checker passes:416 applied of480 offered,64 window refusals, zero unknown/pending,
26 checkpoints and78 reclamations. Earlier failed runs remain retained, and this
selected success does not explain their packets. The p99 remains outside the
fixed-p99 target. See performance/slice158/README.md for exact commands, source
hashes, raw outcomes and limits. This is Linux evidence only; full P0–P7 remains
active and P8/Windows remain deferred.

### Slices159–160 — baseline ledger and automatic metadata reads

Reviewed P0–P7 against current source, chapter11/12/17 requirements and the
component catalogue. BASELINE_ACCEPTANCE.md now records implemented bounded
lifecycle/admin paths and missing obligations separately. Full acceptance is
not established: full-suite runs and corrected macOS execution remain pending.

The new public stateless manifest mappings and owning native adapter preserve
original Node tickets, barriers, result credits and rejected typed completions.
ManifestReadSource's associated result is a version2 Rust API change. Four
TCP/QUIC × WAL/checkpoint metadata histories pass across two actual moves/reopen,
including inactive/fenced refusal, cancellation cleanup and wrong-ticket
recovery. Four original native lookup tests pass; all-feature source/mapping5
and core-only3 pass. No persistence format or quorum rule changes.

Before160, core-only591/68 targets and native-without-TLS882/71 targets pass.
The corrected library/service suite passes64+35; latest client-target fixture
correction passes2/2 separately. Exact-argument retry fixes legal leader changes;
positive-only bounded UDP receive fixes the observed macOS fixture assumption.
Failed attempts remain archived. Strict all-feature/core Clippy has zero
diagnostics, fmt/diff and warnings-denied API docs pass. Inventory metadata
validation passes89 entries; independent existing checker suites pass19.

See [retained evidence](baseline/slice159-160/README.md) for commands, source
hashes, scope and live-sweep observations. A partial log is not a passed suite.
No new throughput, fixed-p99, macOS or full baseline success is claimed.

### Slice161 — public new-voter interruption

The two new tests run four TCP/QUIC × WAL/checkpoint histories and pass in39.86s.
Actual bootstrap and executable configuration records enroll store404/incarnation7;
interrupted readiness does not resurrect a lost request. After explicit retry,
new voter4 restarts from committed joint WAL/checkpoint state. The final command
is observed accepted but uncommitted while4 is absent, then completes after
recovery through normal quorum progress. Exact final retry and original data
receipt/value survive whole-cluster restart. There is no seeded configuration
or production protocol change.

All35 existing service tests also pass after the selected-TLS test helper change.
Both strict Clippy profiles have zero diagnostics; formatting/diff and89-record
inventory metadata checks pass. See [slice161 evidence](baseline/slice161/README.md)
for commands, logs, fixture hashes and limits. These are selected process-crash
histories, not arbitrary fault or power-loss proof. The original broad sweep and
new macOS execution remain pending; the full P0–P7 goal is unchanged.

Slice161 additionally corrects the fixed-poll-count fragmented TCP preface
fixture exposed by macOS job114107132231 onf7886bf. Positive stage waits have a
bounded wall deadline while virtual time remains0; timeout/cancel and queue/
ticket/socket-close assertions are unchanged. The full connect target passes
14/14 locally. The failed job excerpt is retained; fresh macOS validation is
still outstanding.

### Slice162 — bounded recovery preparation

The public SnapshotRouter now supports independent recovery request/capacity
admission before preparing images, with exact receipt lifetime and retained
credits. Local checkpoint publication and reconciliation retain their global
budgets. Readiness accounts for both loaded and application images. Five new
host tests cover rejection, retry, stale completion, provider failure, fenced
cleanup and concurrent checkpoint progress. All136 effect-owner tests pass.

All24 learner tests pass, including native readiness with exact two-image
accounting. All20 native benchmark tests pass, including two new TCP/QUIC
histories with eight forcibly stale groups, a one-job recovery quota, foreground
applied write, quorum reads, duplicate receipts and complete restart. Strict
Clippy is zero in both configurations; formatting, warnings-denied API docs
and89-record inventory metadata checks pass. See
[slice162 evidence](baseline/slice162/README.md) for logs, commands and limits.

Previousd36fc71's macOS run passed the fragmented-preface fix but exposed a
separate fixed-poll-count invalid-hint assumption. The fixture now waits for
actual socket closure at unchanged virtual time while checking request and
handshake state; all14 connect tests are checked locally. Fresh macOS CI remains
unverified. This establishes selected admission/progress behavior, not bandwidth
or tail-latency guarantees or complete P0–P7 acceptance.

### Slice163 — combined provider lifetime

Two new generic conformance histories compose shared admission and frame-buffer
providers through native outbound/transport instances, using both independent
host implementations and native implementations. Partial I/O, withheld flush,
protected control progress, failed-connection completion ownership, stale
replacement-generation refusal, sibling resumption and final zero credits pass.
No production behavior or API changes. Host sessions use test attestation;
no new cryptographic or actual-network claim is made.

All49 all-feature transport/admission/buffer tests pass; all8 core-only tests
pass. Both strict Clippy profiles remain zero, formatting passes and inventory
metadata validates89 records. [Slice163 evidence](baseline/slice163/README.md)
records exact commands and limits. The previous162 Linux/macOS CI run was still
active when inspected and is not counted as platform success. Faulted-history
verification is the next mini-plan item; full P0–P7 remains active.

### Slice164 — recorded faulted Counter histories

A bounded checker now independently validates one Counter order scope, including
real-time precedence, idempotent retries, conflicts, overflow and unknown writes
that may be omitted or take effect after uncertainty is reported. It returns a
concrete ordering witness and distinguishes invalid input, invalid history and
exhausted search. Five positive/negative checker tests pass.

Four actual three-process TCP/TLS/QUIC × WAL/checkpoint histories record concurrent
invocations, replies, unread committed write, leader loss, quorum loss, restarts
and exact retries. All produce valid orderings; corrupting each final read is
rejected. The initial recorder omission for ReadNotReady and its failed traces
are retained, with the exact failed-read outcome now recorded and retried.
No production code or semantics changed. Both strict lint profiles remain zero.
[Slice164 evidence](baseline/slice164/README.md) contains commands, raw histories,
witnesses, full service validation and the limits of these selected schedules.

The final corrected service target passes39/39 in41.36s. The five independent
checker tests also pass. Earlier unsuccessful runs remain retained separately;
Linux/macOS whole-tree CI is not inferred from these focused local results.

### Slice165 — automatic physical WAL scheduling

Opt-in Node scheduling and the executable `--wal-reclaim-ms` flag use the existing
bounded worker and crash-tested native reclaim operation.41 Node conformance
tests pass in both all-feature and core-only profiles;6 exercise new scheduling,
manual-receipt isolation, refusal/backoff, shutdown and recovery ownership. One
clock-exhaustion unit test passes. The native hundred-group test requires actual
scheduled byte reduction and preserved retries after reopening.8 log-reclaim and
5 worker-maintenance tests pass. All42 service tests pass, including new TCP/QUIC
scheduled-maintenance/restart histories and invalid-option preflight.

Both strict lint profiles and formatting pass; metadata validates90 contract
records. [Slice165 evidence](baseline/slice165/README.md) records commands,
initial corrections and exact scope. Scheduling physical replacement does not
advance application checkpoints or prove sustainable throughput, incremental
cleaning, physical power-loss safety or macOS execution. Automatic checkpoint
progression remains planned.

### Slice166 — automatic checkpoints on existing durable contracts

143 core-only effect-owner tests pass;6 new automatic-checkpoint Node histories
also pass with all features.11 snapshot-router tests pass. The unchanged storage
mechanisms pass8 log-reclaim,21 snapshot and14 snapshot-worker checks, including
modeled publication/pin/receipt-loss failures. The native100-group automatic
checkpoint/reclaim/reopen history passes, with positive physical byte reduction
and preserved retries. All45 executable tests pass, including TCP/QUIC automatic
checkpoint waves and actual on-disk bases after shutdown. Strict lint profiles,
formatting and91-record metadata validation pass.

[Slice166 evidence](baseline/slice166/README.md) records exact commands and limits.
New policies bound scan/admission work and preserve existing checkpoint/retention
semantics. They do not establish bandwidth or latency guarantees, incremental
cleaning, general backup/retention policy or complete macOS validation.

### Slice167 — maintenance and recovery composition

Eight-group native TCP/TLS and QUIC histories now hold one real recovery receipt
while requiring foreground apply, a durable automatic checkpoint and a later
physical WAL reclaim. After resuming, every stale group must install a snapshot;
another reopen verifies original operations and duplicate receipts. All22 native
benchmark tests pass. The finite history found and now covers speculative
snapshot-reservation deadlock and visits pinned behind unaccepted snapshot
sends. Reservations retain actual payloads; ordinary sends receive a bounded
50ms default retry grace before local discard, without acknowledgements or
changes to accepted work or learner repair.

All152 all-feature and146 core-only owner tests pass, plus2 exact retry-ledger
unit tests. Both corrected configuration and joint-retirement protocol pairs
pass, followed by the complete45-test service target in43.52s. Strict Clippy in both profiles, formatting, warnings-denied docs and
91-record inventory metadata validation pass. [Slice167 evidence](baseline/slice167/README.md)
retains commands, unsuccessful development runs and scope limits. The earlier
commit's Linux CI checkpoint timeout remains a failed remote observation;
current local success does not prove its remote resolution or macOS execution.
No universal fairness, throughput or latency bound, wider device coverage or
complete P0–P7 result is claimed. Bounded operational events are next.

### Slice168 — operational event history

Public EventObserver/EventReporter and the native preallocated ring add bounded,
volatile aggregate events with exact owner/generation cursors, oldest/latest
positions, eviction totals and cursor gaps. Post-poll recording cannot alter the
original Node result. The executable exports bounded Inspect-authorized pages
and rejects old-session cursors after restart.

Six all-feature and four core-only observer tests,147 core-only owner tests,
selected all-feature owner tests and two sequence/export-limit unit tests pass.
Both new TCP/TLS and QUIC service histories pass, including checkpoint events,
full worker joins/reopen, stale-cursor rejection and original operation retries.
The final complete service target passes47/47 in41.11s.
Strict Clippy in both configurations, formatting, warnings-denied docs and
92-record metadata validation pass. [Slice168 evidence](baseline/slice168/README.md)
retains commands, raw unsuccessful service runs and the bounded fixture fixes.
This is not per-group tracing, latency attribution, durable event storage or a
new macOS claim. The broader baseline and lifecycle acceptance work remain open.

### Slice169 — unresolved creation cancellation

Opt-in metadata schema16 records exact creation cancellation using the existing
reserved publication credit and retained operation history. Canceled identities
cannot be reused, late namespace/insertion publication refuses, and parent
lifecycle locks can proceed. Published owners and claimed transfer targets
cannot be canceled. No file reclamation or direct embedded-service revocation
is implied.

The selected lifecycle regression run passes189 tests, and core-only creation/
directory/insertion runs pass55. Seven final cancellation checks include215
native WAL interruption cuts; each recovers an old reservation or complete
cancellation and retries to the original cancellation. Both new native TCP/TLS
and QUIC histories pass unread readiness/cancellation, lagging metadata recovery,
checkpoint/reopen and non-serving target checks. Formatting, both strict Clippy
profiles, warnings-denied docs and92-record inventory metadata validation pass.
All12 selected native creation/created-source regression histories pass in200.81s.
[Slice169 evidence](baseline/slice169/README.md) records commands, raw failures
and the distinction between modeled persistence loss and real process recovery.

The base commit's completed CI run38018887145 passed formatting/both Clippy
profiles but failed selected service tests on both platforms. Linux reported
LeadershipChanged during write/configuration histories; macOS reported
ReadNotReady and a nonzero shutdown exit. Those are retained follow-up failures,
not evidence of completed platform validation. The linked plan now includes
these service regressions alongside recorded lifecycle schedules. Full P0–P7
and macOS acceptance remain unproven.

### Slice170 — service snapshot refusals and exact retries

The first local service run passed47/48, retaining a QUIC child failure at
`Wire(InvalidMessage("snapshot ack boundary"))`. The native codec now carries the
real core's zero-index, higher-term snapshot refusal in formats1–7. A deterministic
public-core test reproduced the failure before the fix and now checks encoding,
no snapshot installation/commit credit, same-term refusal rejection, higher-term
persistence and recovery. Core quorum and persistence rules are unchanged.

The final service run passes48/48 in41.17s, including TCP/QUIC maintenance,
election, membership and restart histories. Raft/snapshot/wire regression targets
pass58 all-feature and21 core-only tests. The CLI's exact read-not-ready retry
has fake-peer coverage; uncertain writes remain terminal and test-owned retries
retain their original identities. Shutdown and checkpoint failures print the
actual child log. Formatting and both strict Clippy profiles pass; detailed
commands, raw development failures and limitations are retained in
[slice170 evidence](baseline/slice170/README.md).

These Linux results do not prove the earlier macOS shutdown failure fixed.
Base-commit test jobs remained in progress at inspection; their lint job passed.
Broader recorded lifecycle faults, discovery refresh and full P0–P7 acceptance
remain open. No new power-loss, multi-host or complete protocol proof is claimed.

### Slice171 — native cancellation/publication race schedules

Sixteen recorded schedules cover publication/cancellation first, unpolled versus
quorum-applied/unread owner loss, and WAL versus checkpoint recovery on TCP/TLS
and QUIC. They require the exact winning operation/duplicate outcome, absence
of unpolled commands, permanent refusal of the losing decision, unchanged
parent metadata, non-serving canceled targets and original data retries on
activated targets while metadata stays stopped. Actual native stores are
reopened; no protocol changes or physical power-loss claim is made.

Both matrix test entry points pass in12.98s; the final16 schedules with exact
original status comparisons pass in12.68s. The complete14-test native creation/
created-source regression selection passes in210.16s. Formatting, both strict Clippy
profiles, warning-denied docs and92-record inventory validation pass.
[Slice171 evidence](baseline/slice171/README.md) retains the schedules, initial
test compile failures and a reproduced QUIC read-authority fixture failure.
Its pre-activation read now uses the existing bounded explicit-refusal retry
helper; the expected quorum read result and all durability assertions remain.
Broader lifecycle interleavings and macOS execution remain open. Prior CI was
cancelled; current base tests were still running at inspection.

## Slice172 — authenticated remote endpoint hints

Linux: `cargo +stable test --locked --offline --all-features --test remote_discovery
--test connect --test quic_connect --test discovery` passes42 tests. Native-only
`--no-default-features --features native --test remote_discovery` passes13.
Evidence: `validation/baseline/slice172/{remote-native-tests,native-only-tests}.log`.

New coverage: fixed-size bounded remote endpoint requests through independently
injected host/native discovery and SecureSession providers; exact completion and
cancellation, bounded cache/retry, expiry under clock skew/delay, source outage
with cached-peer progress, generation floors across authenticated reconnection,
IPv6, malformed/alien/replayed frames and construction/cleanup ownership. Actual
TCP/TLS and QUIC fetch/expiry/source-failure histories pass. A real TCP/TLS
connector uses the remotely fetched endpoint while retaining its target pin and
original refused request despite stale caller input.

Both strict Clippy configurations, formatting, warnings-denied documentation and
inventory are separately logged. Initial lint and fixture/QUIC-idle failures are
retained; the passing logs refer to their focused fixes, not suppressed checks.
This is not macOS/separate-host evidence, external manifest verification,
executable integration or exhaustive network-fault validation. Hints remain
non-authoritative and cannot reactivate retired groups or alter membership.

## Slice173 — credential generation and session revocation

Linux:39 focused all-feature tests pass across authorization, credential_refresh,
quic_connect and secure. Nine core-only authorization/credential/secure tests pass.
The separately recorded final key-rotation check also passes. See
`validation/baseline/slice173` for commands, logs and tested source hashes.

Coverage includes downstream SessionValidity injection, rejected input ownership,
pre/post-I/O revocation, read-output suppression, native generation replacement,
shared-owner isolation, TLS/QUIC reauthentication and updated authorization. A
real TLS key/pin change rejects both a retired certificate and stale trust. Native
PeerTransport returns the original accepted batch as Failed after revocation.
Formatting, both strict Clippy configurations, warnings-denied docs and inventory
validation pass. Inventory shape/path checking is not protocol conformance.

No automatic executable reload, remote secret distribution, durable rotation
journal, macOS/separate-host execution or complete C09/C21/P0–P7 claim follows.
Accepted external progress cannot be undone by credential replacement.

## Slice174 — durable live command-credential reload

Linux:50/50 service tests pass, including both new unread-reload/revocation/restart
histories. An additional31 credential/security and3 executable unit tests pass.
Core-only checks pass9 and native-only journal checks pass4. Formatting, both
all-target strict Clippy profiles, warnings-denied docs and94-record inventory
checks pass. `validation/baseline/slice174` contains commands, failed/final logs
and source hashes; no filtered-out target is counted as tested.

Public fixed-record journal host injection checks exact retry, owner/sequence/
generation rejection, pre/post replacement uncertainty, byte corruption and
partial staging. Controlled preparation checks single-flight admission, join
and fencing. Service histories verify current generation after unobserved
replies, revoked writer permissions, intact application retries, invalid-file
refusal and restart rollback/same-generation digest refusal.

An old administration test assumed a discovered leader stayed stable until reply;
its fix retries only the documented uncertainty with the identical operation.
A later overlapping test invocation encountered AddrInUse; its actual recovery
log is retained and the final full service run executed alone. No production
consensus rules or lint limits were weakened. Credential preparation is local,
not a replicated membership change or full audit history. Physical power loss,
macOS/separate-host validation, peer key orchestration and remaining P0–P7 gates
are not established by this slice.

## Slice175 — peer close before send admission

The deterministic host regression first failed with the same fatal
PeerRosterError::Transport(Closed) seen in the slice173 macOS service log. After
the fix it verifies exact rejected-ticket retention, backoff, a fresh connection
generation, unrelated-peer progress and eventual delivery. A companion test
retains fatal handling for wrong-binding rejection. No closure is reclassified
as a successful local send or remote durable acknowledgement.

Final-source Linux checks in validation/baseline/slice175 pass26 peer-driver,
18 roster,28 transport and50 service tests. Core-only checks pass18 roster and
one transport-limits test. The full service invocation ran
without another counter_service invocation. The selected hundred-group
checkpoint test passes in14.87s; the retained Ubuntu CI15s timeout remains
unresolved rather than being declared fixed by a single local pass. The two
service fixture fixes preserve timeout, exact operation and sampled-peer
assertions while accounting for BSD accepted-socket flags and leader changes.

Formatting, both strict all-target Clippy profiles, warnings-denied docs and
94-contract inventory validation pass. The inventory check is metadata only.
The starting-revision all-feature sweep is still live; its committed log snapshot
is incomplete and predates this patch's build. Its active output is retained at
/tmp/vb-slice175-all-features.log. Source hashes and command records distinguish
that observation from the final-source selected tests. No complete baseline,
macOS, separate-host or full-roadmap result is claimed.

## Slice176 — bounded duration diagnostics

The public TimingObserver contract is exercised through a downstream host sink
and NativeTimingObserver. Ten observability tests pass with all features, seven
with no default features and ten with native alone. Tests cover exact owner and
generation, nonregressing time, closure, preserved rejected state, every power-of-
two boundary, zero/u64::MAX durations, nearest-rank upper bounds, inconsistent
histogram refusal and checked count/total exhaustion. Four executable unit tests
pass, including maximum timing export below the4096-byte reply limit and visible
sample rejection after closure.

All52 Linux service tests pass in43.88s, including two new actual TCP/QUIC
histories. A dropped pre-authentication connection increases interrupted timing;
normal command connections increase completed timing. A principal scoped to a
different group cannot inspect the distribution. A recovered service starts
with an empty distribution and a higher store session. Formatting, both strict
Clippy profiles, warning-denied docs and95-entry inventory validation pass.
Artifacts and changed-source hashes are in validation/baseline/slice176.

Connection completion is local flush, including error responses, not client
receipt or successful state-machine application. These aggregate distributions
do not establish a benchmark improvement or an additive critical path. The live
baseline175 process and CI175 platform jobs have no terminal result in this
record; no full baseline, macOS or separate-host pass is inferred.

## Slice177 — quorum explanations and recovered full-run evidence

Recovered the original159 process handle from committed metadata. Its terminal
result is exit0:74 target results and1186 passing tests. The complete raw output
is retained in validation/baseline/slice177 with the original metadata and a
summary. This build predates160 and is historical integration evidence only.

Baseline175 is still live. The log move crossed filesystems, so the live writer
kept an unlinked inode while /tmp held a stale copy. A reader now follows the
original descriptor at /tmp/vb-slice175-live.log, with --pid tied to its cargo
process. Fresh phase completions are observed. No tests were restarted; ptrace
was unavailable and no host security setting was changed.

The new Policy/JointPolicy explanation tests pass6/6 with all features and6/6
without default features. They cover all512 nine-voter sets against existing
evaluation, exact immediate-child weights, unknown IDs, maximum weights and
joint root requirements. Six executable unit tests pass, including stable/joint
pagination and maximum-size output. All54 service tests pass in42.78s. Two new
TCP/QUIC histories verify local inspection without a voting quorum, bounded
pages, hypothetical evidence, authorization and restart. Formatting, both strict
all-target Clippy profiles, warning-denied docs and95-record metadata validation
pass; source hashes and commands identify the tested revision contents.

These are diagnostic views of supplied node IDs. They do not establish network
liveness, authenticated/durable acknowledgements, or permission to commit. The
quorum predicate and consensus protocol are unchanged. No current full-suite,
macOS, separate-host or performance-gate success follows from the historical run.


## Slice178 — static local lanes

All28 native benchmark tests pass on Linux; the final six focused lane tests
also pass after the last fanout adjustment. The independent lane validator passes
seven cases and checks retained TCP/QUIC samples, disjoint assignment, global
window/rate/latency arithmetic and native storage counters. Formatting and both
strict Clippy profiles pass with zero diagnostics. Evidence, source hashes and
scope are in baseline/slice178 and performance/slice178.

Three release runs pass actual file reopen, historical retries and worker joins.
They are finite, busy-host composition checks, not sustainable capacity or a
fixed-p99 improvement. One/two-lane TCP p99 is706.891026/1075.042194ms; two-lane
QUIC p99 is1058.867266ms. The original serial250ms gate remains open. Ubuntu CI
for432a6e9 failed three service authority-race histories; raw output is retained
and no fix is inferred. macOS and baseline175 remain pending at capture.


## Slice179 — exact read-transition retries

The fake-peer read regression reproduces Unavailable(LeadershipChanged) before
the client change. Auto read now obtains a fresh invocation for that exact reply;
explicit reads and uncertain writes retain their previous terminal behavior.
Malformed, partial and other errors do not trigger the new retry path.

All54 local service tests pass in42.83s, including the three histories diagnosed
from the saved Ubuntu job. Ten core-only read-invocation tests pass, and format,
both strict all-target Clippy profiles, docs and inventory checks pass. See
baseline/slice179. This is selected current-source evidence, not a claim that the
pending original full baseline or macOS jobs passed. P0–P7 scope remains active.

## Slice180 — placement replacement/removal plans

Common public planning consumers now prepare a replacement learner without
flattening recursive policy, and generate explicit target-policy joint/final
records using only current exact voter/learner stores. Both retirement choices
are tested. Plans grant no readiness or commitment and cannot bypass ordinary
Node admission or joint-commit timing.

All13 planning,8 placement and32 membership tests pass with all features;
11 planning and18 membership pass without defaults. All54 TCP/QUIC service tests
pass in42.39s, including planner-produced native-authorized leader demotion,
unread joint replies and WAL/checkpoint resumption. Formatting, both strict
all-target Clippy profiles, warning-denied docs and95-contract inventory pass.
Evidence: [slice180](baseline/slice180/README.md). These checks do not complete
P0–P7 or establish automatic relocation, remote manifest fetching, physical
power-loss safety, current macOS/separate-host coverage or the unmet P7 p99 gate.

## Slice181 — publication-step attribution, fixed gate still failing

Optional fixed-size native counters now distinguish actual manifest staging
open/write/file-sync, rename and directory-sync operations. Ordered publication,
error propagation and all synchronization calls remain. Selected native primitive
interruption and public real-file error/recovery tests pass. Both strict Clippy
profiles, formatting, warning-denied docs, native benchmark tests and checker
negative controls pass. Details and raw outputs: [slice181](performance/slice181/README.md).

Both256-operation TCP runs recovered320, preserved retries and joined workers.
Uninstrumented reference p99956.095274ms fails the unchanged250ms budget.
Instrumented p99733.740841ms cannot establish a tuning gain. This was a shared
host; the earlier full-suite run remained active on tmpfs. Actual publication
means are dominated by file and directory synchronization, rather than staging
open/write or rename. Parallel sums and live partial snapshots are not client
critical-path measurements. P7 sustainable performance and other P0–P7 acceptance
remain incomplete.

## Slice183 — measured replication-overlap experiment rejected

A tested Written-stage leader replication candidate was evaluated against the
unchanged native disk reference. Control p99 was873.493ms; the first candidate
failed after a leadership change; its uninstrumented repeat had1107.214ms p99.
The diagnostic completed but cannot replace the reference gate. Successful runs
passed full native reopen/retry/join checks. The original250ms budget remains
unmet. No performance improvement is claimed and the production source was
restored. Candidate code, focused safety/ownership tests, failures, raw samples,
binary hashes and scope remain in [slice183](performance/slice183/README.md).
Both the removed experimental tree and restored default receive separate local
formatting/strict-Clippy verification. Full P0–P7 and platform evidence remain open.

## Slice184 — authenticated remote manifest discovery

Seven new host tests and two native TCP/TLS/QUIC histories validate the bounded
remote ManifestDiscovery provider, original directory-read ownership, three-level
routing and offline child service with owner rejection. The complete selected
regression commands pass 32 all-feature cases; default and native-without-TLS
configurations also pass their focused cases. A pre-existing optional-QUIC enum
reference exposed by the default build was feature-gated, and the default strict
Clippy profile was added to the enabled push hook and CI. All three Clippy
profiles and formatting are clean. Exact commands, scope and logs are in
[slice184](baseline/slice184/README.md). No executable discovery integration,
new macOS/separate-host result or complete P0–P7 certificate is claimed.

## Slice185 — source membership and split recovery composition

Four native TCP/TLS/QUIC × WAL/checkpoint histories pass with a source fence
committed during joint membership. They preserve the unread original result,
reject its stale ticket after abort/reopen, retain exact fence/export and import
configuration lineage, finalize the same configuration operation, and recover
independent child retries without duplicate outbox work. All50 native-member cases pass on Linux. One existing full-phase
static split history also passes after the fixture starts using observed current
configuration IDs. No production protocol or durability rule changed.
Formatting and all three strict Clippy profiles are clean; see
[slice185](baseline/slice185/README.md) for commands and phase traces.
The older macOS job114126028727 at1b460ef separately reported two failures;
its saved excerpt is evidence of unresolved platform work, not this slice passing
on macOS or proof of the cause. Broader P0–P7 acceptance remains open.

## Slice190 — executable recursive metadata lookup

The route CLI uses bounded authenticated authority discovery and the existing
checked resolver. All13 Directory executable tests pass (TCP/QUIC),11 pass with
default features, and all65 counter regressions pass after explicit test-source
readiness. Tests include three-authority traversal, stale/incorrect observations,
missing required paths, leader loss and interrupted leaf reads. Formatting and
all three strict all-target Clippy profiles have zero diagnostics. Exact commands,
failures, corrections, source hashes and limits are in
[slice190](baseline/slice190/README.md). No current macOS/separate-host or complete
P0–P7 validation is claimed; the original P7 latency requirement remains open.

## Slice191 — explicit placement artifact and runtime execution

The offline planner emits original learner/replacement/voter administration
records with exact store bindings. Five selected all-feature cases and four
default cases pass; the final sequential run passes70 counter,8 placement and
13 planning tests. Native TCP/QUIC histories stop before enrollment, refuse
premature promotion and recover the same plan after checkpoint restart. All
three strict Clippy profiles and formatting are clean. An invalidated concurrent
feature-build run is retained with its demonstrated cause, not counted as a
pass. See [slice191](baseline/slice191/README.md) for commands, source hashes,
raw results and the offline/operator scope. P0–P7 completion and current platform
proof remain open.

## Slice192 — baseline requirements and current quorum evidence

At639c1fb, the acceptance ledger now maps R01–R19 and all twelve chapter09
operations alongside the existing P0–P7, VB-000–011, invariant and component
catalogues. Source inspection corrects stale claims about recursive lookup,
explicit placement execution, unknown-outcome Counter histories and public
assignment iteration. It identifies the remaining operator surface and keeps
validation/performance gaps separate. No production code changes.

Fresh all-feature checks pass4 activation-model,6 quorum and22 actual-core Raft
tests. The latter include32 seeded256-action schedules with power-loss recovery,
partitions, message duplication/reordering and committed-prefix checks. This
is finite selected evidence; it does not substitute for reference-Raft
comparison, fuzzing, a general replay/minimization harness or a full protocol
proof. Format and strict all/default/core-only Clippy finish with zero
diagnostics;95 inventory records pass shape/path validation only.

Baseline175 cargo1526695/routed1580417 are confirmed live at capture. CI38025706754
for639c1fb is pending without jobs. No current full-suite or macOS success is
claimed. Original slice183 fixed250ms performance gates still fail. See
[commands, provenance and logs](baseline/slice192/README.md).

## Slice193 — pure transfer preview and native command

The new public preview uses the existing scope provider and checked transfer
intent contracts. Native/host tests check split/merge mapping, exact proposed
placement, retained source scopes, schema/provider-bound failures and unchanged
source/target state. A hostile provider panics if preview tries exporting or
importing data. Native CLI tests exercise recursive policy input, exact store
incarnations, malformed/oversized/unsupported profiles and no-success output on
failure. The CLI explicitly reports configured bounds rather than live data.

A first retained test failed because the moved range was used as the original
ownership scope. The corrected implementation subtracts transferred ranges
from the original manifest and verifies the source application covers them.
The failure and passing regression are retained. No lifecycle mutation, quorum
rule, persistence or wire-format change is introduced. Native placement and
counter policy parsing reuse shared implementations.

Fresh results:36 all-feature scope/placement/planning/CLI tests;11 core-only
scope tests;1 selected retained preview;5 counter placement tests including TCP
and QUIC recovery. The3 CLI cases pass again after rendering extraction. All
three strict Clippy profiles and formatting pass with zero diagnostics;96
inventory records pass metadata validation only. See
[commands and evidence](baseline/slice193/README.md). These selected tests do not
establish full baseline/platform acceptance or actual target import readiness.
