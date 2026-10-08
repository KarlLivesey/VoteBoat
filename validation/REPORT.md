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
