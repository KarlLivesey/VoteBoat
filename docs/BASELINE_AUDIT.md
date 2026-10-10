# Baseline audits

Latest implementation follow-up: [slice175](#slice175--platform-closure-and-live-validation-follow-up).
Requirement ledger: [review159](#review159--current-requirements-and-concrete-next-boundary).
Earlier reviews below retain their original revision and scope.

## Slice175 — platform closure and live validation follow-up

Actual slice173 CI logs expose fatal service handling of a peer closing between
poll and send admission, BSD accepted-socket test assumptions, metrics tests
assuming stable leadership, and an Ubuntu hundred-group checkpoint timeout.
Slice175 fixes the close path without changing rejection ownership or consensus
authority, and corrects those two fixture assumptions. The deterministic close
regression was red before the fix;26 peer-driver,18 roster,28 transport and50
service tests now pass locally. The checkpoint test passes alone in14.87s but
its earlier15s timeout remains an open observation, not a proven production fix.

The full all-feature sweep at3b4d324 is still running serialized routed histories;
its snapshot in validation/baseline/slice175 is not a completed run. An older
slice159 sweep is also still live and progressing. These actual live processes
supersede earlier assumptions that handles were absent; neither supplies a
terminal passing result yet. CI at3b4d324 has passed lint, with platform jobs
still running at the recorded observation. Local targeted tests and code review
do not establish macOS compatibility or close the remaining full-scope ledger.

## Original review136

Audited production revision: b2a367e48e045c1776f38aafb4826d4d50462b24.
The full P0–P7 objective remains active. This is a requirement review, not a
completion certificate. See BASELINE_ACCEPTANCE.md for the component and roadmap
ledger, IMPLEMENTATION.md for linked plans, and validation/REPORT.md for actual
execution. Reading source and historical reports is not fresh test execution.
RPL-1.5 remains; macOS/Linux are targets, Windows and P8 are deferred.

## Roadmap findings

- P0: public component contracts, typed identities, injected providers and finite
  virtual-time/fault tests exist. Catalogue coverage is partial; an inventory
  count does not establish full conformance or a complete arbitrary-fault engine.
- P1: native durable three-node service and Rust embedding exist, including
  elections, replication, snapshots, reads and operation-ID retry. Selected Linux
  TCP/TLS and QUIC histories cover restart and interrupted progress. General
  faulted-history checking, macOS and separate-host execution remain unverified.
- P2: shared scheduling, WAL/transport workers and bounded runtime paths exist.
  Hundred-group tests are finite evidence. Operational multi-shard deployment,
  wider workloads, resource fairness and broader failure isolation remain open.
- P3: elections/reads use effective membership predicates in src/raft.rs;
  commitment uses its durable-prefix predicate. src/membership.rs requires both
  stable and next predicates during joint configuration. tests/quorum.rs,
  tests/raft.rs, tests/snapshot.rs and tests/activation_model.rs contain recursive
  policy coverage. This site inspection does not prove all runtime histories.
- P4: learner/session readiness, replicated joint/final membership, checkpoint
  recovery and selected native lost-reply/restart histories exist. Broader remote
  revocation, old checkpoint combinations and overlapping fault schedules remain.
- P5: cached routing, selective assignments, checked creation and same-authority
  root/nested insertion exist. Ordinary directory publication rejects parent or
  authority changes; insertion requires the current parent/authority and fresh
  identities. Deletion/reparenting and moving a metadata authority's own data are
  missing paths. Automatic remote refresh and broad placement remain partial.
- P6: actual data split/merge, source fencing, guarded activation, retry lineage
  and selected repeated/nested movement recovery exist. Slice134 retirement is
  deterministic/native-file evidence;135 networked later movement does not prove
  networked retirement. General retention and recursive lifecycle faults remain.
- P7: shared barriers, reclamation and benchmark modes exist. The fixed serial
  p99 gate still fails; sustainable capacity, controlled recovery throttling,
  operational lane placement and broader device/platform evidence remain open.

Supporting seams must retain their ledger limits: C14 encoded frame leases are
not a complete shared-buffer admission policy; C15 outbound admission is not
client/disk/connection fairness. C17 discovery observations are not automatic
remote refresh. C18 learner selection is not automatic global rebalancing.
C19 volatile counters are not stage-latency telemetry. C20/C21 configured ingress
is not validated live credential rotation or durable principal auditing. C22 has
native framing/integrity but no selectable compression provider. C24 cross-group
transactions are optional P8, not a prerequisite or mandatory global coordinator.

## Invariant evidence and remaining validation

These are source/test pointers and historical selected coverage, not new runs.
The normative definitions remain chapter11; none is proved universally here.

| Invariant | Existing evidence path | Remaining scope |
| --- | --- | --- |
| I01 persistent vote identity | voting/WAL contracts, tests/raft.rs and restart histories | Wider term/persistence/message races |
| I02 matching committed history | actual-core fault schedules in tests/raft.rs, membership histories | Complete arbitrary-fault histories and formal model |
| I03 contiguous durable prefix | log-store completion/barrier and shared-barrier tests | Broader provider/device failure combinations |
| I04 effect dependency ordering | injected storage/runtime failures and native WAL faults | All composed failure and cleanup schedules |
| I05 validated effective policy | quorum reference, membership predicates, recursive election/read/snapshot tests | Full configuration-transition protocol evidence |
| I06 exclusive transferred owner | transfer/routed phase tests, native guarded activation | General recursive lifecycle and networked retirement |
| I07 acknowledged operation recovery | native WAL/checkpoint reopen and replay histories | Power-loss/device assumptions, macOS, separate hosts |
| I08 operation-ID deduplication | counter/routed retry, snapshot and transfer lineage tests | Wider lost-response and mixed lifecycle histories |
| I09 valid snapshot boundary | snapshot-store and membership checkpoint interruption tests | Broader partial-install/corruption combinations |
| I10 bounded resources | queue/reserve/runtime/admission tests | General disk/client/buffer fairness and overload deployment |
| I11 incarnation provenance | ticket/session/owner identity checks and stale-proof tests | Broader stale disk/packet reconnection schedules |
| I12 independent child authority | selected parent-outage and immutable stopped-file histories | Broader authority movement and recursive outage histories |

Chapter11 scenarios1/2/6 have selected core/native persistence and membership
coverage;4/5/9 have selected routed parent-outage, phase-recovery and retry
histories;7 has idle/election evidence but no general dormancy protocol claim;
8 has compaction/maintenance experiments, not comprehensive disk-full/control
contention validation;10 has stale identity/fence refusal, not every remote
reconnection history. Scenario3's striped single-group storage is deferred P8;
ordinary contiguous-prefix/stale-completion checks remain baseline obligations.
The selected long QUIC forced-snapshot experiment in slices105–106 still fails
its group1 boundary gate: buffered traffic may allow log repair instead. Another
group's snapshot cannot satisfy that gate. No broader success is inferred.

## Fresh performance evidence and next decision

Artifacts: validation/performance/slice136. Release example compiled successfully
with all features. Rust production and benchmark sources were unchanged.
Initial /tmp measurement used tmpfs; it is retained as a memory-filesystem smoke
and excluded from disk performance acceptance. Fresh sequential Btrfs roots then
used the original reference: TCP/TLS,3 replicas,1 group,64 warmup,256 measured
8-byte increments,window1,heartbeat50ms,election1000–1999ms.

Disk A stopped at operation250 with Unknown(LeadershipChanged). Its header-only
CSV and failure log provide no complete performance or recovery result. Disk B
completed:5.749 applied operations/s,p99=432.366616ms,recovered320,original retries
verified and workers joined. Independent raw validation passes; the unchanged
250ms p99 gate fails. No preferred rerun, relaxed deadline or weakened durability.
These finite runs do not establish sustainable capacity or a specific cause.
The tmpfs/disk difference is not a controlled attribution experiment.

Next deliverable: retain partial measurement receipts/outcomes and attribute the
actual startup critical path, including the leadership-change failure. This
benchmark diagnostic is needed because early failure currently discards collected
samples and does not distinguish storage waits from owner/peer progress. It
advances P1/P7; it is not another prerequisite for using the static service.
After evidence identifies a cause, make the smallest contract-preserving fix and
repeat matched disk-backed measurements with the same correctness and250ms gate.
Keep all other ledger gaps active; do not replace P0–P7 with a performance task.

## Slice143 source review and next baseline decision

Reviewed starting revisionff9db49, chapter12 exits/chapter17 catalogue and current
buffer/admission/native transport/contracts/tests. Historical136 findings above
remain historical. Since then138 adds optional native file stage timings;139
retains native publication-cut tests and rejected tuning evidence;140–142 supplies
selected guarded nested split/merge retirement/replay/reclamation, including actual
assigned lineage and partial merged-source cleanup. General retention/lifecycle
faults, broad platforms and sustainable fixed-p99 performance remain unproven.
These are linked in the updated acceptance map, not counted as universal proofs.

The selected next missing baseline path is C14 shared encoded control headroom.
Source inspection showed a single undivided pool can exhaust frames even when
C15 outbound control queue capacity is reserved. BufferPool version2 now offers
explicit class acquisition/optional control reserve; native counters enforce total
and restricted bulk limits. Existing undivided selection retains total-only
counter operations. Actual validated batch contents classify sends; unclassified
receives cannot borrow control reserve. Native/downstream saturation, rollback,
concurrency, flush/abort and invalid declaration tests execute that path. This
closes this selected resource gap, not per-owner/receive/connection fairness.

Slice144 adds selected per-authenticated-peer Bulk quotas with checked
non-overbooking and bounded owner registrations. Same-store reconnects retain
held frame credits; different store incarnations use separate slots. Public
native/downstream tests cover other-peer Data delivery under quota pressure,
Control headroom, delayed flush, registration lifetime, concurrent reconnect
holders and exact failure/drop rollback. These host-attested pressure fixtures
are not an encrypted Node load claim. Default service selection is unchanged.
Slice145 exercises an actual TCP/TLS100-group native Node assembly with one
full peer quota held across disconnect/reconnect. Eight groups write/read using
the other peer while the pressured follower stays at its old value; release
permits catch-up. Joined shutdown returns all frames, then durable reopen verifies
original retries and a fresh write. The initial half-quota test assumption failed
because completed receives legitimately return credit; full-quota setup is the
documented focused correction. This is one finite Linux schedule, not arbitrary
fairness/fault, selected QUIC pressure or performance evidence. Slice146 source
review found fixed QUIC Dial endpoints blocked the existing discovery wrapper;
explicit discovered-Dial construction now implements that missing selection.
Actual native/host hint refresh authenticates and exchanges QUIC data, preserving
pins and bounded retained peer leases. Slice147 now supplies automatic original Directory-read lookup/refresh for Rust
hosts; slice148 composes actual routed writes/reads/retries, exact invalidation
refresh, offline-parent operation and WAL/checkpoint restart over TCP/QUIC.
Next implement a missing recursive ownership operation. Retain original P0–P7
scope: wider lifecycle/configuration/discovery/placement/platform/fault and P7
fixed250ms/sustainable-throughput gates stay active. No new performance runs or
macOS execution in143–146. Windows/P8 stay deferred and CI remains background.

Slice149 closes the explicit same-authority restriction at the checked nested
insertion path through opt-in Directory schema7 and separate canonical tags.
The child's local creation records remain authoritative; foreign parent reservation
and child publication observations retain authenticated host provenance obligations.
Actual deterministic data handoff/checkpoint recovery and native intent-frame faults
are exercised. Slice150 adds selected real TCP/TLS and QUIC WAL/checkpoint
separate-authority phase recovery with original command retries, partial assigned
provisioning and independently activated successor service with metadata/source
owners stopped and durable files unchanged. Retained-scope insertion is current151. This advances P5/P6 feature coverage without
closing broader lifecycle, platform/fault or original P7 performance gates.

Slice151a implements the missing scoped ownership guard needed for retained-source
service, selected before bootstrap through the same public RoutedApplication seam.
Transferred ranges refuse while the original group serves retained ranges, with
original fence/retry/outbox facts through checkpoint/replay and selected native
WAL-model interrupted-frame/barrier faults. This advances151 but does not close it:
immutable final scoped export, mixed retained/delegated metadata publication and
activation remain, followed by152 native network composition. No whole-source
handoff is relabeled as retained ownership. Original P0–P7 gaps remain active.

Slice151b completes the immutable scoped source export path: original images,
digests and fixed capacity reservations survive checkpoint/replay while the same
source continues retained writes. Native BucketCounter and independent padded/
wrong-boundary host providers use the same public seam. Selected native WAL-model
faults recover old state or exact fence/image together. Current151 still requires
mixed intent/publication, retained-grant adoption and target activation;152 will
compose the complete path natively. Source/provider import is not activation.

Slice151c adds opt-in schema8 retained-child insertion: one fresh Staging child
receives a strict source subrange while metadata preserves the remainder on the
original group. Explicit intent/plan/scoped-publication tags preserve old formats;
actual creation and original fence/import facts precede atomic publication and
child activation. Root/foreign nested data/retry/checkpoint paths, all old-directory
profile refusals, pre-intent cancellation and native intent-frame faults pass.
The source still retains its E1 grant; fresh E2 hints refuse until checked source
binding/grant adoption151d. Complete native partial service evidence follows152.
This advances P5/P6 without closing the full lifecycle or platform/fault/P7 gates.

Slice151d1 opts source schema2 into exact retained-intent binding at scoped freeze.
Original intent digest/F/image recover together, provider/control ID collisions
refuse, and older raw profiles remain separate. Actual root/foreign handoffs use
bound facts. Source grant adoption151d2 and native complete composition152 remain
required; source E2 retained hints still refuse until that transition exists.

Slice151d2 closes the checked retained-grant adoption gap for the bounded original
source path. Opt-in source schema3 preserves its original routed history guard and
records actual publication-backed active grants at the same committed prefix.
Root/foreign E2 and repeated actual child E3 data/retries/exports recover; stale or
transferred contexts cannot bypass the active grant or original scoped fences.
Ordered pending, checkpoint/profile/budget refusals and native adoption-frame cuts
pass. Current152 still requires complete TCP/QUIC partial-service recovery evidence;
153 deletion,154 reparenting, target-backed partial sources/general mappings/retention
and original wider lifecycle/membership/platform/fault/P7 scope stay open.

Slice152a adds selected native TCP/TLS and QUIC WAL/checkpoint root partial service:
actual quorum creation/partial assignment and unread intent/stage/fence/import/
publication/adoption/activation results recover through original facts/retries.
Source retained E2 and activated-child service continue with metadata stopped;
independent data-owner recovery preserves values/retries/exports while exact
metadata files/GroupLogs stay unchanged. Current152b is foreign-parent composition;
153 deletion,154 reparenting and all original broader scope/gates remain active.

Slice152b completes selected native foreign-parent retained insertion evidence:
actual reservation/recovery binds the child intent; child publication precedes
parent completion and exact locator refresh. TCP/QUIC WAL/checkpoint unread-phase
recovery4/4 passes. Both metadata authorities remain stopped while retained source
and activated child serve/recover; original stopped metadata bytes/logs remain
unchanged. Root TCP/WAL factory regression passes. No production protocol change.
Current153 deletion, next154 reparenting, following155 target-backed partial sources
and wider original scope/gates remain open; Linux evidence, macOS pending.

Slice153a adds schema9 recursive deletion: bounded original manifest reservation,
checked full-owner fences and published child tombstone projections precede
retained Fenced manifests and exact immutable retry history. Leaf/multiple-owner,
same/foreign and three-authority conformance, legacy/bounds/checkpoint/capacity/
ordering refusal and native intent/tombstone journal faults pass. Native TCP/QUIC
resumption remains current153b; reparenting154, partial sources155 and wider original
scope remain open. Physical reclamation and unresolved-creation cancellation are
not granted by this deletion protocol. See DELETION.md and IMPLEMENTATION.md.

Slice153b adds fixed quorum full-fence reads over the original routed owner, with
unchanged commands/schema/checkpoints and host bounds. Selected native TCP/QUIC
WAL/checkpoint two-authority recursive deletion histories pass original unread
results/reopen/retry facts. Both metadata authorities stop during independent owner
recovery; retained values/retry/outbox and exact stopped metadata files/GroupLogs
remain, old service stays fenced, late metadata recovery cannot thaw it. Current154
reparenting, next155 activated partial sources, following156 metadata movement;
wider owner families, retention/cancellation and original scope/gates remain open.

## Review159 — current requirements and concrete next boundary

Starting revision1231153. Chapter12's P0–P7 exits and VB-000–011, chapter11's
invariants/scenarios and chapter17's24 catalogue entries were rechecked against
current source and tests. BASELINE_ACCEPTANCE.md is now the current compact
ledger; earlier sections in this file remain historical. No completion percentage
is inferred from implemented code or test counts.

Deletion, reparenting, retained-child insertion and moving metadata authority
are no longer absent features. They have bounded application contracts, native
journal fault tests and selected TCP/TLS/QUIC phase/reopen histories. Selected
later imported/retained owner transfers and retirement now follow those moves.
Broader lifecycle, retention and failure coverage remains. Public authenticated
configuration endpoints also exist; their unsupported fault combinations remain
listed instead of categorizing the whole endpoint as missing.

The concrete next P5/C17 gap is visible at the native lookup adapter. Its Node
implementation requires Query=ResponsibilityIdentity and
ReadResult=Option<ResponsibilityManifest>. The original Directory satisfies that
shape; LifecycleDirectory and metadata source/publishing/serving wrappers use
explicit enums. Their authoritative read branches already reject Fenced or
NotActive and distinguish current manifests from inherited history. Native
metadata movement tests manually consume these reads; automatic lookup tests
exercise the original Directory only. Thus metadata migration itself works in
selected histories, while automatic lookup cannot yet be assembled for its
serving profile through the native convenience path.

A follow-on must adapt query/result shapes through an explicit public boundary,
keep original Node read tickets/bindings and bounded cancellation, and accept
only the current-authority manifest branch. Historical metadata results cannot
silently become current observations; source fencing and destination activation
must still be enforced by the application. No new runtime, log, fallback identity
or remote bearer proof is needed. Exercise the adapter before/after an actual
metadata move and after restart, with inactive/fenced and late completion cases.

P3 source inspection still finds the four expected quorum uses: durable campaign
self-vote, received election votes, read readiness and commit frontier. Both
accepted joint policies are evaluated by Membership; commit remains bounded by
the local durable prefix and current term. Nine-voter/reference tests and the
independent bounded activation model complement this source review. The
32-seed/256-action native-WAL model in tests/raft.rs covers selected power loss,
partition, duplicate/reordered message and committed-prefix histories. It is
not a general minimizer, differential Raft runner or unknown-outcome
linearizability checker; those broader chapter11 obligations remain open.

The full goal also retains P2/P7 operational lane placement, general shared
resource admission, recovery throttling, broader provider/device/host coverage
and sustainable improvement under the original fixed-p99 budget. No new
performance run is required to know the retained serial gate is still failing.
Optional compression, specific external adapters, global automatic split/merge
orchestration and P8 transactions are distinguished from mandatory baseline
behavior. A core-only dependency tree still includes ring and its cryptographic
support; it excludes the selected native providers, rustls and QUIC. It is not
advertised as dependency-free.

Review of completed GitHub run38015395454 (revisiona803a8b) found a successful
format/strict-lint job, three Linux service test failures and one macOS QUIC unit
test failure. Linux data checks reused an earlier leader ID after membership or
restart; reported errors were NOT_LEADER/Unknown(LeadershipChanged). They now
use the existing automatic routing and original-operation retry path. The macOS
fixture treated UDP send completion as immediate receive availability; only its
positive receive assertions now poll WouldBlock with a bounded deadline. Queue,
packet, stale-lease and negative assertions remain exact. No production timers,
protocol semantics, test serialization or CI limits are relaxed.

Fresh local test results and the final platform status belong to this review's
validation record. A prior or partial remote run cannot establish current macOS
acceptance, and CI is background feedback rather than an implementation gate.

Update160: the read-shape gap identified above now has a public stateless
ManifestReadMapping and owning MappedManifestReadSource. Four selected native
TCP/QUIC × WAL/checkpoint histories resolve current routes before/after two
metadata moves, reject inactive/fenced sources, drain cancellation ownership and
recover rejected original typed completions. The earlier paragraph records the
pre-implementation finding. Broader remote discovery remains open. See
validation/baseline/slice159-160 for final focused results, strict-lint evidence,
retained failed attempts and explicitly pending full-suite/platform checks.
The latest completed CI38015646068 also exposed a client-target configuration
leadership-change assumption; its fixture correction preserves exact operations
and does not add automatic admin retry to the CLI.
