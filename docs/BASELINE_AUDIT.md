# Baseline audit — slice136

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
