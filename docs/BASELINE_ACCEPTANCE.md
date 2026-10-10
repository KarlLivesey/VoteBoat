# Baseline acceptance map — review192, updated193

Current review starts at 639c1fb (slice191). The roadmap and catalogue below
retain historical test scopes, with corrections from source inspection and
fresh selected tests in [slice192](../validation/baseline/slice192/README.md).
This is not a full-suite or release certificate. The first review started at
1231153; do not attribute that review's results to today's source.

The original review started at revision1231153 against design-pack chapters11,12,17 and the
component contract specification. This is a current requirement ledger, **not a
completion certificate**. Selected tests do not prove all schedules. Detailed
historical results remain in IMPLEMENTATION.md, BASELINE_AUDIT.md and
validation/REPORT.md; validation/baseline/slice159-160 retains fresh results and
any failures. The original pre160 full all-feature sweep is now verified complete:
74 target results,1186 tests, exit0. Its historical source scope is not a current
full-suite pass. Baseline175 is still running with its original handle; repaired
live output capture is documented in slice177. Slices162–167
add recovery-budget, provider-lifetime, Counter client-history, automatic physical
reclamation, automatic checkpoints and maintenance under held recovery pressure.
Slice167 composes both maintenance policies with actual eight-group snapshot
recovery over TCP/TLS and QUIC. Selected histories require foreground apply,
durable checkpoint and later physical reclaim while a recovery receipt remains
held; then all stale groups recover and original retries survive another reopen.
This covers the selected composition, not general fairness or a latency bound.
Slice168 adds bounded post-poll operational history with host/native injection,
explicit overflow/cursor gaps and scoped service export. No observer becomes
consensus authority. Slice176 adds bounded poll and connection timing;
per-group/critical-path attribution remains open.
Slices169–171 add unresolved-creation cancellation, repair a native snapshot
refusal/codec mismatch and exercise16 native cancellation/publication race
schedules across owner loss, unread receipts and WAL/checkpoint recovery. The
competing decision is rejected after recovery; a canceled target stays
non-serving, while a published target preserves data retries with metadata
stopped. These selected schedules do not close general lifecycle fault coverage.
The full P0–P7 objective stays active. RPL-1.5; Linux and macOS targets.

Slice187 adds authenticated, explicitly configured remote command listeners and
client endpoint lists. Native executable TCP/QUIC tests use non-default command
ports, enforce access and TLS names, and preserve retry/recovery behavior. This
closes the command path's hard-coded loopback limitation; it does not prove
separate-machine deployment. Slice188 wires authenticated command endpoint
discovery through the public protocol. Slice189 adds a replicated metadata
authority executable with explicit initial publication and quorum-backed remote
manifest lookup. Slice190 adds bounded multi-authority CLI recursion; slice191
generates explicit learner/replacement/voter plans and executes them through the
existing authorized membership driver, including interrupted new-store recovery.
Automatic global orchestration is separate and is not a baseline prerequisite.
Slice186a preserves original worker errors and corrects test shutdown clocks;
fresh macOS confirmation remains outstanding.

## R01–R19 requirements

These rows distinguish an implemented mechanism from its acceptance coverage.
Named historical suites establish only their recorded histories. The fresh192
run covers22 actual-core Raft tests,6 quorum tests and4 activation-model tests.
It includes32 seeds of256 actual-core fault actions, not exhaustive scheduling.

| Requirement | Implementation / direct evidence | Remaining acceptance or limitation |
| --- | --- | --- |
| R01 crash-fault consensus | Raft, scoped persistence completions, WAL/recovery; raft, log_store, snapshot and Counter histories. | Selected histories pass; broader combined faults and current platform sweep remain. |
| R02 recursive responsibility | ResponsibilityManifest, durable delegation and recursive resolve; nested native routed histories; executable lookup190. | General recursive lifecycle fault composition remains; no protocol-specific two-level ceiling. |
| R03 multiple ordered lanes under one responsibility | Partitioned manifests and independent groups, source/target split and compatible merge; routed histories. | Operator lifecycle workflow and broader recovery coverage remain. No cross-lane total order is claimed. |
| R04 hierarchical quorums | Policy/JointPolicy, all four election/read/commit sites inspected192; fresh quorum/raft/activation tests pass. | Finite intersection/activation evidence does not establish a distributed protocol proof or all schedules. |
| R05 selective participation | Explicit assigned local groups, public groups/groups_after iteration, selected manifest paths and cached routing; native parent-offline histories, remote lookup190. | Broader deployment/failure coverage; executable list-assigned-groups command remains missing. |
| R06 shared WAL throughput | Shared native log worker/barriers and batching; shared_barrier/runtime tests. | Shared mechanism exists; sustainable throughput improvement and original latency gate remain unmet. |
| R07 cheap idle groups | Shared scheduler/transport/worker, hundred-group histories and static multi-lane benchmark178. | Wider group counts/active fractions and separate idle CPU/allocation measurements remain. |
| R08 no ancestor commit | Direct owner proposals/check_owner, cached child writes with stopped metadata authorities. | Broader outage/migration combinations remain; no root transaction counter is added. |
| R09 safe transitions | Membership journal/readiness, source fencing, import/publication/activation/retirement and nested movement. | Selected native cuts exist; full combined failure matrix and operator workflow remain. |
| R10 distinct outcomes | AdmissionTicket, LogTicket/worker durability, commit/apply effects, ClientCompletion and explicit unknown outcomes. | Maintain downstream conformance and unknown-outcome history coverage as surfaces grow. |
| R11 deterministic core | Event/effect Raft and host-controlled storage/delivery/time;32 seeded schedules pass freshly. | General replayable/minimizable fault driver and reference-Raft comparison remain. |
| R12 bounded overload | Explicit queue/byte/request/image/retention budgets; pressure, maintenance and recovery tests. | Wider disk-full/control/recovery and receive/connection fairness coverage remain. |
| R13 native providers first | Native storage, scheduling, multiplexed TCP/TLS/QUIC and codecs behind public contracts. | No external adapter is required or claimed. |
| R14 public integration seams | Public contracts, NodeParts and native/host tests; catalogue C01–C24 below. | Complete reusable contract-obligation matrix; a listed trait alone is insufficient evidence. |
| R15 embedding / host ownership | Node::from_parts, injected shared providers, downstream ownership/cancellation tests and embedded example. | Wider composition/failure coverage, without creating hidden runtimes or stores. |
| R16 optional build availability | src/lib.rs native gate; Cargo native/tls/quic features; core/default/all-feature lint profiles. | Supported runtime feature profiles require their own evidence; enabling every feature cannot compile feature-disabled branches. |
| R17 domain persistence API | LogStore batches/barriers/ranges/truncation/compaction and snapshot/recovery contracts. | Broader backend/device conformance, not replacement with generic byte I/O. |
| R18 one authoritative log path | Explicit selected store bindings; startup compatibility rejection and host/native assembly tests. | Backend migration is an explicit future procedure, not a second automatic WAL. |
| R19 replacement safety/bounds | Typed incarnation/version/owner/ticket contracts and rejection/lifetime tests. | Complete reusable conformance and supported provider-combination evidence remain. |

## Chapter09 operator surface

The design allows CLI or host integration through typed contracts. Existing
Rust lifecycle operations must not be called unimplemented merely because the
counter executable does not expose them. Conversely, a low-level primitive is
not a complete operator workflow.

| Required operation | Current surface | Gap |
| --- | --- | --- |
| inspect | Counter status/metrics/timings/events; public Node/local/core views. | Broader bounded group/deployment summaries. |
| explain-quorum | Policy/JointPolicy explanations and authenticated paged Counter command177. | No missing basic operation; hints are explicitly not acknowledgements. |
| inspect-route | Public resolve/check_owner and executable directory route190. | Broader operational integration; route hint is not write authority. |
| list-assigned-groups | Public EffectOwner/TimedShard::groups and groups_after over local assignments. | Executable operator command remains missing; reuse these existing cursors rather than adding another inventory abstraction. |
| drain-node | Slice197a adds the local admission/campaign gate;197b1 adds the bounded durable journal and ordered cancellation. Slice197b2a connects authenticated retained-replica drain/start/status/resume/cancel/stop to durable handoff, restored gates and worker shutdown, with TCP/QUIC restart, lost reply and failed-publication evidence. Slice197b2b1 adds journal-bound Rust evacuation plans and exact committed-membership readiness. Slice197b2b2 connects bounded original executable plans to explicit authenticated membership commands with TCP/QUIC joint/final restart, lost replies and source shutdown. Slice197b2c adds a bounded authenticated foreground runner for the single-group sequence and interrupted-runner recovery. Slice197b2d adds explicit maintenance-profile learner enrollment and selected TCP/QUIC replacement-voter drain, absent-learner refusal, interrupted-runner and survivor-restart histories. Slice197b2e documents and verifies explicit final learner removal, uncommitted-retirement recovery and stale-source gating. | Local status is not remote-quorum or decommission evidence. Automatic/full multi-group orchestration and broader replacement/retirement faults remain open. |
| move-leader | Slice196a supplies the core;196b1 adds durable original-ID Rust maintenance, Node controls and selected native TCP/QUIC restart/status histories. | Slice196b2 adds authenticated start/status/resume/cancel in the explicit counter schema2/wire8 profile, including TCP/QUIC recovery and lost/disconnected waits. General multi-group drain and broader profiles remain; terminal records describe historical completion. |
| add-learner | Node configuration/readiness, native administration and placement-plan191. | Broader lifecycle/authorization fault coverage. |
| change-membership | Node configure/status/resume, joint/final records and executable administration. | Broader combined schedules; operator-supplied plans remain explicit. |
| split-preview | Public preview_transfer193 and executable split-preview report bounded scope/placement/payload/pause/retention requirements. Native and host providers plus executable refusal tests pass. | CLI is an offline native template profile, not a live data import test or reservation. Operator lifecycle execution remains separate. |
| begin-split | TransferSource/ScopedTransferSource freeze, TargetImport and TransferPublication contracts;194a public TransferOperation and194b authenticated native split commands join the existing stages. | Broader recursive/retained/merge executable profiles and phase-internal fault coverage. Native whole-responsibility counter split is exercised over TCP/QUIC. |
| resume-operation | Node::resume_configuration plus lifecycle status/query, exact original receipts and194b authenticated split resumption. Slice195 checks every native split phase over TCP/WAL and QUIC/checkpoints plus admitted fence/publication reply loss. | Broader lifecycle profiles, phase-internal power loss and combined membership cuts; membership resumption already exists. |
| retire-group | RetirementGuard/proofs and durable Directory deletion; native retired-owner histories. | General operator workflow and retention policy, not the absence of retirement semantics. |

Slices197b4b2a–197b4b2b3 add authenticated multi-group data, membership and
leadership commands plus complete source-side drain controls. The drain binds
the original mixed voter/retained-learner inventory, persists before receipt,
restores before polling and checks all-assignment readiness before stop.
Selected TCP/WAL and QUIC/checkpoint source recovery, all-group permissions,
bounded manifest input and failed publication are exercised. The foreground
multi-group runner, assignment listing and broader platform/fault acceptance
remain open; these commands do not close those ledger entries.

Mutations must retain durable operation IDs and generation/authorization checks.
Previews must not reserve resources, fence sources or act as durability evidence.
Peer credential rotation, full persisted admin audit, complete core metrics and
rolling-format negotiation/migration evidence also remain open chapter09 work.
Separate-machine execution and current macOS success have not been established.

## Roadmap exits

| Requirement | Current implementation and direct test paths | Remaining acceptance |
| --- | --- | --- |
| P0: reviewed ownership/ordering/cancellation contracts; downstream injection | Public contracts, scoped identities/tickets and explicit NodeParts; tests/{log_store,worker,runtime,effect_owner,lookup_discovery}.rs and tests/support implement public host providers. | Complete the catalogue gaps below; broader actual-core simulation, reusable provider conformance and generated/faulted-history checking remain. An inventory path is not evidence its assertions cover the full contract. |
| P1: durable three-node service; crash/restart preserves acknowledged operations and single-group linearizability | Raft, native WAL/snapshots, owning Node, counter service; tests/{raft,snapshot,counter_service,native_member_startup}.rs. Local reads require quorum plus applied-prefix evidence; retries retain operation/payload identity. | Selected histories cover these paths; Slice164 adds a bounded independent Counter linearizability checker including unknown outcomes and four TCP/QUIC WAL/checkpoint client histories. Broader application/order scopes, generated storage/network fault schedules, complete macOS execution and separate-host validation remain. No release-completion claim. |
| P2: shared groups/lane/transport, bounded fair scheduling and unrelated-group isolation | Shared Shard/TimedShard/Node, workers, multiplexed TCP/TLS and QUIC. tests/runtime.rs hundred-group host/native histories; tests/worker.rs and tests/support/{native_node,native_pressure}.rs. | Slice162 adds eight-group forced repair under a one-request recovery quota with concurrent writes;163 composes shared host/native admission and frame budgets under abort and replacement. Broader active fractions, multi-shard/lane deployment, receive/connection fairness and combined recovery/overload evidence remain. No per-group threads/sockets are introduced by these native assemblies. |
| P3: fixed recursive/weighted policy at every quorum site; intersection/nine-voter/election/read/commit tests | src/quorum.rs; Membership::is_satisfied/frontier; Raft election, read and commitment sites. tests/{quorum,raft,snapshot,activation_model}.rs and src/raft/membership_tests.rs. Nine-voter exhaustive intersection and reference-frontier checks exist. | Finite independent activation model excludes arbitrary log forks/term traces and liveness. Preserve full transition/fault coverage under P4; never infer a runtime proof from intersection alone. Live policy hot reload remains forbidden. |
| P4: learners, joint changes, exact activation through election/rollback/restart/partial delivery | NativeMemberStartup, readiness/witness contracts, configuration journal and authenticated counter commands. tests/{learners,member_recovery,native_member_startup,counter_service}.rs and src/raft/membership_tests.rs. Slice157 tests public joint receipt loss and serving-leader demotion;161 adds newly enrolled store promotion with interrupted readiness and committed-joint WAL/checkpoint recovery over TCP/QUIC. | Slice185 adds source joint-membership/split-fence abort and recovery with an unread original result, durable resumption and final learner state over TCP/QUIC and WAL/checkpoint. Older mismatching checkpoint combinations, broader revocation and overlapping new-voter fault schedules remain. Slice170 repairs selected service retry assumptions and the snapshot-refusal codec mismatch; the full local48-test service target passes. Cancelled CI runs do not establish successful platform execution. |
| P5: versioned manifests, selective placement, cached routing/delegation; established children survive parent outage | Directory/LifecycleDirectory, created groups, retained insertion, deletion, local/foreign reparenting, metadata relocation and locator/grant adoption. tests/{directory,group_creation,namespace_creation,retained_insertion,deletion,reparenting,metadata_transfer}.rs; tests/routed native histories. | Slice160 composes automatic manifest lookup with lifecycle and moved metadata serving envelopes, with selected native move/reopen histories. Slice172 adds bounded authenticated remote endpoint refresh, cancellation, source replacement and retained cached-peer operation. Slice184 adds provisioned authenticated remote manifest fetching through the public Rust seam with TCP/QUIC original-read, disconnect and offline-child checks. Slice189 adds executable initial publication;190 adds cold recursive multi-authority lookup;191 connects explicit placement plans to the authorized membership executor. Broader fault gates and the operator lifecycle surface remain. Automatic global orchestration is not a baseline prerequisite. Native selected deletion/reparenting/migration paths now exist; they must not be listed as absent. |
| P6: staged split/import/fence/publish/activate/merge, retry lineage, exclusive owners and bounded recoverable pauses | ScopeStateMachine and native BucketCounter; transfer source/target/publication/retirement, retained and imported partial owners. tests/transfer_*.rs, tests/{scoped_source,imported_parent,retained_insertion}.rs, tests/routed phase/restart/retirement histories. Metadata migration preserves original authority domains. | Slice185 adds selected same-source joint-membership plus split-fence abort/recovery, exact import configuration lineage and independent target retry/outbox reopen. These are bounded profiles and selected cuts, not every lifecycle composition. Unresolved-creation cancellation is implemented169, with selected native competing-publication owner-loss schedules171. General retention/reclamation policy, broader compatible mapping/provider coverage and other combined failure schedules remain; no automatic timeout unfreeze. |
| P7: multiple local lanes, batching/pipelining, reclamation and recovery throttling; sustainable improvement at fixed correctness/p99 with attributable costs | Explicit lane/store identities, shared barriers, exact-ticket reclamation and offered-load/maintenance/journal harnesses. tests/{shared_barrier,log_reclaim,maintenance}.rs and validation/performance. Slice158 adds forced eight-group native snapshot recovery and one successful original long QUIC pause run. | Slice162 adds explicit concurrent recovery request/image-capacity admission. Slice165 adds opt-in bounded periodic physical reclamation in Node and the service, with native hundred-group and TCP/QUIC restart/retry evidence. Slice166 adds bounded automatic checkpoint progression in the same Node/service composition; native hundred-group and TCP/QUIC reopen/retry histories pass. Bandwidth shaping and latency guarantees are not implemented. Slice178 adds static multi-lane native composition with disjoint stores/groups, bounded host threads and TCP/QUIC recovery/retry evidence. Sustainable load, fuller allocation/RSS/copy/queue attribution and platform/device coverage remain. Automatic live lane placement/migration is an optional enhancement, not substituted for these acceptance gates. Original250ms serial TCP p99 gate remains unmet; latest retained control gate is slice183 (873.492994ms; same host with the older baseline active). Slice183 rejected a Written-stage replication candidate: its first run failed and its repeat had1107.213750ms p99. The source was restored; this is experimental evidence, not an installed feature. Slice158's2918.173043ms offered-load p99 is a different workload, not that gate. |

Historical QUIC pause failures105/106 remain valid failed observations; the
single success158 does not reconstruct their packet histories or guarantee
snapshot necessity from a host-poll pause. The controlled158 tests instead close
all transports and compact both surviving replicas beyond each stale prefix.

## Initial tickets and composition gates

| Design ticket/gate | Evidence and unresolved obligation |
| --- | --- |
| VB-000 contracts/assembly | Public NodeParts and domain contracts with host tests; complete remaining catalogue seams and reusable conformance scope. |
| VB-001 identities/envelopes | Distinct node/store/group/configuration/ownership/lane generations; bounded codecs and identity refusal tests in identity/wire/log/transport suites. |
| VB-002 WAL/recovery | Native framed batches, hard state, barriers, range/suffix/prefix operations, snapshots and corruption/tail tests; general segment cleaning/retention and wider device failures remain. |
| VB-003 deterministic effects | Raft/EffectOwner scoped persistence dependencies; stale/wrong-body/failed completion tests. Broader generated scheduling remains. |
| VB-004 three-node writes/retries | Host actual-core and native service histories;164 adds independent bounded Counter history checking including unknown outcomes. Broader generated/application histories remain. |
| VB-005 shared runtime | Hundred-group shared workers/transports and overload-isolation tests; broader scale and active fractions remain. |
| VB-006 quorum-site audit | Election after durable self-vote and received votes uses Membership::is_satisfied; read readiness uses the same predicate; commitment uses Membership::frontier with local durability/current-term bounds. Joint checks require both policies. No new count-based majority site found in this review. |
| VB-007 recursive example | Separate services/assignments and parent-offline child progress are exercised by tests/routed/native.rs; broader live discovery remains. |
| VB-008 safe split | Native source fence, actual import/publication/activation, lost-reply retry and no-dual-owner checks exist; general lifecycle fault coverage remains. |
| VB-009 native/host lifetime | Native and host providers use public assembly; construction rejection, shared-resource independent shutdown, pending close/abort tests in effect_owner/worker/transport/startup suites. Extend with each new seam. |
| VB-010 packaging/capabilities | src/lib.rs gates native; Cargo features native/tls/quic explicitly compose; startup validates selected providers, identities, wire/limits/durability. Core-only and supported feature builds are separate checks. No third-party adapters advertised. |
| VB-011 reusable conformance | Shared host/native checks exist for storage, snapshots, scheduling and transport; full reusable harness coverage for every promised provider obligation remains. |
| Chapter17 resource/capability gates | Scoped accepted-work ownership and shutdown are documented per subsystem. Missing general shared admission/retention and observer capabilities stay listed below. Backend migration is not a hot pointer swap. |
| Chapter17 measurements | Batching, latency, throughput and selected native I/O timings are retained; comprehensive allocations/copies/RSS/binary-size/dependency attribution remains. No zero-overhead claim. |

## Component catalogue

Each row refers to implemented public types and actual provider/test paths, not
just the proposed design name. docs/component-contracts.json provides detailed
operations/scope and test locations; its checker validates metadata only.

| ID | Public/native implementation | Remaining scope |
| --- | --- | --- |
| C01 log | LogStore/NativeLogStore; narrow VoteStore; log_store/log_reclaim/raft tests | General retention and segment cleaning; broader real backend/device failures. |
| C02 durability | Explicit LogTicket barriers and PersistenceWorker completions bound to one store | No independent assertion provider; keep domain/generation/contiguous-prefix conformance. |
| C03 snapshots | SnapshotStore/Retention/Worker/Router and native files | Wider partial-install, membership and retention combinations. |
| C04 platform I/O | JournalIo/SnapshotIo/VoteIo with native file providers | Complete macOS runtime evidence; no alternate platform provider claimed. |
| C05 application | StateMachine, bounded admission/read/checkpoint/retry contracts; Counter/BucketCounter and lifecycle wrappers | Full declared deployment-envelope enforcement only where provided; alternative applications must supply their actual bounds. |
| C06 transport | PeerTransport/Factory, PeerDriver/Roster/Connector; native TCP/TLS and QUIC | General receive/connection admission/fairness and separate-host evidence. Local send completion is never remote durability. |
| C07 wire | WireCodec/NativeWireCodec, bounded negotiated formats | Fuzzing/compatibility expansion; no gRPC/Protobuf implementation. |
| C08 persistent codec | LogCodec/VoteLogCodec/SnapshotCodec and native codecs | Persistent format migration remains explicit, separate from trait compatibility. |
| C09 secure session | SecureSession and native rustls/QUIC | Slices173–174 add validity guards, prepared replacement and durable executable command-channel reload. Selected key/pin, TCP/QUIC revocation, lost-reply and restart checks exist. Peer credential orchestration, broader failure and platform coverage remain. |
| C10 scheduler | ReadyScheduler, FairScheduler, Shard/TimedShard/EffectOwner/Node | Wider multi-lane deployment/resource isolation. |
| C11 timers | TimerService/DeadlineQueue, generation-scoped expiration | Broader queue/lateness attribution. |
| C12 clock | Clock/MonotonicClock and explicit core MonoTime | No hidden core wall clock; maintain host injection. |
| C13 entropy | ElectionEntropy/JitterEntropy and deterministic source | Election jitter only, not cryptographic identity. |
| C14 buffers | BufferPool/FrameBuffer/BufferClass/BufferOwner; native reservations/control reserve/per-peer quotas | Receive/connection/group fairness and broader payload coverage; WAL/snapshot/application buffers remain separate. |
| C15 admission | AdmissionPolicy/Lease and NativeAdmissionPolicy; hard runtime ceilings | General client, disk, connection and shared-resource admission remains. Policy cannot bypass control reserve or hard bounds. |
| C16 routing | PartitionPolicy, checked manifests/resolve/check_owner, native byte partition | Broader mappings/automatic split policy; no hidden global ordering. |
| C17 discovery | PeerDiscovery/DiscoveryConnector, ManifestDiscovery/ManifestReadSource, NativeManifestLookup/AuthorityDiscovery | Slice160 adds public read mappings and automatic lookup across selected native metadata moves/restarts. Slice172 adds native authenticated endpoint fetch/retry/cancel/reconnect through PeerDiscovery, with TCP/TLS and QUIC evidence. Slice184 adds provisioned authenticated remote manifest fetch over TCP/TLS and QUIC, with original read drain and offline-child evidence. Slices188–189 add executable command endpoint discovery and initial metadata authority lookup;190 adds recursive multi-authority command-line resolution with bounded cache/hops/attempts. Broader discovery faults and persistent refresh integration remain. Hints never activate owners. |
| C18 placement | PlacementAuthorizer and PlacementPlanner/plan_learner; native bounded deterministic ranking; slice180 replacement preparation and explicit-policy voter-change plans;191 offline plan generation and exact-store executable administration | Automatic sample collection/reservations and global rebalancing are not implemented; broader fault coverage remains. Recommendations never change membership alone. |
| C19 observability | Observer/NativeCounterObserver, EventObserver/NativeEventObserver, TimingObserver/NativeTimingObserver and optional JournalTimings | Bounded aggregate history, explicit overflow and scoped export are implemented168. Slice176 adds bounded poll/connection duration distributions, including interrupted connections and restart reset. Per-group tracing, queue/critical-path decomposition and external exporter integration remain. |
| C20 configuration | Typed startup and Node configure/status/resume; authenticated provisioned/client-target service commands | Selected new-store interruption/recovery161 is exercised; broader ingress, revocation and combined failures remain. Native configuration endpoints exist and must not be listed wholly missing. |
| C21 authorization | PrincipalCredentials/ServiceAuthorizer/authorize_session, NativeServiceAccess, CredentialJournal | Executable live command-policy reload records local preparation before publication, revokes existing channels and validates restart files. External issuer, full durable audit history, peer rotation and broader revocation schedules remain. Unflagged loopback mode is explicitly trusted. |
| C22 integrity/compression | Named native checksum/digest framing internally | A separate selectable integrity provider is not exposed; optional bounded compression is unimplemented. Chapter17 permits related providers to share an implementation: neither a new trait per checksum helper nor optional compression is automatically a baseline blocker. |
| C23 scope transfer | ScopeStateMachine/ScopeImage, native BucketCounter, source/target/lifecycle guards | Wider application/provider and recursive fault/retention coverage; core retains fence/publication/activation authority. |
| C24 transactions | One-group ordered application commands | Cross-group transactions are unsupported P8, not a hidden baseline coordinator. |

## Invariants and scenario limits

| Chapter11 invariant | Direct evidence families | Unclosed scope |
| --- | --- | --- |
| I01/I02/I03/I04: vote uniqueness, committed history, contiguous durability, effect dependencies | raft/ballots/replication_scope/log_store/shared_barrier/worker/effect_owner | General generated message/storage schedules, external devices and faulted-history checking. |
| I05: validated effective policy | quorum/activation_model/member_recovery plus actual Raft sites above | Full policy-transition protocol model and arbitrary log/term cases. |
| I06: exclusive transferred owner | transfer_source/target/publication, scoped_source, metadata_transfer, native routed phase histories | General recursive lifecycle composition and external provider assumptions. |
| I07/I08/I09: acknowledged recovery, deduplication, valid snapshots | application/raft/snapshot/member_recovery/counter_service/routed | Broader power-loss/device/partial-install combinations and generated histories. A bounded independent Counter unknown-outcome checker exists164. |
| I10: bounded resources | buffer/admission/runtime/worker/transport and configuration_capacity | General disk/client/connection pressure, recovery-throttle fairness. |
| I11: stale incarnation refusal | identity/wire/log/peers/member_recovery and owner-completion tests | Broader stale-disk/packet/reconnection schedules. |
| I12: independent child authority | native routed parent-offline and unchanged stopped-file tests | Broader recursive outage/membership/migration combinations. |

Scenarios1/2/6 have selected persistence/election/membership histories;
4/5/9/10 have selected parent-outage, phase-cut, stale identity and retry histories.
Scenario7 has per-group timer/idle tests, not a complete dormancy protocol.
Scenario8 has reclamation, buffers/control reserve and snapshot experiments, not
complete disk-full/recovery competition coverage. Scenario3's striped-group
protocol is P8; its ordinary stale-completion obligations remain P0–P3.
tests/raft.rs also executes32 seeds of256 actual-core actions with native WAL
model power loss, partitions, reordered/duplicated messages and committed-prefix
checks. This is a bounded majority schedule family. It does not establish a
general schedule generator/minimizer, known-Raft differential runner or
general end-to-end linearizability checking across all applications. Slice164 now independently checks bounded Counter client histories including unknown outcomes and selected real service crashes/restarts. Those
chapter11 validation obligations remain explicit rather than being replaced by
a passing test count.

## Next implementation decisions

1. Slices193–194 implement read-only split preview, public restartable decisions
   and authenticated native split start/status/resumption. Slice195 checks native
   phase-boundary restarts and lost fence/publication replies using those contracts.
   Slice196a supplies the targeted handoff primitive;196b1 adds durable Rust
   operation tracking;196b2 adds authenticated counter-profile operator execution.
   Broader lifecycle profiles remain open; do not create another ownership engine.
   Coordinated node drain and the assignment-list command
   remain required operator work; public assignment iteration exists.
2. Retain P7's original fixed250ms gate. The saved183 control is873.492994ms
   and the rejected candidate repeat1107.213750ms. Both fail. Do not relabel a
   different workload, a finite successful recovery, or added static lanes as
   a sustainable throughput improvement.
3. Broaden chapter11 evidence: replayable/minimizable actual-core fault schedules,
   compatible-semantics reference-Raft comparison, fuzz coverage for WAL/RPC/
   snapshot/routing codecs, declared-scope partitioned histories, and the missing
   combined membership/lifecycle/storage schedules. Existing deterministic and
   malformed-input tests remain useful but do not close this whole requirement.
4. Observe baseline175 and current platform jobs under their original identities.
   At192 capture the old routed process is live, and CI38025706754 for639c1fb
   is pending with no jobs yet. Neither is a passing current-source baseline.
   Continue local feature work while these run.

P8 and Windows are deferred. Automatic global split/merge orchestration,
busy-polling, custom allocators and particular external adapters are explicitly
not prerequisites in the design. That does not defer the baseline behavior or
validation obligations identified above.
