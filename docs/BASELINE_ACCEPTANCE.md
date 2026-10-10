# Baseline acceptance map — review159, updated172

Reviewed starting revision1231153 against design-pack chapters11,12,17 and the
component contract specification. This is a current requirement ledger, **not a
completion certificate**. Selected tests do not prove all schedules. Detailed
historical results remain in IMPLEMENTATION.md, BASELINE_AUDIT.md and
validation/REPORT.md; validation/baseline/slice159-160 retains fresh results and
any failures. The old full all-feature sweeps have incomplete logs and their
process handles are absent; they do not establish full completion. Slices162–167
add recovery-budget, provider-lifetime, Counter client-history, automatic physical
reclamation, automatic checkpoints and maintenance under held recovery pressure.
Slice167 composes both maintenance policies with actual eight-group snapshot
recovery over TCP/TLS and QUIC. Selected histories require foreground apply,
durable checkpoint and later physical reclaim while a recovery receipt remains
held; then all stale groups recover and original retries survive another reopen.
This covers the selected composition, not general fairness or a latency bound.
Slice168 adds bounded post-poll operational history with host/native injection,
explicit overflow/cursor gaps and scoped service export. No observer becomes
consensus authority; latency and critical-path attribution remain unimplemented.
Slices169–171 add unresolved-creation cancellation, repair a native snapshot
refusal/codec mismatch and exercise16 native cancellation/publication race
schedules across owner loss, unread receipts and WAL/checkpoint recovery. The
competing decision is rejected after recovery; a canceled target stays
non-serving, while a published target preserves data retries with metadata
stopped. These selected schedules do not close general lifecycle fault coverage.
The full P0–P7 objective stays active. RPL-1.5; Linux and macOS targets.

## Roadmap exits

| Requirement | Current implementation and direct test paths | Remaining acceptance |
| --- | --- | --- |
| P0: reviewed ownership/ordering/cancellation contracts; downstream injection | Public contracts, scoped identities/tickets and explicit NodeParts; tests/{log_store,worker,runtime,effect_owner,lookup_discovery}.rs and tests/support implement public host providers. | Complete the catalogue gaps below; broader actual-core simulation, reusable provider conformance and generated/faulted-history checking remain. An inventory path is not evidence its assertions cover the full contract. |
| P1: durable three-node service; crash/restart preserves acknowledged operations and single-group linearizability | Raft, native WAL/snapshots, owning Node, counter service; tests/{raft,snapshot,counter_service,native_member_startup}.rs. Local reads require quorum plus applied-prefix evidence; retries retain operation/payload identity. | Selected histories cover these paths; Slice164 adds a bounded independent Counter linearizability checker including unknown outcomes and four TCP/QUIC WAL/checkpoint client histories. Broader application/order scopes, generated storage/network fault schedules, complete macOS execution and separate-host validation remain. No release-completion claim. |
| P2: shared groups/lane/transport, bounded fair scheduling and unrelated-group isolation | Shared Shard/TimedShard/Node, workers, multiplexed TCP/TLS and QUIC. tests/runtime.rs hundred-group host/native histories; tests/worker.rs and tests/support/{native_node,native_pressure}.rs. | Slice162 adds eight-group forced repair under a one-request recovery quota with concurrent writes;163 composes shared host/native admission and frame budgets under abort and replacement. Broader active fractions, multi-shard/lane deployment, receive/connection fairness and combined recovery/overload evidence remain. No per-group threads/sockets are introduced by these native assemblies. |
| P3: fixed recursive/weighted policy at every quorum site; intersection/nine-voter/election/read/commit tests | src/quorum.rs; Membership::is_satisfied/frontier; Raft election, read and commitment sites. tests/{quorum,raft,snapshot,activation_model}.rs and src/raft/membership_tests.rs. Nine-voter exhaustive intersection and reference-frontier checks exist. | Finite independent activation model excludes arbitrary log forks/term traces and liveness. Preserve full transition/fault coverage under P4; never infer a runtime proof from intersection alone. Live policy hot reload remains forbidden. |
| P4: learners, joint changes, exact activation through election/rollback/restart/partial delivery | NativeMemberStartup, readiness/witness contracts, configuration journal and authenticated counter commands. tests/{learners,member_recovery,native_member_startup,counter_service}.rs and src/raft/membership_tests.rs. Slice157 tests public joint receipt loss and serving-leader demotion;161 adds newly enrolled store promotion with interrupted readiness and committed-joint WAL/checkpoint recovery over TCP/QUIC. | Older mismatching checkpoint combinations, broader revocation and overlapping new-voter fault schedules remain. Slice170 repairs selected service retry assumptions and the snapshot-refusal codec mismatch; the full local48-test service target passes. Cancelled CI runs do not establish successful platform execution. |
| P5: versioned manifests, selective placement, cached routing/delegation; established children survive parent outage | Directory/LifecycleDirectory, created groups, retained insertion, deletion, local/foreign reparenting, metadata relocation and locator/grant adoption. tests/{directory,group_creation,namespace_creation,retained_insertion,deletion,reparenting,metadata_transfer}.rs; tests/routed native histories. | Slice160 composes automatic manifest lookup with lifecycle and moved metadata serving envelopes, with selected native move/reopen histories. Slice172 adds bounded authenticated remote endpoint refresh, cancellation, source replacement and retained cached-peer operation. External authority fetching, executable endpoint wiring and broader placement remain. Native selected deletion/reparenting/migration paths now exist; they must not be listed as absent. |
| P6: staged split/import/fence/publish/activate/merge, retry lineage, exclusive owners and bounded recoverable pauses | ScopeStateMachine and native BucketCounter; transfer source/target/publication/retirement, retained and imported partial owners. tests/transfer_*.rs, tests/{scoped_source,imported_parent,retained_insertion}.rs, tests/routed phase/restart/retirement histories. Metadata migration preserves original authority domains. | These are bounded profiles and selected cuts, not every lifecycle composition. Unresolved-creation cancellation is implemented169, with selected native competing-publication owner-loss schedules171. General retention/reclamation policy, broader compatible mapping/provider coverage and other combined failure schedules remain; no automatic timeout unfreeze. |
| P7: multiple local lanes, batching/pipelining, reclamation and recovery throttling; sustainable improvement at fixed correctness/p99 with attributable costs | Explicit lane/store identities, shared barriers, exact-ticket reclamation and offered-load/maintenance/journal harnesses. tests/{shared_barrier,log_reclaim,maintenance}.rs and validation/performance. Slice158 adds forced eight-group native snapshot recovery and one successful original long QUIC pause run. | Slice162 adds explicit concurrent recovery request/image-capacity admission. Slice165 adds opt-in bounded periodic physical reclamation in Node and the service, with native hundred-group and TCP/QUIC restart/retry evidence. Slice166 adds bounded automatic checkpoint progression in the same Node/service composition; native hundred-group and TCP/QUIC reopen/retry histories pass. Bandwidth shaping and latency guarantees are not implemented. Operational multi-lane placement, sustainable load, fuller allocation/RSS/copy/queue attribution and platform/device coverage remain. Original250ms serial TCP p99 gate remains unmet; latest retained gate result is slice138 (734.953838ms). Slice158's2918.173043ms offered-load p99 is a different workload, not that gate. |

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
| VB-004 three-node writes/retries | Host actual-core and native service histories; general unknown-outcome linearizability checking remains. |
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
| C09 secure session | SecureSession and native rustls/QUIC | Live credential renewal/revocation integration and broader failure coverage. |
| C10 scheduler | ReadyScheduler, FairScheduler, Shard/TimedShard/EffectOwner/Node | Wider multi-lane deployment/resource isolation. |
| C11 timers | TimerService/DeadlineQueue, generation-scoped expiration | Broader queue/lateness attribution. |
| C12 clock | Clock/MonotonicClock and explicit core MonoTime | No hidden core wall clock; maintain host injection. |
| C13 entropy | ElectionEntropy/JitterEntropy and deterministic source | Election jitter only, not cryptographic identity. |
| C14 buffers | BufferPool/FrameBuffer/BufferClass/BufferOwner; native reservations/control reserve/per-peer quotas | Receive/connection/group fairness and broader payload coverage; WAL/snapshot/application buffers remain separate. |
| C15 admission | AdmissionPolicy/Lease and NativeAdmissionPolicy; hard runtime ceilings | General client, disk, connection and shared-resource admission remains. Policy cannot bypass control reserve or hard bounds. |
| C16 routing | PartitionPolicy, checked manifests/resolve/check_owner, native byte partition | Broader mappings/automatic split policy; no hidden global ordering. |
| C17 discovery | PeerDiscovery/DiscoveryConnector, ManifestDiscovery/ManifestReadSource, NativeManifestLookup/AuthorityDiscovery | Slice160 adds public read mappings and automatic lookup across selected native metadata moves/restarts. Slice172 adds native authenticated endpoint fetch/retry/cancel/reconnect through PeerDiscovery, with TCP/TLS and QUIC evidence. External manifest protocols and executable integration remain. Hints never activate owners. |
| C18 placement | PlacementAuthorizer and PlacementPlanner/plan_learner; native bounded deterministic ranking | General voter replacement/removal, measured sample collection/reservations and global rebalancing. Recommendations never change membership alone. |
| C19 observability | Observer/NativeCounterObserver, EventObserver/NativeEventObserver and optional JournalTimings | Bounded aggregate history, explicit overflow and scoped export are implemented168. Per-group tracing, latency/queue/critical-path attribution and external exporter integration remain. |
| C20 configuration | Typed startup and Node configure/status/resume; authenticated provisioned/client-target service commands | Selected new-store interruption/recovery161 is exercised; broader ingress, revocation and combined failures remain. Native configuration endpoints exist and must not be listed wholly missing. |
| C21 authorization | PrincipalCredentials/ServiceAuthorizer/authorize_session, NativeServiceAccess | Live rotation/external issuer/durable audit and broader revocation schedules. Unflagged loopback mode is explicitly trusted. |
| C22 integrity/compression | Named native checksum/digest framing internally | A separate selectable integrity provider is not exposed; optional bounded compression is unimplemented. Preserve this distinction. |
| C23 scope transfer | ScopeStateMachine/ScopeImage, native BucketCounter, source/target/lifecycle guards | Wider application/provider and recursive fault/retention coverage; core retains fence/publication/activation authority. |
| C24 transactions | One-group ordered application commands | Cross-group transactions are unsupported P8, not a hidden baseline coordinator. |

## Invariants and scenario limits

| Chapter11 invariant | Direct evidence families | Unclosed scope |
| --- | --- | --- |
| I01/I02/I03/I04: vote uniqueness, committed history, contiguous durability, effect dependencies | raft/ballots/replication_scope/log_store/shared_barrier/worker/effect_owner | General generated message/storage schedules, external devices and faulted-history checking. |
| I05: validated effective policy | quorum/activation_model/member_recovery plus actual Raft sites above | Full policy-transition protocol model and arbitrary log/term cases. |
| I06: exclusive transferred owner | transfer_source/target/publication, scoped_source, metadata_transfer, native routed phase histories | General recursive lifecycle composition and external provider assumptions. |
| I07/I08/I09: acknowledged recovery, deduplication, valid snapshots | application/raft/snapshot/member_recovery/counter_service/routed | Broader power-loss/device/partial-install combinations, unknown-outcome checker. |
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

1. Slice170 closes the observed Linux service failure paths;171 records selected
   creation decision races. The earlier macOS shutdown failure remains unverified.
   CI stays background feedback; a cancelled run is not a passing baseline.
2. Slice160 implements automatic lookup across actual metadata source/serving
   query envelopes, with native move/reopen, inactive/fenced refusal and original
   read ownership checks. Slice172 adds remote endpoint fetch and selected refresh/source-failure checks. External manifest fetch, executable integration and broader discovery faults remain.
3. Slice161 exercises public new-store interruption, exact operation retry and
   recovery through joint consensus. Slices162–163 close the selected recovery preparation budget and combined shared-provider lifetime gaps. Slice164 adds bounded faulted Counter history verification. Next review the remaining concrete baseline requirements and implement the next unresolved capability; keep broader P0–P7 faults and acceptance work active.

P8 and Windows are deferred. Automatic global split/merge orchestration,
busy-polling, custom allocators and particular external adapters are explicitly
not prerequisites in the design. That does not defer the baseline behavior or
validation obligations identified above.
