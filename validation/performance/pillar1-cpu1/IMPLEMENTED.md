# CPU1 implementation receipts

Status: bounded candidate implemented and checked for parent review.
Performance, independent source review and full qualification remain PENDING.
Branch: cpu/p1-commit-notices. Base: 0a752089; production baseline:
0bf9271ecb5a34c4089c6bb10381303232624170. Performance gate PENDING.

Resource limitation: the first two read-only shell launches did not use the full
stock guard/envelope; no builds/tests ran then. Subsequent controllers explicitly
set absolute nice19/affinity0–3. Own automatic tool hosts were identified by exact
cwd/exe/start ticks (2165160:4667880, 2165194:4667881, 2165344:4667884)
and every existing thread constrained. No retrospective all-task claim.
Stock guard passed at initial baseline: root1GiB, worktree0GiB, live2 including
one foreign worktree; Mac10GiB. Original design pack read from main's untracked
VoteBoat-Design-Pack; it is absent from this worktree and was not copied/edited.

## Preserved first expanded-run failure

Default focused raft run:193 passed /1 failed, rc101. New fallback fixture failed
at membership_tests.rs166 before RPC assertions: invalid configuration journal.
Source tree800df77284423415d55c500dac1bc9b5f8288851 and full binary patch,
manifest, original log/resource receipts retained in raw report directory.
No runtime state dump was emitted; exact fixture source and error are preserved.
Dependent launches stopped. Parent explicitly released bounded new-fixture repair
before any sweep. Cause: Final incorrectly used prior Learners operation100;
the validator requires the active Joint operation101. Existing valid Final
builders use101. Parent briefly suggested fresh102, then corrected it before
any edit adopted that suggestion. Production validation is unchanged.
Both accepted Final and completed retirement coverage are retained.

The first repair launch used an unqualified --exact filter and ran0 cases;
not counted as a pass. Correct exact filter then ran1 and exposed another
new-fixture misuse: retired follower passed to leader-only broadcast helper
(debug role assertion). Corrected retirement to public Heartbeat (empty output)
and kept leader helper assertions for learner/joint/accepted Final states.
No production behavior or validator was changed for either fixture correction.

Expanded new consumer control initially14/1 rc101: native owning Shard refused
a volatile fixture (WrongOwner at native.rs24). This is expected assembly
validation; corrected new native fixture to the durable core seam supported by
Shard. Production owner validation unchanged. Earlier14 successful cases retained.


## Implemented result and scope

Explicit Rpc::CommitNotice and public startup enable_commit_notices select an
equal-guarantee physical composition. Only installed current-term tip commitment
and exact durable data ACK progress permit commit-purpose emission. One exact
scalar stamp per peer records real output; request counters cannot authorize it.
Stamps never become requests/aliases, quorum/progress/readiness/read evidence or
notice ACK drain obligations. Actual retained Vec capacity is checked in
reservation; 1000 steady cycles retain exactly two peer keys without growth.
Role/term/configuration/fence/compaction/snapshot boundaries invalidate stamps.

Strict receive identity/current configuration/origin/term validation precedes
state changes. A matching local boundary alone can advance commit; existing
completion gates still precede application. Valid nonmatching contact resets
elections, invalid/old traffic does not. A higher-term follower sends no notice
reply; the next acknowledged heartbeat discovers its term. Ordinary Append,
heartbeat, probe, catch-up, retry, transfer, learner/joint/Final/retirement and
authorization-bridge paths retain acknowledgements.

Static summary refinement is confirmed: ordinary checkpoints keep membership
None and summary plain. A 5000-original checkpoint/compaction control resumes
the six-message cycle; a positive compacted boundary is contact without commit.
Matched default-retention maintenance completes12000 originals with all three
applied frontiers12005 and at least two checkpoints per traced replica, retaining
fewer than5000 entries. Retain-all was not substituted for this primary control.

Wire9/tag19 is explicit codec/HELLO consumer integration only; older codecs
refuse the RPC. Native defaults, ALPN, network policies and storage formats
are unchanged. Logical authorization explicitly refuses even a valid wire9
notice against a committed parent image; logical assemblies stay capability-off.
Matched's only source change is public capability enabling during construction.

## Actual checks (22–23 October; sequential configurations)

| Check | Actual result |
| --- | --- |
| Final core default / all-features |202 /202 passed, rc0 each |
| Public Raft + volatile + wire, default / all-features |83+11+21 passed per configuration, rc0 |
| Logical application, default / all-features |66 passed, one existing ignored timing report each, rc0 |
| Native relay notice controls | genuine one-way frame, Sent/Failed/Cancelled ownership/drain, bounded control reserve/refusal, label/family refusal passed in default/all-features |
| Matched locked offline workspace, default / workspace all-features |75 /75 passed, rc0; no optional matched features, contracts-only VoteBoat dependency |
| Pinned off/on cycle, volatile and durable |messages8→6, events9→7, effects14→12, persist6→6; identical logs/commits/applied originals |
| Causal mutations |4/4 killed by executed assertions (each rc101/one failed case): omit boundary term, omit contact reset, infer stamp from counter, route heartbeat through notices; source restored and controls green |
| Strict root formatting + four root Clippy configs + matched formatting/Clippy |seven rc0, zero diagnostics; exact commands in raw logs |

Counts overlap; they are not a whole-repository suite total. The final core
202-case runs include18 CPU1 core/fallback cases; companion relay-kind and
logical-authorization cases passed separately (20 CPU1 cases across those
namespaces). Other consumer cases are in their stated harnesses.
The final extra core case explicitly checks delayed higher-term discovery.
The enabled actual pre-push hook repeats all seven strict checks at publication;
its command/exit/resource receipt and remote head are kept with raw reports.

Other normal development failures remain in raw logs: initial new-test module/
Counter API compile errors; a new bridge fixture borrow error; first Clippy
lookup/borrowed-slice diagnostics; and a test-only vec! replacement parse error
caught by formatting before dependent launches. Each had a focused correction.
No original failure was hidden or cleared by an unchanged retry. No lint level,
threshold, production journal validation or pre-push check was weakened.

## Consumer audit

| Consumers | Result |
| --- | --- |
| core receive/source, progress broadcast, admitted ranges, requests, send windows | typed purpose and exact stamps; ordinary requests/replies unchanged; new input is sender-originated |
| leadership, readiness, configuration, authority, joint/batched/snapshot repair and connections | existing proof gates/rosters retained; new enabled-path fallback controls plus existing core suite |
| outbound and runtime effects/ingress/connections/ready batch | scalar Control cost/reserve; normal leased effects and serialized ingress; no new batching, membership import or queue owner |
| wire encoder/counting/decoder/capacity and TLS/startup selection | explicit9/tag19; shared checked budgeting and exact-version refusal; default selections unchanged |
| native relay frame/forwarder ledger/provider ingress | exact OneWay output; Request/Reply relabel refused; no ReplyScope/RequestRecord or return collector for notice |
| logical authorization/release/bridge/node | explicit refusal; assemblies capability-off; generic existing capture, child durability and reply windows unchanged |
| matched construction/actor/client/maintenance | construction-only enable; generic send/persist/apply and all original accounting/shutdown controls retained |

No CPU gain, allocation reduction, power-loss proof, full source approval or
complete release is claimed. No long matrix, full suite, exhaustive relay,
native performance, Mac work or unrelated reliability repair ran; the builder
spawned no child agents.
Known historical native relay BrokenPipe, benchmark filesystem lock and QUIC
drain failures remain unproven. Parent owns source reviews, equal-core primary
default-retention CPU measurements, keep/reject and integration. Macro P1 and
the next CPU2/CPU3 dependency plan remain open.

## Resource/evidence boundary

All recorded Cargo controllers used direct Python setpriority(...,19) and
sched_setaffinity(0,{0,1,2,3}) immediately before exec, max4 Cargo jobs,
incremental0, debug symbols0 and unoptimized normal-debug assertions. No extra
CARGO_HOME, source copies, per-revision targets, detached job or browser ran.
Own target stayed below15GiB; final pre-publication measured size is in raw
resource summary. Stock guards used unchanged global caps before configurations
and after work. Sampled descendant task records report zero observed violations;
this does not erase the early read/tool-host limitation above or establish
unsampled all-task compliance.

Raw safe logs, command environments, every recorded rc, mutation patches,
resource samples, preserved first-failure full source patch/manifest/tree,
artifact hashes and publication/cleanup receipts live under main's
target/claude/pillar1-20261022/cpu1-builder (reports only). Own target, checkout,
registration, seed branch and exact identified automatic hosts are swept after
checked publication; final stock guard/zero-owned-process receipts are recorded
there by cleanup, not inferred from test success.


## Semantic preservation follow-up plan (before test edits)

The feature gate preserves VoteBoat capabilities above the OpenRaft comparison
baseline. Cargo all-features compilation alone is not semantic qualification.
Add one bounded off/on volatile/durable control using initially validated weighted
and recursive bootstrap policies: an ordinary flat majority must not commit when
the actual policy is unsatisfied, and real data ACKs must still converge through
the existing policy. Test fixture policy is selected before recovery, never hot
swapped. No production API/state/ownership change is planned. Existing logical
application and release controls retain child evidence; select the missing
release-focused controls without launching exhaustive native/network suites.
Record source consumers, actual evidence and remaining parent qualification in
a feature matrix. No new production feature or roadmap completion is implied.

Chapter06 responsibility/execution-group decomposition is an architectural
boundary: fleet→ship→department can mean independently owned per-department
logs, each with its own ordered prefix and no global order. CPU1 remains local
to one explicit group/incarnation/configuration/context. Parent metadata/routing
must not become a per-write sequencer, commit/global-lock dependency or hot path.
Existing responsibility ownership/migration/lifecycle fences remain in force;
CPU1 adds no splitting, department or routing implementation. Existing fallback
and identity/fence controls suffice for this scope; no invented decomposition
test is planned.


## Semantic feature matrix

This is a preservation audit of implemented consumers, not a claim that the
whole design-pack roadmap is complete. OpenRaft is only the comparison baseline.
Excluded notice states keep their functional acknowledged path. All-feature
compilation is listed separately from actual executed semantic controls.

| Capability | Source consumer | CPU1 path | Actual regression evidence / boundary |
| --- | --- | --- | --- |
| Recursive and weighted quorum trees; joint old-and-new policy | quorum.rs; raft.rs::maybe_commit_into; raft/configuration.rs | Notice only after the unchanged durable policy commits; joint uses acknowledged fallback | New enabled_notices_preserve_weighted_and_recursive_quorum_commit_rules: off/on × volatile/durable × two policies; flat majority refused, exact ACKs converge, six persists, identical state. Existing core202 includes joint_commit_uses_old_and_new_and_never_counts_a_learner and recursive_joint_recovery controls, default/all-features |
| Logical-voter composition and child durable evidence | logical_voter/application/authorize.rs; bridge/release.rs; runtime/logical_release; logical_voter/persistence | Logical assemblies remain supported through existing acknowledged composition; explicit notice authorization refusal, capability off | Logical application66 per configuration includes original retry, term/vote/prefix checkpoint, stale read capsule and joint→Final import controls; new wire9 notice refusal. Additional release capacity/classification11 and exact written→durable correlation2 per configuration. Correlation controls do not independently prove child consensus or whole native lifecycle |
| Hierarchical responsibility, routing, independently owned/migratable execution groups | routing.rs; routed.rs; reparenting.rs; transfer.rs; scope.rs | CPU1 stays within one group/context; lifecycle/configuration/fence transitions invalidate stamps or use fallback | Strict source group/incarnation/configuration/store/context controls and role/config/fence/snapshot invalidation executed. Existing hierarchical routing/ownership sources unchanged; all-target strict builds pass. Full routed migration/lifecycle suites not rerun in this slice |
| Chapter06 fleet→ship→department decomposition | Original design chapter06; same responsibility/routing consumers | One ordered prefix per concrete group; no global order or parent per-write sequence/commit/global lock dependency added | Architectural source boundary above plus existing identity/fence/fallback controls. No splitting/department/routing implementation or new feature completion claimed |
| Membership, learners, retirement and leadership transfer | raft/configuration.rs; raft/leadership.rs; raft/progress_broadcast.rs | Notice excluded; normal acknowledged requests and original lifecycle effects continue | New configured learner/joint/accepted Final/completed retirement assertions; exact bridge fallback; transfer stamp invalidation; existing learner/joint/read/election/retirement/transfer core controls, default/all-features |
| Reads and authority/readiness | raft.rs::begin_read/maybe_read_ready; raft/source.rs; logical_voter/application | Notices never create read, quorum, readiness or reply evidence | Fake Appended/read contexts cannot upgrade progress/read proof; invalid/contact split; source preflight learner read refusal and same-voter joint election/read controls; logical stale read capsule control |
| Operation identities, deduplication, client outcomes | raft/proposals; application.rs; logical_voter/application; matched actor/client | Original append/application/outcome path retained | Fixed off/on identical committed/applied originals, matched75 original receipt/failure/shutdown controls, logical exact-retry/replacement/original-outcome controls. No client/oneshot/reply/accounting edits |
| Snapshots, recovery and persistence completion | raft.rs::checkpoint_stored/snapshot_applied; raft/receive.rs; log_store; public recovery APIs | Static checkpoint remains eligible after new data ACKs; configured snapshots use fallback; restart explicitly selects capability again | 5000-original compaction→six-message cycle; matched default-retention12000 originals and lagging-follower snapshot recovery; public durable/volatile/no-early-apply/failure controls; stale/higher-term completion; no storage-format change |
| Multi-group identity, bounded resources, public host/native composition | runtime; native/relay; outbound; wire; public Raft startup | Exact per-group provenance; bounded peer stamps; explicit wire9/tag19; generic host ownership preserved | Public83+volatile11+wire21 per configuration; real one-way capture/frame/local Sent/Failed/Cancelled drain and reserve/refusal controls;1000 cycles bounded two peer keys; strict all-target builds across four configurations. Whole multi-group/native qualification remains parent-owned |

The semantic follow-up changes only the test fixture/control and documentation.
Production/runtime, matched source, Cargo/locks, build scripts/configuration and
tracked hooks are identical to the first checked published eb32cbef head. That
allows the parent to identify its pinned source/performance results precisely;
performance approval is still PENDING. Supplemental checks are reported as
separate controls, not added to the earlier core202 run count.


Supplemental receipts: semantic-quorum-default and semantic-quorum-all-features
ran1 each, rc0; semantic-logical-release-{default,all-features} ran11 each,
rc0; semantic-logical-persistence-{default,all-features} ran2 each, rc0.
No production weakening, dependency or additional roadmap code was needed.

The final notice-module regression after the fixture refactor ran18 cases in
both default and all-feature configurations, rc0 each (semantic-final-notice-*).
All seven strict follow-up checks also passed, rc0/zero diagnostics
(semantic-strict-*). The actual enabled hook is repeated on the follow-up push;
a temporary own-target Cargo proxy runs the unchanged stock guard before each
hook configuration and execs the original /usr/bin/cargo under absolute nice19
and affinity0–3. The proxy is removed with the own target during cleanup.
