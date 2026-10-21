# PR23 independent-main source preparation — qualification pending

Pre-edit [plan](PLAN.md); existing PR https://github.com/KarlLivesey/VoteBoat/pull/23.
Original N1c44 + qualified mainb77 by real merge; no rejected ST2/DR1 production.
Desired source audit: [source-audit.json](source-audit.json), full protected-path
blob/SHA256 inventory in source-manifest.tsv.gz. Of 2621 protected paths,
2591 equal main,20 equal original N1c,4 are real three-way composition with
exact original executable added/deleted lines,1 differs only in hold comments,
and5 are exact reviewed fixture blobs from66fc. Four compositions are raft.rs,
admitted_replication.rs,progress_broadcast.rs,effect_owner.rs. No other input
changed; Counter and live8B benchmark metadata equal main. No ST2 maintenance,
stall/noop admission propagation,DR1 retention/readiness/session16B code enters.
Hold module/timed runtime/core tests/runtime tests executable identity equals44.

The five test-only exceptions are ports.rs,fixture_isolation.rs,transfer_service.rs
and its private root.rs/root_tests.rs. Pre-existing files match558 preimages;
all five resulting blobs match66fc exactly. The reviewed checked-layout fix is
needed for the known65200+487 overflow; independent directory identity prevents
reuse/delayed-cleanup/stale-store aliasing. No raft comment fromDR1 is imported.
Main/parent PR14+PR15 rejection/closed/parked and PR24 kept/merged records remain.
Original N1c and supersededb959 reports/performance remain labelled historical.

Confirmed nonzero prefixes, data>=batch_floor/notices>=last, exact ack/current-
term/frontier commitment, one request/follower, peer-bounded holds and all
learner/catchup/probe/config/transfer/compaction/reject/heartbeat paths remain.
In-flight extensions are scheduling only. Correlated rejection releases spares;
stepdown/config/close clear holds. Two-voter commit notices may be held; data
requires quorum coverage without the peer. Byte-limit splits may rehold a spare
mid-batch. Frozen fixtures explicitly release, facade heartbeat only follows
owner drain; runtime tests separately cover deadlines/per-group/late poll.
Cancel is appended before transfer starts. No new hold policy/N1/N1d/d2/unsafe.

Validation and publication results will be appended after focused execution.
An initial fmt invocation reached no Cargo: runner used a nonexistent cargo
path (rc127); corrected to observed /usr/bin/cargo. Both stock guards passed,
observed priority/affinity compliant, no source or product check ran in that
invocation. This launch error is retained in raw reports.

Parent still owns two fresh pinned Sol reviews, full five-configuration gate
and fair exact-baseline/head performance. No full green/performance clearance,
Done,keep/merge or closed candidate dependency is claimed. No full matrix,
relay/perf/nat.sh/Mac workload/new research/delegation runs in this task.
