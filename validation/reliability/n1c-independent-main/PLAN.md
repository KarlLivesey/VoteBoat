# Independent main reconciliation plan — before source edits

Scope: existing PR23 only, original N1c `44b95083e96f7b09ab4e900ed4e1c25042896c2a`
plus qualified main `b77fd087b06c5ae76a5a1a25f2cfe3ed6891c339` by real merge.
Merge base `12fb21de87cf31e3799fc9bc93d56a6da726f2b7`. Published
`b95943a3187f8a3c6f48fbcaf2cbd5f6a4e93bad` contains rejected ST2/DR1;
join its history only AFTER committing and auditing the desired tree. This
avoids treating already-ancestral main as if a later merge would restore it.
No main edits, new PR, delegation, research, performance or full matrix here.

## Schema/API, transitions, ownership and partial progress

Keep original AppendHold/ReleaseHeldAppends/TimerConfig surface, bounded held
peer -> durable-last mark, batch_floor, ack recency and configuration notice.
Combine main's V3e PeerSlots/cached order with these fields; preserve V3f
in-place plans/transitions, refusal and canonical authority and EO1 continuation
ownership. No persisted format or quorum-rule change. Counter remains main's
8-byte command, no session ledger, DR1 readiness identities or adapters.

Only confirmed nonzero matching prefixes qualify: data >= batch_floor,
notices >= last. In-flight ordinary requests extend the confirmed prefix for
scheduling only; exact ack correlation/current-term/frontier commit remains.
Data holds require quorum coverage without that peer; notices can be held in
two-voter groups too. A byte-limit split may rehold a spare mid-batch.
One request/follower; bounded holds, no new queue/public state or hold policy.
Preserve learners/catch-up/probes/config/transfer/compaction/reject/heartbeat.
Clear holds on stepdown/config/close; correlated rejection releases spares.
TimedShard owns per-group deadlines and late-poll release, with original host
admission/backpressure limits. Restart reconstructs normal durable Raft state,
not volatile scheduling holds. Frozen fixtures explicitly release held work;
facade heartbeat follows owner drain. Cancel is appended before transfer starts.

Expected conflicts: raft fields/init/heartbeat; progress broadcast cached order
versus hold dispatch; documentation history/status. Reuse only prior relevant
V3e/V3f/EO1+N1c resolutions, never ST2 stall/noop/capacity propagation. Audit
all automatic paths against both parents; stop with concrete evidence if an
indispensable rejected production dependency exists.

## Necessary test-only gate exception

The known AtomicU16 14000/+1024 transfer allocator reaches 65200, overflowing
its max offset487. Extract only reviewed `558643b3..66fcf3e0` fixture hunks:
ports.rs, fixture_isolation.rs, transfer_service.rs and private root modules.
Their preimages match qualified main; verify that before applying. Checked24
layout reserves shared TCP/UDP leases; 65048 valid, 65049/65200 rejected.
Fresh exclusive PID/sequence directories are independent of reusable ports,
skip stale entries, and clean only their owner on partial/unwind paths; children
join before lease release. Preserve main Counter8B and all other semantics.
This fixes a known gate blocker; it is not DR1 production or a new prerequisite.
No relay BrokenPipe or benchmark-lock repair: prior causes remain unproven.

## Acceptance and evidence

1. Merge/commit/audit original44+main; preserve both doc histories and parent's
   PR14/15 rejected/closed/parked and PR24 kept/merged records. Keep macro plan
   and current+next2 existing wind-down gates linked; no roadmap slice.
2. Compare original hold executable code, all original regression fixtures,
   V3e/V3f/EO1 sources and every other production input against pinned parents.
   Record allowed fixture hunks/blobs, protected source hashes and exclusions.
   Then join b959 history with identical desired tree; verify both ancestors.
3. Sequential default/all focused core/hold/runtime/original-nine/logical/
   terminal and V3e/V3f/refusal/retry/EO1 checks with nonempty result audits.
   If fixture exception carried, full isolation+transfer targets in both.
   No new mutations unless actual hold logic changes justify them.
4. Root fmt/four strict Clippy plus matched fmt/Clippy, locked/offline,
   keep-going/all-targets/-D warnings, and enabled actual nonforce push hook.
   Aggregate every rc and reject empty required acceptance. No fullgreen claim.
5. Update same PR CURRENT/body/comment6304682074 with exact source,8B and
   pending two fresh source reviews/full5config/fair exact-head perf. Historical
   standalone and old70-run8B evidence remains separate, no new clearance.
6. Commit compact evidence, save raw reports only in main target reportdir,
   publish same PR branch with all b959 ancestry, then sweep own target/worktree/
   registration/scratch and audit zero owned jobs plus stock guard PASS.

Resource envelope: own checkout target only (matched same root target), cores
10-11, absolute kernel nice19 for all observed build/test threads, <=2 Cargo
jobs, CARGO_INCREMENTAL=0, own<=60GiB/global worktree<=120/root<=150/live<=5/
free>=500GiB. Unchanged stock diskguard before every build/hook step and after
cleanup. Never touch shared /tmp/voteboat-test-ports, foreign conformance or
other owners' processes/scratch. No detached/scheduled jobs.
