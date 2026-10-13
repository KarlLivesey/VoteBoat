# Slice 423: bounded original-count parity

Authorized implementation in an isolated exact1091610 source overlay first; no main
production edits. Root owns publication/COMPARISON/macro plans. No timing authorized.

## API, limits and ownership

Extend only BatchLimits.MAX_PROPOSAL_ENTRIES from4096 to16384. Existing public
BatchLimits shape/default64, startup configuration lock, ordinary receive/outbound
maximum1024, repair64 and native ready_visit_items1..64 remain. In the matched host,
upstream-default keeps untouched OpenRaft4096 API requests and4096 soft append-merge
setting. VoteBoat selects4096*request_batch originals, with checked arithmetic;
common64 explicitly means64 API requests and therefore64*request_batch originals.
Network replication entries are independently1024 primary/64 common. Report units,
source-derived aggregate original bound and hard/soft meanings; retain raw legacy
fields with an explicit unit disclaimer for consumers.

Before files/tasks/actors, derive exact empty-payload owned Proposal Vec capacity +
staged LogEntry count + update/mutation metadata. Select an explicit bounded harness
LogLimits.max_batch_bytes at least the original1MiB and the checked envelope. Record
its value and Core readback. Native/default limits are unchanged. Count does not waive
command, retained log, full raw allocated capacity, owned staging, effect budget or
per-original application/response bounds. The host retains clients*4+1 pending originals
and4GiB effect ceiling. Header/payload capacity/error order remains the current oracle.

Accepted proposals still install one pending exact log transition; responses escape
only after application of every original after quorum commitment. Written is not a
receipt.1024-entry replication and partial ACKs retain contiguous prefixes, exact
context/term/store/config/ticket matching, finite commit notifications and snapshot
catch-up. Startup choice reselected only after verified recovery, never live mutation.
Whole batch errors precede changes; provider/callback failure fences/closes outstanding
ownership by existing paths. No on-disk/wire/provider/safety feature changes.

## Acceptance

- Defaults and selected4096/16384; invalid0/16385, locked/fenced/error precedence.
- Independent count/allocation/slack/byte/retained/output refusal; insufficient bytes
  cannot accept a larger proposal by count alone. Exact minimal envelope and derived
  host selection are read back before launch.
- Actual4096 batch4 requests merge to16384 originals, with exact every original result,
  positions/frontiers and weighted widths; common units and batch1 independently.
- Partial1024 replication/ACK, delayed/duplicate stale completions, finite commit,
  retained maintenance and actual lagging-follower snapshot catch-up.
- Actual ModelIO proposal/commit append/sync/publication cuts, phase-appropriate exact
  recovery, original retry/at-most-once and configuration reapplication.
- Sequential feature checks, strict/fmt zero, exact complete source maps and original
  failures preserved. Parent reviews before promotion; no timing claim.

## Owned update inventory

Production source: src/raft/batch_limits.rs (ceiling only). Controls:
src/raft/proposals/tests/batch_limits.rs and aggregate.rs, tests/log_store/
batch_limits.rs and aggregate.rs. Existing component-contracts BatchLimits record
updates its maximum and conformance references; no new provider joint.

Comparison host: main.rs/envelopes.rs (+tests) owns one coherent request/original
configuration and budget calculation; workload.rs reports legacy value semantics
and typed capacity_contract; voteboat_engine.rs and actor/tests.rs use selected
bounds/log limits and verify actual16384 originals. stats.rs stores its expanded
bounded histogram in one prelaunch boxed allocation instead of large nested stack
arrays. Existing exact receipts/channels, maintenance and peer-priority paths remain.
No main-tree source writes or promotion before parent review; changed/new inputs
are recorded against BASE.json. Root422 sources remain independent of this overlay.
