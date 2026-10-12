# Recovered physical checkpoint publication

Purpose: P8 cold recovery must await the checkpoint actually covering the
original current application prefix before final shutdown. Cluster::checkpoint
currently accepts any nonzero durable base. On recovered stores this is already
true before its newly admitted manual checkpoint runs. The automatic scheduling
pending map does not track manual NodeControl::Checkpoint admissions.

Data/API: capture a Vec<u64> of each original node's applied frontier immediately
after existing catch-up. Preserve original manual controls. The existing wait
zips the unchanged cohort with those immutable frontiers and requires durable
SnapshotRef.index and base_index >= that node's frontier, plus the original
pending-map/router checks and an explicit selected snapshot worker drain check. No new helper or public API.

Transitions/ownership: existing catch-up -> capture prefix -> submit checkpoint ->
observe published durable reference/compaction/real provider drain -> close.
The checkpoint result is local durability, never quorum authority. Retain exact
same-store recovery, all original node/provider joins and port/directory cleanup.

Failure/partial progress: an old recovered snapshot below the submitted frontier
cannot satisfy publication. No retries, campaigns, timer increases, cancelled
accepted effects or forged completion. A snapshot already covering its frontier
remains valid; core manual checkpoint NotApplied when commit<=base stays intact.
This fixes an independently demonstrated fixture contract; it does not establish
that the old Mac close timeout was caused by this gap.

Acceptance: recovered-store regression with a real newer committed Claim and
original actual durable snapshot coverage, all cleanup preserved; full cold
TCP/QUIC qualification after appropriate exclusive Mac handback. Format and four
strict Clippy profiles zero. Record exact source scope and failures if any.
