# Slice415: ordinary progress versus heartbeat retransmission

Purpose: remove payload retransmission caused solely by other peers' progress,
while preserving ordinary request identity, heartbeat repair, exact durable
quorum evidence and every original application result. This changes production
Core work ordering; it does not change host batching, maintenance or storage.

## Data and API

Add private `Replication::commit_at_start: u64`: the durable leader commit value
on creation of that request. It is a lower-bound notification obligation, not
evidence that the peer applied or even received that frontier. Retransmission
keeps this scalar unchanged even when the retry carries a newer leader commit,
because an ACK with the same context can answer the earlier transmission.

Existing maps own this scalar inside existing bounded per-recipient request
records. No public seam, new provider, disk format, wire field or growing cache.
All constructors initialize it; snapshot/repair records retain original paths.

`broadcast_progress` sends ordinary work only to peers without an outstanding
ordinary request. An outstanding snapshot retains the original fallback.
`broadcast` remains the exact heartbeat/explicit fallback retransmission path.
LeaderAppend and Commit use progress broadcasts; early admitted request ranges
remain pending and are skipped rather than being retransmitted.

## State transitions and ownership

1. A new ordinary Append records exact context/configuration/range and current
   durable commit. Owned entry copies leave Core under existing effect budgets.
2. Another peer's progress does not replay this in-flight request. Heartbeat
   reconstructs the same range/context and may advertise the current durable
   commit, without upgrading `commit_at_start`.
3. Exact successful ACK validates the original end, retires the original request,
   updates durable matching progress, then evaluates the existing quorum frontier.
4. A newly required commit persist remains the only pending transition; its exact
   Durable completion emits committed originals and invokes progress broadcast.
5. Without a new persist, ACK emits the next bounded data range if the peer is
   behind. Otherwise, if `commit_at_start < durable.commit_index`, emit one new
   empty Append with a fresh context and the current durable commit. ACK of this
   notification cannot recursively notify at the same frontier.
6. Failure ACK follows original rewind/retry behavior. Duplicate/stale ACK,
   empty-alias handling, higher terms and configuration scope preserve original
   validation and cancellation. Term/configuration/compaction clears request
   records exactly as before; verified recovery reconstructs no transient debt.

Snapshot/repair/retirement/transfer paths retain original fallback behavior. A
joint configuration or configuration not yet committed also retains legacy
broadcasts; only stable committed membership uses progress suppression. A
lost progress request is retried by the existing heartbeat. This removes
opportunistic progress-triggered ordinary retries, so packet-loss latency may
wait for the existing heartbeat driver; it does not suppress that driver.

The same serialized owner controls planning, ACK handling and completion. No new
lease, cancellation or failure lifetime. Uncertain storage failures still fence
and clear all in-memory requests. Effect reservations remain conservative: fewer
ordinary sends per progress broadcast; ACK emits at most its original bounded
ordinary-send allowance. Existing conservative per-peer message/effect metadata
reservations dominate the added private scalar. Retry construction is separated
from new-request construction to keep the existing function-length lint clean.

## Acceptance

* One peer ACK/commit does not duplicate another peer's in-flight data frame.
* Heartbeat retains exact context/range and latest durable commit; an old ACK
  after a newer retry still causes a fresh owed commit notification.
* Lost/delayed/duplicate ACKs do not invent progress, drop original results or
  produce a same-frontier empty-notification loop.
* Partial prefixes and a batch spanning several RPC chunks commit/apply every
  original separately, using only exact durable quorum and local completion.
* Stable configured and checkpoint-backed histories preserve the same behavior;
  snapshot/repair/retirement/transfer retain fallback and existing scope checks.
* Existing admitted overlap and empty-alias controls, request-window bounds,
  membership and checkpoint histories remain valid.
* Focused library controls plus root integration and four strict configurations;
  no performance claim until a reproducible frozen-source comparison.
