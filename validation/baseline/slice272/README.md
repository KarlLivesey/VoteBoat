# Slice 272: timed-owner and WAL fault replay

Extends actual-core replay to timed-owner composition using the existing
AutoNode assembly: native TimedShard, FairScheduler, DeadlineQueue, NativeLogStore
with the model journal, and Counter. An unchanged private effect-fulfillment
block is extracted for reuse; production/provider/protocol code is unchanged.

Six checks cover a real timer-origin campaign, accepted-but-uncompleted WAL
visits, separate sync/completion, stale owner/visit refusal across process crash
and simulated power loss, overdue timer work, independent group progress,
delayed/duplicated old messages and original Counter retries after cold restart.
Thirty-two bounded schedules vary three time-step patterns, crash mode and read
IDs. Encoding/decoding replays exact owner, timer, core, journal, message and
application states. This is finite native-majority virtual-time/model-journal
scope, not arbitrary faults, physical worker/device behavior or other policies.

The captured seed1 schedule has150 actions/151 recorded states. Both hosts read
that same file through the bounded decoder and replay it against the actual
owner. To replay an artifact locally:

```sh
VOTEBOAT_TIMED_TRACE=/absolute/path/recorded.vbtime cargo +stable test --locked --offline --all-features --test runtime timer_replay::schedule::bounded_recorded_timer_file_replays_through_the_actual_owner -- --exact --nocapture
```

Linux and macOS each pass full Raft/runtime targets in all/default/core/native
profiles:62/44,62/44,29/29,62/44 (376 executions), plus the captured-file check.
Formatting/four strict Clippy profiles are clean and all666 frozen inputs verify
before/after each matrix. Metadata references remain14 partial reviews/88 named
operations; the checker is not a behavioral certification.

`published.sha256` hashes normalized artifacts and `raw.sha256` records original
bytes. Linux269 is terminal69/0; matching Mac269 and old262/251 lifecycle jobs
remain separate source/acceptance obligations. The full P0–P7 goal and broader platform/fault/provider work stay open.
