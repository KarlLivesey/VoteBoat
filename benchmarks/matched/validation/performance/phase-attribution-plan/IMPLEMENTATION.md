# Reviewed schema implementation choices

One const-generic Actor<TRACE> and MemoryStore<TRACE> select fixed local elapsed
counters at startup. False branches read no attribution clocks and update no
attribution counters. Existing protocol/runtime clocks remain unchanged.

Actors return an optional finalized record on successful owned shutdown. The
cluster collects records after joining all actors; the report attaches them only
then. Scope includes campaign, warmup, measured work, follower drain and shutdown.
Store append/barrier counters are nested inside persistence or maintenance;
maintenance also includes its effects. These elapsed intervals must not be summed
as exclusive CPU phases or critical-path time.

The CLI explicitly accepts --attribution true/false. True is rejected for OpenRaft
because upstream phases are unavailable. Default reports omit the optional field.
Counters have fixed phase count, checked u64 arithmetic and per-call failures;
overflow fails the diagnostic instead of silently wrapping. No per-entry traces
or global measurement-window/reset machinery are added.
