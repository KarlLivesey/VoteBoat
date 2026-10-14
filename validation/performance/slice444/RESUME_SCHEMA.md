# Slice444 resume correction

Pi trace2015 proves the paused follower campaigns immediately after unpause,
displacing leader1; all replicas reach12002 instead of12001, with one snapshot
install and successful cleanup. Preserve both failed corrections and raw traces.

Test-only data: Actor owns a cfg(test) boolean recording that its tick was
suspended. A paused tick sets it and emits no event. The first resumed tick takes
the flag and refreshes last_contact before ordinary heartbeat/campaign selection.
This supplies the existing election interval to receive queued leader contact,
including when the already-selected timer wakes after the pause flag changes.
No extra channel, public API, production layout/timer/protocol or storage change.
The flag is local test control, initialized false, with no durable generation or
restart dependency. Drop/cleanup and pending original ownership stay unchanged.

Remove temporary tracing; retain bounded write failure cleanup and all exact
12000-original/snapshot/frontier assertions. Deterministic control requires paused
state unchanged, first resumed tick stays follower with refreshed contact, then a
separately forced overdue unpaused tick advances the normal election. No sleeps
or increased election deadline. Run these two focused controls and fmt/strict on
Linux/Pi/Mac; reuse other source scopes. Publish through the enabled full hook.
