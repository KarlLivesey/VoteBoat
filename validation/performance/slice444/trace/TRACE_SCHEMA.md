# Slice444 trace amendment — failed receipt can remain pending

Written before the trace amendment. Preserve the initial444/main archive and
original Pi failures. The focused Pi diagnostic stayed alive for3m24/CPU6s,
without a returned Actor error, then the parent interrupted it and restored the
exact baseline. Logging only Actor.run Err is blind to a live actor holding an
unresolved receipt; no election/queue cause has yet been established.

Add cfg(test) campaign lines before the same existing Campaign event and cfg(test)
role/durable-term transition lines after each existing actor turn. Report slot,
paused-receive flag, pending result count and application/frontier state. A private
test-local observed tuple tracks transitions; no callback/API/provider/production
state changes. Keep the existing actual Actor Err observation. No incoming event,
vote, campaign, timer, queue or original outcome is filtered or suppressed.

Only the existing snapshot test gains a10-second diagnostic failure budget per
client write. Success still requires every original receipt and all original
snapshot/frontier/attribution assertions. Timeout cancels the same owned write
future, drops its remaining receiver waits, captures IDs/frontiers and signals
the existing watch-stop/joins before test failure, retaining the result and cleanup
error. Timeout is never accepted work or a longer production/campaign deadline.
No pause-fixture fix or production behavior change is selected until actual trace.

Same two test paths only, no new module/helper. Capture separate trace before/after
SHA/modes/patch and production projections; removing added cfg(test) trace blocks
reproduces the prior Actor bytes, and the changed snapshot fragment remains inside
the existing cfg(test) module. Run only the exact snapshot case plus standalone
formatting/strict/source guards on Linux. Parent owns one focused Pi causal run
with its pinned compiler/cached target; no broad suite/artifact/profile or remote
command by this agent. Preserve every failure, original handle and initial archive.
