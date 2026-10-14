# Slice444 schema — observe the actual Pi test failure

Written before diagnostic implementation. The Pi standalone qualification has
63/64 passing tests; the lagging-follower snapshot test fails at client.write
with RecvError. Its paused follower still runs the existing tick/election path;
a higher term after elapsed time or a bounded peer-queue error are hypotheses,
not established causes. Preserve the original Pi source, result and error.

Data/API: add only cfg(test) Actor.run error reporting to stderr with replica
slot, current Core role/term, receive-pause flag and the actual returned error.
No public observer/provider/API or production error change. In the existing
cfg(test) snapshot test, retain a failed client-write error, await the existing
Cluster.shutdown on all owned actor handles, then fail with both the original
client error and cleanup result. Keep successful receipt-count/frontier/snapshot/
attribution assertions exactly as before. No skipped work or accepted failure.

Ownership/partial progress/cleanup: the same Actor owns pending senders, requests,
Core/provider state and queues. Reporting borrows the failed result before it
escapes; it does not restart, retry, suppress or replace it. The same Cluster
owns joins; shutdown signals stop before joining and returns its first actual
Actor error, with the existing unfinished-handle ownership/Drop behavior. Do not
unpause or alter peer/election state before reporting the cause. Completed and
unresolved originals remain the original outcomes; cleanup precedes test panic
instead of letting assertion unwinding abort the actor handles first.

Failure/restart: no storage format, durable state, token, watermark, generation,
recovery/default timer/queue/resource/protocol/authentication change. The test
retains the same two-second campaign behavior,16 server workers,12000 originals,
batch4, paused follower, compaction/catch-up controls and assertions. No timeout
extension, capacity increase or fixture workaround is selected from an assumed
cause. Original Pi qualification stays failed until the actual Actor error is
observed and an independently justified correction is qualified separately.

Scope/checks: only benchmarks/matched/src/voteboat_engine/actor.rs and the existing
test block in benchmarks/matched/src/voteboat_engine.rs change. Capture complete
selected source hashes/modes before/after and exact production projections:
removing the sole cfg(test) diagnostic block and restoring the changed test
statement reproduces the prior bytes; all other selected inputs remain exact.
On Linux run only the exact failed test, formatting and strict standalone Clippy;
preserve failures and original handles. No broad suite, new helper, performance
artifact, benchmark/profile, native/root suite, Mac or Pi command. Parent transfers
the exact diagnostic delta onto the preserved Pi source and reruns only the failed
test once with its pinned compiler/cached target; no dependency update or deadline
change. Diagnose before any production or fixture fix and publish only after the
parent reviews the actual cause and checked scope.
