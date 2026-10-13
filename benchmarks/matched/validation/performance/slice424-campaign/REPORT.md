# Bounded measurement and client-completion boundary (424)

The isolated candidate preserves reviewed 422/423 observer and count/unit/byte
changes. Its historically captured 1091610+merged working inputs all match the
published 82bce5fd1808c73b1c5ea610bd01e2f56ef6a128 Git blobs: 1404 source/config/
docs/test/example inputs, not all necessarily compiled. That isolated scope is historical. Root subsequently applied the exact patch
and completed [merged qualification](main/README.md).

The unified runner records campaign_timeout_secs, default 3600/range 1..7200, and
cleanup_timeout_secs=30. Timeout means an incomplete failed measurement. Cleanup
always runs; both failures are retained and campaign remains Error::source/first
cause. VB shutdown keeps unfinished handles owned until their joins complete;
canceling cleanup retains the exact handle identities for Cluster::Drop to abort.
Normal actor processing and slot join order remain unchanged.

The throughput clock stops after every client join, before latency-vector
flattening, exact global validation, sorting and follower verification. Individual
client result/latency sampling is unchanged. Post-clock aggregation validates every
client's original/sample counts and total exact counts; no partial result is a
success. Upstream source/artifact, engine Config and dependencies are unchanged.
Its untouched driver's 400s internal guard remains a separate recorded limitation.

Twenty-five tests pass; isolated formatting and strict standalone all-target Clippy
have zero diagnostics. Controls exercise finite zero-budget Tokio deadlines
without long waits or a paused clock, owned JoinSet cancellation, actual Cluster
shutdown cancellation/all three abort-release acknowledgments, cleanup expiry,
original and combined errors, CLI default/bounds, and exact post-clock samples/
malformed counts. All existing observer/envelope/original/maintenance/follower
snapshot controls remain. The owner-cancellation fixture first completes actual
Raft shutdown, then refills the same Cluster owner with three controlled stalled
actors; it tests handle ownership without claiming a new protocol failure model.

Five actual CLI histories succeed: VB default/off and maximum/on each 12000 originals/
3000 batch4 requests/frontiers12001/two checkpoints per replica; OpenRaft maximum
also 12000/3000/frontiers12001; both engines' 128-original singleton histories yield
frontiers 133. Defaults 3600, explicit minimum 1 and maximum 7200 are recorded. Two
expected parser failures reject 0 and 7201. Raw commands/stdout/stderr/exits and
CHECKS.json preserve all cases. These are debug functional checks, not timing or
performance-minimum evidence.

The initial test compile's missing Duration import is preserved in test-first.log;
a focused qualified import fixes it. The reviewed cleanup ownership fix receives
its own actual owner control before final qualification. No lint suppression,
threshold, runtime feature or dependency change occurred.

The finite async cleanup guard does not itself preempt blocking synchronous work
or runtime destructor joins. External process watchdogs still need an explicit
setup/campaign/drain/cleanup allowance. No asserted release, benchmark rate claim,
main promotion or upstream guard extension is included in this handback.
