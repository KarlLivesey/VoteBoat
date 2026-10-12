# Mac324 controlled-network failure: read-only localization

Original all-feature command failed at controls::real_dispatched_rejected_batch_cancels_exact_original_after_close.
The exact log is mac-results/mac-all-logical-network.log: child fixture wait at
cluster.rs:170 exceeded its existing 15s end. Root path ends
voteboat-logical-nine-child-85674-7. The following refusal test failed only because
the shared history mutex was poisoned. Target summary: 2 passed / 2 failed; the
separate nine-owner target did not execute after Cargo stopped this stage.

Frozen Cluster::open enqueues the selected original child Campaign and immediately
waits at line 123 for leader() readiness. The predicate requires Role::Leader, no pending
dependency, current-term committed prefix and application through commit. This is
inside creation of the first cohort in the failed history, before claim/proof,
queue cancellation or session-control body. It localizes the failed wait to
initial child election/readiness; it does not establish a particular stalled job,
load cause, protocol defect or session cancellation failure. Claim has its own
separate timeout and message. No timing/assertion changes or unchanged retry.

Original aggregate run=101 / source_guard=0 is retained, plus failed audit. A separate
unrun-only continuation skips the original failed stage and every passed gate,
uses separate terminal/source markers, and retains original stage logs. Linux full
selected matrix independently passes. Whole 324 supported-platform scope remains
partial. Later nested fixture edits and Mac315b diagnosis are outside this cut.
