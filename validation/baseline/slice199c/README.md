# Slice199c — macOS operator failure evidence

Completed job114158445085 in run38033281575 at78382c0 reports114 counter
tests passing and eight failing in276.70 seconds. The job therefore did not
reach directory/transfer tests. `macos-job.log` preserves the output with only
ANSI and trailing whitespace removed. These failures are not established as
fixed by01d5190's independent retirement feature or draining-read correction.

Failing histories:

- assignments::assignment_pages_quic_bind_permissions_membership_and_reopen
- drain_replacement::maintenance_replacement_drain_waits_for_exact_learner_quic
- group_admin::group_membership_quic_reopens_joint_checkpoints_and_preserves_scope
- group_drain::multi_group_drain_quic_recovers_partial_checkpoints_and_stops_only_when_ready
- group_drain_runner::multi_group_runner_drives_distinct_targets_and_stops_source_quic
- group_drain_runner::multi_group_runner_resumes_partial_membership_after_source_checkpoint_quic
- group_leadership::group_leadership_quic_recovers_independent_pending_and_cancelled_checkpoints
- groups::authenticated_group_commands_quic_recover_independent_histories

Observed replies include explicit LeadershipChanged/NOT_LEADER, unknown command
deadlines, and a fixture expecting Joint after the operation already reached
Final. The assignment failure has an empty assertion message. These observations
do not establish one common cause or justify increasing deadlines or suppressing
failures. Next inspect each failing contract, preserve original operation IDs,
and distinguish invalid fixture assumptions from production recovery defects.
The fresh01d5190 platform jobs were queued when this evidence was recorded.
