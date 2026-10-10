# Slice197b3 — bounded multi-group Rust drain dispatcher

Base revision: `df134dc`. Linux local execution; the full P0–P7 goal remains
active. This slice adds `MembershipDrainCoordinator`, opaque dispatch tickets
and bounded result batches over the existing immutable membership drain plan.

The host supplies fresh Raft observations, dispatches ordinary Node requests,
owns their completion/cancellation and restores the original journal before
polling after restart. Finishing a dispatch ticket only releases a slot. It
never proves commitment or rollback. Source readiness remains the existing
complete-assignment, committed-membership and local-quiescence check.

## Executed evidence

- `core-minimal.log`: 29 Raft tests (including two new dispatcher tests) and
  six membership-plan tests pass without default features. Bounds, cursor
  fairness, foreign/duplicate/pre-restart tickets, wrong journal and mixed
  missing/wrong-group observations are covered. The native-only journal target
  contains no tests in this profile.
- `journal.log`: all11 native drain-journal tests and six membership-plan tests
  pass with all features.
- `regression.log`: all30 native benchmark assembly tests pass, including the
  new TCP and QUIC multi-group histories and existing static/lane/recovery
  histories. The existing static assembly retains its original profile.
- `focused.log`: the final two multi-group histories additionally assert exact
  returned operation IDs and counter values, including original dedup receipts
  and new writes after source shutdown.
- Formatting and strict all/default/no-default-feature Clippy pass with zero
  diagnostics. `inventory.log` checks104 metadata entries and their file paths;
  this is not behavioral conformance evidence.

Each new native history hosts three independent Raft groups over one shared WAL
per node. Group1 reaches final membership, group2 remains untouched and group3
stops at committed joint membership. Planned handoff leaders differ by group.
The source cannot report ready in that mixed state. TCP restarts from WAL;
QUIC checkpoints all groups before restart. Both reload the original drain
journal, reject an old dispatcher ticket, finish original configuration IDs,
drain/join the source and retain service on the two remaining voters. The source
remains a learner; this is not final removal or file-deletion authority.

## Failures that informed fixes

`initial-native-failure.log` records an overly strict test-driver assertion:
leadership change ends a configuration wait with an unknown outcome, not proof
of failure or rollback. The driver now releases that wait and re-observes state.
`initial-timeout.log` and `diagnostic-timeout.log` record followers refusing joint
records because the shared test assembly had not enabled configuration
replication. The explicit member profile now selects the same configuration and
snapshot-repair capabilities as member startup. Consensus gates are preserved.

The deterministic fairness assertion also requires group3 to receive the next
freed slot after groups1/2 fill the window. Scanning now pauses at a full window
rather than circling back and favoring earlier groups indefinitely.

No hardware power-loss, separate-host, macOS, mixed learner-only source,
multi-group executable administration or arbitrary-fault claim is made. Native
restarts here use joined worker shutdown with durable logs/checkpoints.
