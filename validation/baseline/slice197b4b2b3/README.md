# Slice197b4b2b3 — multi-group source drain controls

Base revision: `3ff2dbe`. Local Linux execution; full P0–P7 remains active.

The counter executable now binds a complete mixed-role group manifest to the
existing `MembershipDrainPlan`, native drain journal and Node admission gate.
Source start/status/resume/cancel/stop and bounded one-row inspection compose
with the existing group membership and leadership commands. All-group access
checks precede node-wide commands; only authoritative local readiness permits
stop. One joined publication worker preserves the original durability boundary.
There is no new public provider, consensus record or dependency.

- `focused.log`: six tests pass. TCP/WAL and QUIC/checkpoint histories recover
  partial evacuation of two voter groups alongside an unchanged learner group,
  preserve the original plan and data retry IDs, and refuse premature stop.
  Other cases cover source-plan omission/substitution, persisted cancellation,
  all-group permissions, failed publication without successful receipt, malformed
  and incomplete manifests,256-assignment/64KiB/1MiB input limits, and refusal
  before store creation.
- `service-regression.log`: the final full run passes114 service tests and nine
  command tests. All service tests finish in51.41s, with no failures or ignored
  tests. This is the counter suite, not a new full-crate or platform sweep.
- `service-first.log`: the first114-service-test and nine-command-test sweep
  passed. `service-before.log`: the subsequent sweep exposed an older QUIC
  replacement test waiting for readiness only on node2. The preserved source
  log shows the correct original operation preparing learner4 on node1;
  `replacement-runner-before.log` records the resulting timed-out wait. The
  corrected observer accepts the same event on any voter, retaining all
  absent-learner, no-commit and exact replacement-store assertions. The two
  targeted TCP/QUIC replacement histories pass in `replacement.log`.
- `service-second-before.log`: the next sweep exposed an initial follower
  inspection before it learned the write's commit (`joint-create-before.log`)
  and a documented UNKNOWN configuration deadline in the interrupted runner.
  The fixture now waits for all replicas' commit before inspecting their
  recovery. The interrupted-runner case permits one identical-ID rerun for
  that exact UNKNOWN result; the ordinary runner tests still require first-run
  success. Four runner and two joint-retirement tests pass in
  `runner-regression.log` and `joint-regression.log`. Production deadlines,
  outcome classifications and durability are unchanged.
- `startup-before.log`: the first fixture queried a recovering service before
  its listener was ready. It now waits for actual service readiness.
- `focused-before.log`: a later QUIC fixture failed to observe a drain intent.
  Setup had relied on a leader's membership receipt without confirming that
  the retained source learner knew the final commit. The final fixture waits
  for that source's exact committed final record before shutdown and after
  reopen. The failed observation is retained, not counted as a pass.
- `module-before.log`, `clippy-before.log`, `clippy-second-before.log`: initial
  module resolution, enum-size and function-length findings. The final source
  uses an explicit submodule path, boxed driver variants and separate startup/
  run-loop and command parsing responsibilities. No lint thresholds are relaxed.
- All four strict Clippy profiles, formatting, warning-denied API docs and the
  105-contract inventory check pass. Inventory checking validates metadata and
  file paths, not consensus behavior.

`commands.txt` records reproducible checks; `source-sha256.txt` identifies the
validated sources. These finite histories do not prove arbitrary power-loss,
macOS or separate-host operation. Multi-group foreground orchestration remains
the next deliverable; current group moves use the existing explicit commands.
