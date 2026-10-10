# Slice197b2b1 — journal-bound Rust membership evacuation

Base revision: `9e1e612`. Local Linux validation. The full P0–P7 goal and
executable membership-aware drain remain open.

`MembershipDrainPlan` owns bounded, immutable original configurations, handoff
identities and joint/final records. Its canonical SHA-256 binding includes
recursive policy structure, fixed weights, stores and operation identities.
The journal persists that binding, not the whole plan. Hosts must retain and
reload the original plan. Native V2 records preserve the binding on cancellation;
V1 local-only records remain readable. The retained-replica executable refuses
to interpret a bound journal without its original host plan.

Pure action selection reuses current-term leadership, targeted handoff and the
existing configuration executor. Normal placement authorization and learner
readiness remain mandatory. Local readiness requires the restored active
journal, complete assignment coverage, exact committed final membership,
application catch-up and drained local ownership. The source retains a learner
entry so it can observe final commitment before shutdown; deleting that entry
is a later operation. No remote-availability certificate is inferred.

## Actual validation

- `contracts-native.log`: all11 journal,6 plan,170 Node and20 native startup
  tests pass. New cases cover changed plans, retained-byte bounds, nested/weighted
  digests, prepared learners, uncommitted/rolled-back membership, and incomplete
  or unrestored local assignment coverage.
- Native TCP/WAL and QUIC/checkpoint histories lose a joint wait, recover the
  original joint/final operations, reopen both boundaries, stop the source and
  preserve original retries and new writes on the remaining voters. The focused
  `native.log` and the unfiltered startup run overlap.
- `counter-service.log`: all84 executable service histories pass, including
  refusal to treat a bound membership journal as retained-replica maintenance.
- `minimal-tests.log`: all6 plan and164 Node tests pass without default features.
- `fmt.log`, `clippy-all.log`, `clippy-default.log`, `clippy-minimal.log`:
  formatting and all three strict all-target profiles pass with zero diagnostics.
- `inventory.log`:103 contract metadata/path checks pass; this is not a safety
  proof. `source-sha256.txt` and `commands.txt` identify inputs and checks.

There are11 new tests across the five affected suites. Test counts are not a
claim of exhaustive safety. Host tests check multi-group completeness; the
native evacuation histories use one group. Native new-voter promotion during
drain, full multi-group orchestration, final learner deletion, phase-internal
power loss, macOS execution and broader roadmap fault gates remain open.

## Preserved failures

`native-initial.log` records a fixture that attempted membership startup from
uninitialized files; it now initializes the original static stores before
opening the member runtime. `planned-owner.log` records a fixture missing the
required peer resources; `planned-owner-fixed.log` shows the correction.

`regression.log` and `profile-refusal.log` preserve the wider suite and focused
failure of the old incompatible-profile test. It reopened node1 immediately
after a routed write, without ensuring that node1 had learned its commit.
The fixture now reopens the replica that applied the acknowledged command.
The full84-test rerun passes with the intended refusal assertion unchanged.

The focused native log retains rejected old-session/membership packets emitted
by the fixture. Successful joint/final commitment and post-restart application
assertions are the tested evidence; this is not a claim that no packets were
rejected or that all transport schedules were checked.
