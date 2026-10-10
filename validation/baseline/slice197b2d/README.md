# Slice197b2d — maintenance-profile replacement drain

Base revision: `7eb265f`. Local Linux validation. Full P0–P7 remains active.

The offline `enroll` command accepts `--leadership-maintenance enabled` and
uses the same schema2 application wrapper/wire8 selection as serving. Default
enrollment remains schema1/wire7. Both source recovery and destination image
validation use the explicitly selected application. All existing source,
membership, pinned checkpoint and same-image retry checks remain in force.
Enrollment does not copy the source's local drain journal.

Two new authenticated process histories use TCP/TLS and QUIC peer replication
(commands remain TCP/TLS). They start from the original three-voter cluster,
commit an exact learner4 assignment for store404/incarnation7 and a maintenance
handoff, checkpoint and stop the source, then import that actual image. Wrong
profiles refuse before creating a destination or changing an existing import;
retry preserves the original GroupLog. The imported checkpoint contains the
completed maintenance record and the original data retry.

The bounded drain runner starts with replacement4 offline. Tests observe the
readiness request and absence of an admitted joint record, then kill the client.
Starting the learner and rerunning the original drain identity completes the
joint/final transition. Source1 joins shutdown; after killing original voter3,
voters2/4 still commit new writes. Their native files recover the exact final
membership and both old/new retries; a fresh post-restart write succeeds.

## Actual validation

- `sandbox-refusal.log`: first run could not bind sandbox sockets; both cases
  stopped at fixture port reservation before service creation. Retained, not a
  protocol failure or pass.
- `replacement-initial.log`: both initial replacement histories pass.
- `regression.log`: all 94 service histories and 8 command unit tests pass,
  including stronger imported maintenance-record and wrong-profile retry
  assertions. Service execution took 48.78 seconds.
- Formatting, all three strict all-target Clippy profiles and the 103-entry
  contract inventory are recorded alongside the executed commands/source hashes.

These are finite selected process schedules. They do not establish multi-group
orchestration, final learner removal, arbitrary phase-internal power loss,
macOS execution, separate-machine deployment or a performance improvement.
The replacement starts without a local drain journal; enabling one later is
still an explicit migration rather than copying the source's local gate.
