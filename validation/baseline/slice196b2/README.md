# Slice196b2 — authenticated leadership maintenance

Base revision: fe882d6a490758119240aaab095c09df58f14803. RPL-1.5.
Changed production/test inputs are recorded in `sources.sha256`.

The existing counter executable now exposes the durable Rust maintenance
workflow through an explicit `--leadership-maintenance enabled` profile.
It selects application schema2, wire8, authenticated service access and 64
permanent operation records. The ordinary counter retains schema1 and its
original command bytes. Existing plain data is refused by the new profile;
reopening it with the original profile preserves the value.

Start records an original exact target/configuration and source identity.
Status obtains a fresh Node quorum read. Resume authorizes a bounded local
retry; cancellation releases local quiescence before proposing the original
durable cancellation. A terminal cancellation retry reads the original record
instead of interfering with a newer handoff. Completed records are historical,
not current leadership certificates. Inspect is sufficient for status; mutation
requires Admin through the existing authenticated command channel.

The single local driver retains at most one internal client ticket and one
attempt identity/deadline. It attempts once per original intent/leader term,
cancels its exact transfer/completion wait on five-second expiry or shutdown,
and consumes outputs separately from the connection's own ticket. A committed
Pending intent authorizes resumption after restart. Lost connections cancel
observation, not the replicated operation. There is no new persistence owner,
durability token, generation, watermark or consensus protocol.

## Evidence

- `executable.log`: six focused executable tests cover TCP/WAL and
  QUIC/checkpoint pending recovery after process termination, terminal restart,
  target writes and original retries; reader/writer mutation denial; deadline,
  resume, cancellation and old terminal retry during a newer handoff; lost Begin
  reply; accepted status read close/deadline cleanup; incompatible-profile
  refusal followed by successful original-profile recovery.
- `regression.log`: 106 passing test executions: counter binary6, counter
  service76, directory service13, durable maintenance application6, transfer
  service5. The directory/transfer binaries also compile and have zero unit tests.
  These scopes overlap the focused tests; counts are not distinct proofs.
- `fmt.log`, `clippy-all.log`, `clippy-default.log`, `clippy-minimal.log`:
  formatting and all-target strict Clippy for each required feature profile pass
  with zero diagnostics.
- `inventory.log`: metadata/path checks for 100 contracts; not a safety proof.

Exact commands are in `commands.txt`. Socket/process tests use authorized local
loopback access. No macOS or separate-host execution is inferred.

## Failures and focused repairs

`executable-before-terminal-retry-fix.log` retains two failures: the fixture
looked for a hardcoded recover-member log while running create mode; it now uses
the actual selected service log. An old terminal cancellation retry tried to
append during another handoff and correctly received Busy. The command now reads
that original terminal outcome through a quorum, leaving the newer attempt alone.
The final history explicitly verifies the newer handoff still completes.

`regression-before-message-fix.log` retains the existing configuration client's
failed exact-message check. Its previous unknown-outcome wording was restored;
the new maintenance interruption message is a separate branch. No lint, quorum,
durability or ownership requirement was weakened.

## Limits

This executable profile is the native counter composition without a membership
administration plan. The public Rust wrapper remains independently composable.
There is no automatic format migration or record eviction. The new histories
exercise selected majority configurations and phase-boundary process kills,
not arbitrary power loss, joint/recursive handoff faults, multi-group drain,
other lifecycle application wrappers or general failover certification.
Coordinated drain197 and assignment listing198 remain separate work. Full P0–P7,
platform validation and the original performance gates remain active; P8 is deferred.
