# Slice197b2b2 — explicit executable membership-drain workflow

Base revision: `88b4719`. Local Linux process evidence. The full P0–P7 goal,
automatic multi-group coordination and final learner removal remain open.

The source can load a bounded original drain file alongside the authenticated
provisioned administration plan. It binds exact source/handoff stores, original
configuration, outer operation and original joint/final records. The records
must match the trusted administration file. Store bindings are checked against
deployment; the source identity is checked before opening its runtime.

The existing journal worker publishes the plan digest before releasing handoff.
Recovery reloads the original plan, checks the journal binding and restores
admission before polling. The operator invokes the existing authenticated
configuration executor on the current leader, preserving original operation IDs
across joint/final recovery. The source permits stop only after exact committed
final membership, application catch-up and local quiescence. It remains a
non-voting learner; this is not a complete decommissioning certificate.

Maintenance and provisioned membership now share the wrapper's actual readiness
envelope. Normal Node/core readiness, placement and live-session authorization
remain required. Automatic administration stays incompatible with this profile.
There is no new consensus engine, wire format, log owner or background worker.

## Checks

- `process-initial.log`: first three TCP/QUIC process histories pass.
- `counter-service-first.log`: full87-test service run passes before the last
  malformed-input test and equivalent startup-validation extraction.
- `process-final.log`: all four new process tests pass. TCP/WAL and
  QUIC/checkpoint histories never read the initial drain or joint replies,
  reopen joint and final states, retain IDs, reject premature/unauthorized
  commands, stop the source and preserve retries/new writes.
- Recovery refuses a changed or omitted active plan. Oversized, malformed,
  unprovisioned and mismatched files refuse with the regular files at the stopped
  source store's top level unchanged. Automatic administration is also refused.
- `command-unit.log`: all7 command unit tests pass.
- `fmt.log`, `clippy-all.log`, `clippy-default.log`, `clippy-minimal.log`:
  formatting and all three strict all-target profiles pass without diagnostics.
- `inventory.log`:103 metadata/path records validate. This is not a safety proof.

`counter-service.log` records the final complete88-test suite, all passing. `source-sha256.txt`
identifies implementation and test inputs; `commands.txt` records commands.
No failed process history was discarded. During development Clippy identified
two oversized adapter functions; cohesive profile validation and option-name
recognition were extracted without changing thresholds or suppressing lints.

These are explicit operator steps for the current single-group executable.
Automatic/multi-group orchestration, replacement-learner promotion during drain,
full source removal, physical power loss and macOS execution are not established.
