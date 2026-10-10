# Slice197b2c — bounded authenticated drain runner

Base revision: `19eaaf5`. Local Linux execution. Full P0–P7, general multi-group
coordination and final learner removal remain open.

`drain-run` drives the existing single-group authenticated workflow from a fixed
source and original sequence/operation. It reads the bound membership operation
and SHA-256 plan identity from source status, checks the identity on every
observation, sends the original configuration operation to current leaders,
and asks the source to stop only after its existing readiness check passes.
The source rechecks stop conditions. No new consensus protocol, log, worker
or background process is introduced.

The foreground client owns one request connection at a time. Limits are128
requests,45 seconds total and five seconds per request. Explicit non-leader
responses allow another configured target; unknown configuration or shutdown
delivery stops with preserved operation identity. Rerunning the same command
resumes from the source's durable journal and original plan. The source must
initially lead or already have the requested drain.

Success means `shutdown_requested=true`: the source accepted the stop request
after reporting local readiness. It does not prove remote worker joining or
current cluster availability. Peer replication is TCP/TLS or QUIC; operator
commands use the existing authenticated TCP/TLS command channel.

## Actual validation

- `runner-initial.log`: all4 new executable histories pass.
- `runner-final.log`: all4 pass after adding shuffled explicit endpoint routing
  to the QUIC case and missing-source refusal. TCP and QUIC clusters complete
  joint/final changes, stop/join the source and continue original retries/new
  writes. A killed runner and source recover the original durable plan.
- `regression.log`: all92 service tests and8 command unit tests pass. Includes
  malformed/wrong/duplicate status identity and plan field rejection.
- `final-targets.log`: the parser unit test and all four runner histories pass
  after refining uncertain initial-admission reporting to preserve the original
  request identity. This final focused run includes the configured-endpoint cases.
- Unauthorized callers, wrong/zero operation IDs, missing credentials and a
  missing configured source refuse without starting the requested drain.
- `fmt.log`, `clippy-all.log`, `clippy-default.log`, `clippy-minimal.log`:
  formatting and all three strict all-target profiles pass with zero diagnostics.
- `inventory.log`:103 contract metadata/path checks pass; not a safety proof.

There are five new tests, including the status-parser unit test. Repeated runs
overlap. `commands.txt` and `source-sha256.txt` identify commands and inputs.
No failing process histories were discarded.

The finite process histories do not cover every coordinator interruption,
network partition, platform or changed-credential schedule. The runner does not
implement endpoint discovery, automatic source leadership acquisition,
multi-group coordination, learner deletion or a durable global operation log.
