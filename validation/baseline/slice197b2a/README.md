# Slice197b2a — executable retained-replica drain

Base revision: `a9a5103`. Linux local execution. Full P0–P7 and general
membership-aware drain remain open.

The opt-in authenticated counter profile connects the existing replicated
Maintenance operation, local DrainJournal and Node gate to start/status/resume/
cancel/stop commands. Original source/target/configuration and operation IDs are
retained. One off-owner publication worker is joined before directory ownership
ends, including shutdown/error cleanup. Missing or corrupt required journals
refuse recovery. Omitting the selected profile while its journal exists refuses
startup. Initialization may persist an owner-bound empty envelope, never a fake
operation; existing full records remain readable.

Stop requires a completed original handoff, unchanged committed stable
configuration, sufficient remaining configured voter capacity under the actual
recursive policy, and local quiescence. This is retained-replica maintenance,
not decommissioning or a current remote-availability certificate. Cancellation
applies to the local gate; a delivered handoff cannot be retracted. The separate
replicated handoff retains its own cancellation command. Restart never stops
the process automatically.

## Actual checks

- `affected.log`:83 counter-service tests,10 drain-journal tests and7 command-unit
  tests pass. Includes the seven new authenticated process tests, initialized
  envelope conformance and nested/weighted capacity checks. These run actual
  TCP/QUIC processes and native files, not in-memory substitutes.
- `final-drain.log`: all7 process tests pass after adding explicit QUIC
  checkpoint publication before stop/recovery. Covers active/cancelled restart,
  continued two-voter writes, original retries, lost replies, denied mutation,
  incomplete-handoff stop refusal, missing journal, omitted profile and a
  deliberately failed journal publication that stops the service.
- `owner-native.log`: all169 owning-Node and18 native startup tests pass,
  including prior gate/order and TCP/QUIC durable-journal histories.
- `minimal-tests.log`: all163 owning-Node tests pass without default features.
- `fmt.log`, `clippy-all.log`, `clippy-default.log`, `clippy-minimal.log`:
  formatting and strict all-target Clippy pass with zero diagnostics.
- `inventory.log`:102 contract metadata/path checks pass. This is not a safety
  proof. The existing journal contract now includes explicit initialization.

There are11 new tests across the journal, executable and command-unit targets.
Repeated runs overlap. `source-sha256.txt` identifies source inputs; commands are
listed in `commands.txt`. The pre-push hook remains enabled.

## Preserved failures and limits

`drain-process-initial.log` records a test-harness race: it queried a restarted
process before the command listener was available. Bounded readiness observation
fixed it. `recovery.log` records an incorrect automatic-checkpoint assertion
reused for a manual checkpoint: the output already showed checkpoint_base=6.
The corrected manual observation passes in `final-drain.log`. No production
quorum, durability check or lint threshold was relaxed.

Atomic-journal rejection/uncertainty cases use public host I/O; executable
publication failure is injected with an unwritable staging shape. Process-kill
and file-corruption tests are not physical power-loss validation. macOS,
separate-host deployment, membership-aware replacement/removal, general
multi-group orchestration and the remaining full-roadmap gates stay open.
