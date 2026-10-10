# Slice197b4b2b1 — group-bound membership commands

Base revision: `9224c93`. Local Linux execution; full P0–P7 remains active.

The counter executable accepts a bounded manifest of original group-scoped
administration plans. Configuration completion, cancellation, replies and
execution-time permissions retain exact group/operation bindings. The existing
Node and native administration contracts continue to supply durable joint/final
evidence. No public provider, consensus/storage format or dependency is added.

- `service-regression.log`:105 counter-service tests and nine command unit tests
  pass. This includes the existing automatic, provisioned and client-target
  single-group administration, maintenance and interrupted-new-voter histories.
- `group-admin-final.log`: all four focused tests pass after strengthening
  administrator scope refusals and direct durable-file assertions. TCP keeps
  the committed joint entries in its WAL; QUIC compacts joint bases before
  reopening. Both finish the same original operations, preserve independent
  group histories and allow repeated operation IDs across groups. Reader,
  wrong-group/incarnation admin and unconfigured-group commands are rejected.
- Manifest tests reject malformed headers, empty/duplicate/unknown scopes,
  unknown incarnations, missing files, oversized input, more than256 plans and
  more than1 MiB of aggregate input before creating node storage. The4 MiB
  retained-record ceiling is enforced in the loader; it is not independently
  exhausted by these fixtures.
- `initial-focused.log`: the initial three new tests passed before additional
  count/aggregate and permission/file-inspection checks were added.
- `clippy-before.log`: dispatch complexity26 exceeded the unchanged25 limit.
  Request validation was moved into the administration owner. Final lint logs
  cover all/default/no-default/native-only profiles with warnings denied.
- Formatting, warnings-denied API docs and inventory metadata pass. Inventory
  validation checks105 records and referenced paths, not protocol conformance.

The source hashes identify final source inputs. These finite process and native
file histories do not prove arbitrary power-loss behavior, separate-host or
macOS operation, or complete multi-group leadership/drain orchestration.
