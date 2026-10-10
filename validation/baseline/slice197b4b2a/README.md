# Slice197b4b2a — authenticated executable group selection

Base revision: `b5f82b5`. Local Linux execution; full P0–P7 goal remains active.

The counter executable consumes explicit bounded original group manifests and
opens the shared native node. Group-addressed status/read/add/checkpoint commands
use the exact same group/incarnation for permission checks and execution.
Independent applications retain their own operation IDs and receipts. Prefixes
cannot authorize node-wide commands. Unsupported multi-group membership,
leadership/drain and discovery profiles are refused; those integrations remain
next in the plan.

## Evidence

- `final-regression.log`:101 service tests, nine command unit tests and25 startup
  tests pass. Five new service tests cover actual three-process TCP/QUIC groups,
  repeated operation IDs with different values, permissions, leader loss,
  checkpoint/reopen, wrong/missing group identities and malformed/group-count/
  byte-limit/profile inputs. Fake-peer routing histories check original request
  bytes, explicit leader rejection and termination after uncertain delivery.
- `omission-before.log`: the new omitted-flag test fails because ordinary
  single-group recovery starts a partial service over a multi-group WAL.
  `omission-after.log` records all five group tests passing after both startup
  paths enforce complete durable group inventories. Its startup target has zero
  selected tests; complete startup coverage is in `final-regression.log`.
- `directory-before.log`: the additional directory regression reports11 passing
  tests and two address-in-use failures. The fixture fix coordinates descriptor
  release with all process spawning, including clients; it does not serialize
  child execution or alter production timeouts.
- `other-services-final.log`: all13 directory-service, five transfer-service and
  nine credential-refresh tests pass after the fixes. The transfer suite includes
  TCP/WAL and QUIC/checkpoint interruption at every selected split phase and lost
  fence/publication replies.
- `directory-confirm.log`: both originally failing directory histories pass
  separately after the fixture's final stdin/output behavior is checked.
- `clippy-native-before.log`: the native-only build identifies a TLS startup
  helper compiled without callers. TLS-only helper gates are corrected; the
  expanded check also required matching gates on TLS-only credential test
  helpers. `clippy-native.log` records the final result.

Formatting, strict all/default/no-default/native-only Clippy, public API docs
and inventory metadata results are recorded separately. The pre-push hook and
background CI now enforce the fourth supported feature configuration as well.
`source-sha256.txt` identifies the source inputs. Inventory checking establishes
metadata shape and referenced paths, not protocol conformance.

These are finite local process, native-file and routing checks. They do not
establish arbitrary power-loss behavior, macOS execution, separate-host
deployment or complete coordinated multi-group operator maintenance.
