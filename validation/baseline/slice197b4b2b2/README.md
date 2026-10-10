# Slice197b4b2b2 — group-bound leadership maintenance

Base revision: `5c640e6`. Local Linux execution; full P0–P7 remains active.

The counter executable composes one existing maintenance application and one
bounded local leadership driver per declared group. Group-prefixed commands,
cancel waits and client completions preserve exact scope. The original
single-group wrappers and drain workflow remain available. The explicit
schema2/wire8 profile also supplies the correct readiness envelope to group
membership plans. No public provider, new persistence format or dependency is
introduced.

- `focused.log`: three tests pass. TCP/WAL and QUIC/checkpoint histories retain
  pending and cancelled operations across restart, independently use the same
  operation ID in different groups, complete original handoffs, and preserve
  completed historical records across another recovery. They check permission
  scope, wrong stores, data retry history, unaffected group1 and subsequent
  membership. Native recovery inspects pending records before restart, including
  checkpoint base coverage in the QUIC case. Profile mismatch is refused in
  both directions, and original data remains recoverable with its original
  profile.
- `service-regression.log`:108 service tests and nine command unit tests pass,
  including existing single-group leadership, drain, credential, membership
  and interrupted new-voter histories.
- `clippy-before.log`: startup exceeded the unchanged100-line limit. Loading
  administration plans now belongs to the options owner. `fixture-compile-before.log`
  records a fixture's u64/u128 store-ID mismatch, corrected to the typed width.
- `stopped-node-before.log`: fixture leader lookup dereferenced a deliberately
  stopped child. The helper now skips stopped nodes. `follower-commit-before.log`:
  the TCP crash fixture inspected a follower before it had learned the latest
  commit. The final fixture waits for both surviving replicas' commit metadata
  before deliberately crashing them; quorum acknowledgement alone does not
  establish every replica's local commit/apply state.
- Formatting and strict Clippy pass for all/default/no-default/native-only
  configurations. Warnings-denied API docs pass. Inventory validation checks105
  records and conformance paths; it is metadata validation, not a proof.

`source-sha256.txt` identifies final source inputs. These finite local histories
do not establish arbitrary power-loss behavior, macOS or separate-host operation,
or complete coordinated multi-group drain.
