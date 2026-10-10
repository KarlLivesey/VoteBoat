# Slice209e — versioned executable endpoint updates

Starting revision: f8708b399154dbe98f08d9bf1188542a13699fce.

The opt-in counter discovery source accepts a bounded v2 startup file with an
explicit nonzero hint generation. Authenticated Inspect/Configure commands
report status and replace one existing endpoint address using expected/next
generations. Identities and TLS names remain fixed. Shared views observe
accepted updates; closing one view does not close the source used by another.
The exact latest tuple is retryable during the source lifetime. Invalid, stale,
conflicting, unknown-node and duplicate-address updates leave state unchanged.

Updates report durable=false. They neither rewrite the startup file nor update
other sources. The process histories explicitly save the accepted v2 snapshot
before restarting; no automatic persistence is claimed. Legacy v1 sources
retain immutable endpoints and session-derived generations. The existing
discovery wire protocol, Raft state and independent peer pin checks are unchanged.

Evidence:

- unit.log: five new source unit tests pass.
- clippy.log: initial cognitive-complexity finding in the command dispatcher;
  extracting discovery command handling fixes it without changing thresholds.
- clippy-final.log, checks.log and format.log: intermediate clean checks.
- service.log: initial seven discovery integration tests pass before the
  malformed-generation process test was added.
- shared.log: all34 counter binary tests,19 directory service tests and18
  transfer service tests pass. These cover the shared endpoint parser/access
  mapping and surrounding credential/lifecycle operations.
- service-final.log: the first final run passes seven tests and fails the QUIC
  update history's initial read. The fixture deliberately advertised node2's
  stale address but assumed auto-routing could still find any elected leader.
  The correction checks initial data via known authenticated addresses, restores
  discovery and preserves the original stale-address refusal/update assertions.
  No production timeout, routing behavior or quorum rule was changed.
- service-corrected.log: all eight all-feature discovery tests pass. TCP/QUIC
  data transport, authorization refusal, exact/conflicting update, explicit
  saved-file restart and original application operation retry are exercised.
  Generation0 is refused before storage is opened.
- default.log: all six default-feature discovery tests pass independently.
- checks-final.log: formatting and strict Clippy pass with zero diagnostics in
  default, all-feature, core-only and native-only configurations.
- inventory-final.log:108 inventory entries and paths pass.
- metadata-final.log:13 provider-obligation metadata tests pass. No new public
  provider or additional reviewed obligation family is claimed.
- source.sha256 / source-check.log: fingerprints of changed source and evidence.

Commands:

```sh
cargo +stable test --locked --offline --all-features --bin voteboat-counter command_discovery
cargo +stable test --locked --offline --all-features --bin voteboat-counter --test directory_service --test transfer_service
cargo +stable test --locked --offline --all-features --test counter_service command_discovery -- --nocapture
cargo +stable test --locked --offline --test counter_service command_discovery -- --nocapture
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

Runtime feature configurations ran sequentially with loopback permission.
Commands and discovery use authenticated TCP/TLS; the two service histories
select TCP or QUIC for Raft data transport. These are Linux process histories,
not macOS/separate-host acceptance or device power-loss evidence. Native peer
source provisioning and combined long-lived discovery/recursive movement
remain open. The full P0–P7 goal remains active.
