# Slice209f — source reconnection inside the owning Node

Starting revision: 07a8154.

No production API or behavior changes. The native Node discovery fixture now
composes the existing ReconnectingPeerDiscovery and DiscoveryConnector with
separate owned TCP/TLS source connectors/acceptors. Initial source authentication
remains explicit; replacement sessions use a reserved range above generation1.
Node polling drives the client-side source reconnect, while the fixture polls
the independently owned source acceptor/responder.

The new history closes and drops the original source responder, expires hints
and disconnects one data peer. A write commits through the unaffected peer while
the source is unavailable. Source recovery accepts a later authenticated session
and refreshes the missing peer without replacing the Node or its cache. The
original operation1 receipt remains value7 while current state is10. After
draining/joining and reopening every original file, both operation IDs remain
deduplicated; a new operation reaches15.

The previous Node history still exercises renewal, peer reconnect, cached writes
with the source offline and shutdown with a pending lookup. It now also verifies
cleanup of the dedicated source connector and acceptor workers.

Evidence:

- node.log: initial compile failure because the new fixture omitted the
  SecureSession trait import needed to close a returned session. Import fixed.
- node-run.log: both owning-Node histories pass.
- default.log: both pass with the default feature build.
- node-final.log / default-final.log: both pass again after adding explicit
  original receipt value/duplicate assertions.
- protocols.log: all15 QUIC connector and27 remote discovery tests pass,
  including the existing host/TCP/QUIC reconnect and generation-floor cases.
- clippy.log, checks.log: intermediate strict checks pass.
- checks-final.log: final formatting and all four strict Clippy profiles pass
  without diagnostics.
- inventory.log / metadata.log:108 contract inventory paths and13 obligation
  metadata tests pass. This extends evidence for an existing contract.
- source.sha256 / source-check.log: changed source/evidence fingerprints.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test effect_owner owning_node_ -- --nocapture
cargo +stable test --locked --offline --test effect_owner owning_node_ -- --nocapture
cargo +stable test --locked --offline --all-features --test remote_discovery --test quic_connect
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

The new whole-Node history selects TCP/TLS for both source and data. It does not
upgrade the separate QUIC protocol tests into whole-Node QUIC evidence. Native
startup provisioning, combined endpoint/recursive movement, macOS/separate-host
acceptance and the rest of P0–P7 remain open. File close/reopen is not hardware
power-loss evidence, and bounded applied reads are not new quorum-read evidence.
