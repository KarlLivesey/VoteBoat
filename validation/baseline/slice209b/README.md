# Slice209b — discovery progress through the owning connector

Starting revision43426fe. DiscoveryDriver is an optional public extension for
bounded asynchronous endpoint progress, pending ownership and wake deadlines.
DiscoveryConnector::new_driven selects it without changing the existing manual
constructor or PeerConnector/Node assembly. NativeRemotePeerDiscovery implements
the extension; no thread, shared global, new wire format or persistent format is
introduced.

Driven-mode transient misses retain exact connection requests rather than
rejecting them into roster backoff. Waiting, submitted and terminal work share
the declared request capacity. One discovery visit advances the source and at
most one waiting request; one-visit polling alternates with the underlying
connector and zero-I/O polling does not rotate priority. Original deadlines,
peer pins, generation and cancellation checks remain in force. A local terminal
outcome stays owned if the underlying connector poll fails. Wake deadlines
include negative-result retries only while work needs them.

Evidence:

- core.log and core-waiting.log: earlier host checks during implementation.
- node.log: the initial Node fixture held time fixed, preventing roster backoff
  from expiring. node-clock.log and node-state.log retain the advancing-clock
  failure:50ms hints expired during the minimum100ms post-rejection backoff,
  repeatedly refreshing without opening peer connections. Queued driven requests
  fix this boundary while preserving backoff after actual failed connections.
- node-waiting.log / node-outcome.log: connections progress, but the reconnect
  fixture observes Unknown(LeadershipChanged) while hundred-group timers compete.
  node-reconnected.log retains that election-pressure failure. The test now uses
  explicit10s minimum election timers and the existing100-step replica budget;
  default profiles retain their original timers. This isolates discovery from
  an unrelated election-load schedule, not a claim that schedule is validated.
- node-initial.log / node-diagnostic.log / lint-final.log: retained development
  compile errors, corrected before the final checks.
- node-controlled.log: the selected Node reconnect/reopen history passes.
- quic.log: real QUIC discovery and pinned-target connection passes with only
  connector progress calls, followed by bidirectional data exchange.
- all.log:235 all-feature tests pass across discovery, connect, remote_discovery,
  quic_connect and effect_owner, including176 owning-runtime tests.
- final-all.log:59 final discovery/connector tests pass, including the final
  negative-retry wake change. node-final.log: the strengthened Node test passes,
  committing new data with expired hints and discovery offline, then draining a
  pending lookup and preserving all three original operation IDs after reopen.
- default.log / default-node.log:45 default-feature tests and the Node history
  pass. native-only.log:24 tests pass. core-final.log:7 downstream host/public
  discovery tests pass without native providers.
- checks-final.log: formatting and strict Clippy pass for default, all-feature,
  core-only and native-only builds with zero diagnostics. Earlier check logs
  remain historical evidence, not substitutes for this final check.
- inventory.log / metadata.log:107 inventory paths and13 metadata checks pass.
  The new seam does not count as an operation-obligation audit.
- source.sha256 / source-check.log: final changed source and evidence fingerprints.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test discovery --test connect --test remote_discovery --test quic_connect --test effect_owner
cargo +stable test --locked --offline --all-features --test discovery --test connect --test remote_discovery --test quic_connect
cargo +stable test --locked --offline --all-features --test effect_owner owning_node_drives_discovery -- --nocapture
cargo +stable test --locked --offline --test discovery --test connect --test remote_discovery
cargo +stable test --locked --offline --test effect_owner owning_node_drives_discovery -- --nocapture
cargo +stable test --locked --offline --no-default-features --features native --test discovery --test remote_discovery
cargo +stable test --locked --offline --no-default-features --test discovery
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

Runtime feature configurations were run sequentially with loopback permission.
These are bounded Linux process/file histories, not hardware power-loss or
macOS/separate-machine acceptance. Endpoint caches remain volatile and cannot
grant membership or ownership. Automatic source-session reconnection, executable
session provisioning and recursive parent-offline composition remain open.
Full P0–P7 remains active.
