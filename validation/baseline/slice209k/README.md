# Slice209k — endpoint repair after recursive metadata movement

The native data group uses the existing driven discovery and reconnect contracts
after two actual metadata moves (1 -> 9 -> 11), parent-locator updates and owner
grant adoption. Every metadata group is stopped and its files captured before
child restart. The highest-numbered data replica then moves to a new address;
source sessions are explicitly dropped, lower-generation hints are refused by
both surviving replicas, and generation3 permits fresh data connections.

The test source connector creates actual authenticated TCP/TLS or QUIC session
pairs on accepted reconnect attempts. Its reserved endpoint is a fixture handle,
not an executable network discovery deployment. Cache floors persist within
the same resolver, not across reconstruction. Native data recovery, transport,
worker ownership, source protocol and routing decisions use production paths.

## Executed evidence

- `reconnecting-final-runtime.log`: all four new TCP/QUIC WAL/checkpoint cases
  pass. Original operation receipts retain values7/10, a new operation reaches15,
  and another cold restart retains all three receipts and three outbox entries.
  All stopped metadata files remain unchanged.
- `discovery-regressions.log`: all15 QUIC connector and28 remote-discovery tests
  pass, including existing source reconnect, generation, malformed reply,
  cancellation and ownership histories.
- `metadata-regressions.log`: all22 metadata-movement histories pass, including
  the four new cases, automatic lookup, retained/imported ownership, later
  handoff, retirement and ancestor-independent writes after recovery.
- `default-runtime.log`: both new TCP cases pass with default features after
  the all-feature processes complete.
- `pre-push.log`: formatting and strict Clippy pass for default, all-feature,
  core-only and native-only configurations, with zero diagnostics.
- `inventory.log`:108 contract entries have valid inventory metadata and paths.
- `metadata-direct.log`:13 provider-ledger metadata tests pass. These checks do
  not establish provider conformance by themselves.

Final commands (socket tests use the authorized native runtime):

```sh
cargo +stable test --locked --offline --all-features --test routed metadata_moves -- --nocapture
cargo +stable test --locked --offline --all-features --test remote_discovery --test quic_connect
cargo +stable test --locked --offline --test routed moved_metadata_endpoint_refresh -- --nocapture
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

The initial compilation failures (`initial.log`, `identity-shape.log`) show
incorrect fixture assumptions about non-cloneable startup and the runtime owner
identity. `checks-initial.log` also records duplicate inclusion of a shared
session module. Those were corrected by consuming startup once, querying the
actual Raft identity and sharing the existing module.

`first-runtime.log`, `quic-diagnostic.log`, `quic-loss-cause.log` and the superseded
`reconnecting-runtime.log` retain the initial QUIC source failures. A temporary
diagnostic identifies QUIC `TimedOut`: the fixture kept an idle source without a
reconnect owner. The diagnostic was removed. The final fixture composes
ReconnectingPeerDiscovery and explicitly closes source sessions on both
transports; neither production timeouts nor stale-generation rules were relaxed.

These are selected Linux loopback histories. Broader faults, durable discovery
cache policy, separate-host/macOS acceptance and the P0–P7 performance gates
remain open. No new production API, dependency or persisted format is added.
