# Slice209c — bounded discovery-source reconnection

Starting revision: a8552c226b3c6a2217ec58efef9794a563b4d38e.

ReconnectingPeerDiscovery owns the existing authenticated remote cache/protocol
and a dedicated public PeerConnector. Numeric source address, provisioned identity,
finite reserved generations, caller-supplied monotonic time and explicit bounded
polling remain host inputs. No background executor, wire change, persistent cache
or ownership authority is introduced. The existing outer DiscoveryConnector and
Node progress path can drive the new resolver.

The first QUIC test found that closing a failed source left its socket lease
owned by the retained session. That blocked replacement through the same native
connector. The fix detaches and drops the failed session before dialing, retaining
the source binding, cached hints and generation floors. Fresh attachment resets
framing. Existing explicit replacement keeps its return contract; optional-session
reclamation represents a previously released source without fabricating an owner.

Evidence:

- build.log: initial native-only build.
- host.log: initial status-reporting failure after a malformed completion; the
  pending original ticket was retained, but status incorrectly said Connecting.
- host-final.log:22 native-only discovery tests after that status fix.
- network.log: initial TCP fixture compile error; no runtime pass.
- network-final.log:14/15 QUIC tests, failing same-peer source replacement with
  Overloaded because the old source retained its lease. No TCP result claimed.
- network-lease.log:15 QUIC tests pass after session retirement;24/25 remote
  tests pass. The remaining assertion expected a retained session after automatic
  retirement and was updated to check explicit optional-session reclamation.
- clippy-all.log / clippy-all-final.log: development diagnostics for a large
  construction error and a checked Option unwrap. The final API returns a boxed
  rejection containing both original owners; the test uses checked extraction.
- all.log:245 all-feature tests pass across connect(19), discovery(9),
  effect_owner(176), quic_connect(15) and remote_discovery(26), including the
  existing native Node reconnect/reopen history. This precedes only the additional
  host test of automatic source repair through outer connector polling.
- all-final.log:42 final overlapping all-feature tests pass:27 remote discovery
  and15 QUIC connection tests, including the added outer-connector test.
- default.log:55 default-feature tests pass:19 connect,9 discovery,27 remote.
- native-only.log:33 tests pass:9 discovery and24 remote, with no TLS dependency.
- core.log:7 discovery tests pass with all default features disabled.
- checks-final.log: formatting and all four strict all-target Clippy profiles
  pass with zero diagnostics. Earlier lint logs are retained separately.
- inventory.log:108 inventory paths pass. metadata.log:13 obligation metadata
  checks pass. This adds a documented seam, not another reviewed obligation set.
- source.sha256 / source-check.log: final changed-source/evidence fingerprints.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test remote_discovery --test quic_connect --test connect --test discovery --test effect_owner
cargo +stable test --locked --offline --all-features --test remote_discovery --test quic_connect
cargo +stable test --locked --offline --test remote_discovery --test discovery --test connect
cargo +stable test --locked --offline --no-default-features --features native --test remote_discovery --test discovery
cargo +stable test --locked --offline --no-default-features --test discovery
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

Runtime feature configurations run sequentially. Native socket tests use loopback
network permission. The shared TCP/TLS and QUIC scenario uses the original source
connector, refuses a lower-generation replacement hint, accepts a newer hint,
keeps an unrelated cached peer available and closes with an outstanding lookup.
Host tests cover exact tickets, malformed bindings/rejections, closed sessions,
clock/budget rejection, constructor return, finite retries and delayed terminal
receipts. TCP dial workers are explicitly joined after reclaim.

These are selected Linux histories, not macOS/separate-host acceptance, recursive
parent-offline composition, executable source provisioning, power-loss evidence
or full P0–P7 completion. Host-managed generation allocation and fresh recovered
store sessions remain required. Source endpoints and pins are provisioned;
cache/floors are volatile. No performance claim is made.
