# Slice209d — child restart with every metadata replica offline

Starting revision: d765cb42d0dbbc1910aeaf067754c00eb7cf08fe.

No production API, protocol or persistence change. Four new native histories
exercise TCP/TLS and QUIC with WAL recovery or installed checkpoints. Initial
three-level root/service/child routing uses the real authenticated remote
manifest protocol backed by original Directory quorum reads. Metadata/source
owners then drain, close and join; their complete files are captured.

Each child is closed and reopened twice with a fresh local store session,
application object and routing cache while all metadata stays offline. Before
campaigning, assertions check restored initialization, application value, exact
grant and checkpoint base. Root resolution with a lookup budget actually reaches
the closed remote source and fails Closed, before and after admitting the local
child grant. Direct child routing uses zero lookups and a source that panics on
any call. Original receipt values and deduplication survive, fresh writes reach
10 then15, quorum reads serve current values, and stale epoch routes refuse.
Every stopped metadata file remains byte-for-byte unchanged at the end.

Evidence:

- native.log / clippy.log: initial fixture compile diagnostics for using an
  unordered identity as an ordered-map key.
- native-run.log / clippy-run.log: subsequent compile diagnostics exposed that
  RuntimeOwner carries the store binding, not a NodeId. The final fixture matches
  the complete store identity in a bounded vector and checks a fresh session.
- native-corrected.log: four failures at the same test assertion. Original
  operation1 correctly returned its cached receipt value7 after a later write;
  the fixture incorrectly expected current value10. The assertion was corrected;
  current state remains independently checked through fresh quorum reads.
- routing-final.log:10 all-feature automatic/remote routing tests pass, including
  all four new histories and the six surrounding cancellation/routing tests.
- routing-source-check.log: all10 pass after strengthening cold root failure
  from a zero-lookup-budget refusal to the actual closed authenticated source.
- default.log: all five default-feature automatic/remote routing tests pass
  before that assertion strengthening.
- default-final.log: both final TCP child-restart histories pass afterward.
- checks-final.log: formatting and strict Clippy pass with zero diagnostics for
  default, all-feature, core-only and native-only configurations.
- inventory.log:108 inventory paths pass; metadata.log:13 obligation metadata
  checks pass. No additional seam or reviewed obligation set is introduced.
- source.sha256 / source-check.log: changed source and evidence fingerprints.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test routed child_reopens -- --nocapture
cargo +stable test --locked --offline --all-features --test routed automatic_lookup -- --nocapture
cargo +stable test --locked --offline --test routed automatic_lookup -- --nocapture
cargo +stable test --locked --offline --test routed child_reopens -- --nocapture
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

Runtime feature configurations run sequentially with loopback permission. These
are selected Linux close/reopen histories through native files/workers/sockets,
not abrupt process kills, device power-loss tests, independent-machine or macOS
acceptance. The three-level manifests share one metadata authority; moving
multiple authorities and combining long-lived endpoint-source reconnection with
this history remain separate work. Endpoint provisioning, volatile-cache restart
policy and the full P0–P7 goal are not declared complete.
