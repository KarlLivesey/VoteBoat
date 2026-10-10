# Slice207a — prepared peer-key replacement

Starting revision8bafd10. Optional PeerCredentialControl forwards through the
public Node/PeerDriver boundary. RotatingPeerConnector wraps the existing native
connector with fixed-generation session leases and bounded accepted-ticket
tracking. The native material provider replaces keys/pins for the exact same
identities, addresses, wire version and connection-generation counters.

Actual TCP and QUIC histories exchange data, change certificates/keys while
preserving node/store identity, reject old sessions and stale pins, then reconnect.
Invalid material and stale expected generations leave the old session usable.
Controlled host cases verify late successful completion is revoked, capacity
is not released early, rejected input/constructor ownership is retained, and
dropping the wrapper revokes its sessions. A core-only Node case checks forwarding,
unchanged consensus state and shutdown refusal.

No durable rollout or executable integration is claimed. Hosts load/authorize/
persist prepared material before calling the API and reconstruct it at restart.
Only sessions established through the wrapper carry its leases. These are
connector/data-exchange tests, not full Raft recovery after credential rotation.
207b adds that next composition and its restart evidence.

Executed evidence:

- initial-clippy.log: initial production mechanism compiles with strict all-feature Clippy.
- rotation-initial.log: initial test compilation catches the wrong identity
  type and an unused import; corrected in place.
- rotation.log: four initial rotation/host tests pass.
- rotation-final.log: six tests pass with stale-pin and invalid-material cases.
- all.log:215 passing all-feature connect, credential_refresh, effect_owner,
  peer_rotation and quic_connect tests, including seven rotation tests.
- node-core.log: core-only Node forwarding/shutdown test passes.
- default.log:15 connector and5 TCP/host rotation tests pass.
- rotation-verified.log: all7 final rotation tests pass with strengthened
  independent-owner and stale-connection-generation assertions.
- clippy-tests.log, pre-push.log, pre-push-final.log: strict checks pass; the
  final log covers formatting plus default/all/core/native-only Clippy.
- inventory-initial.log: metadata validator rejects an entry in the wrong array.
- inventory.log / metadata.log: corrected106-contract inventory and13 obligation
  metadata tests pass; metadata alone is not conformance certification.
- source.sha256 / source-check.log: final source/evidence identity.

```sh
cargo +stable test --locked --offline --all-features --test peer_rotation --test connect --test quic_connect --test credential_refresh --test effect_owner
cargo +stable test --locked --offline --test peer_rotation --test connect
cargo +stable test --locked --offline --no-default-features --test effect_owner node_forwards_prepared
cargo +stable test --locked --offline --all-features --test peer_rotation
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
sh .githooks/pre-push
```
