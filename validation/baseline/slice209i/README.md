# Slice209i — executable source for exact Raft peer identities

Starting revision: 2f8bb5a.

The counter's authenticated discovery responder accepts the explicit
voteboat-peer-discovery-v1 format. It binds each endpoint to an exact node,
store ID and store incarnation. Existing command discovery formats retain
their separate synthetic service identities. Files are bounded to16KiB and64
targets, with exact identity/address uniqueness and nonzero generations.
Address updates preserve identities/names and remain explicitly volatile;
restart loads the operator's separately saved file.

Evidence:

- unit-initial.log: initial compile refused StoreIdentity as a BTreeSet key
  because it does not implement Ord. The implementation now uses the existing
  ordered store ID/incarnation tuple; no public identity contract changes.
- unit.log / unit-final.log:7 source tests pass, including two new cases for
  peer identity matching, updates/reconstruction and malformed/bounded input.
- service-all.log:11 all-feature discovery service histories pass. The new
  public native-client consumer checks exact peer replies, wrong stores and
  incarnations, command-identity confusion, authorized/denied address updates,
  exact retries, unchanged input files, saved-source restart and preserved
  replicated counter retries. The source channel is TCP/TLS in both histories;
  Raft peers use TCP/TLS or QUIC.
- default.log:7 source and8 service tests pass in the default build.
- counter-unit.log:28 tests pass and8 loopback tests fail because the sandbox
  denied binding. counter-unit-sockets.log reruns with local socket permission:
  all36 counter unit tests pass. No code change was needed for these failures.
- clippy.log / checks.log: strict all-feature Clippy, then formatting and all
  four strict Clippy profiles pass with zero diagnostics.
- inventory.log:108 contract paths pass; metadata.log:13 obligation metadata
  checks pass. This is an extension of the existing C17 implementation, not a
  new provider or completed obligation review.
- source.sha256 / source-check.log: changed source/evidence fingerprints.

Commands:

```sh
cargo +stable test --locked --offline --all-features --bin voteboat-counter command_discovery
cargo +stable test --locked --offline --all-features --test counter_service command_discovery:: -- --nocapture
cargo +stable test --locked --offline --bin voteboat-counter --test counter_service command_discovery
cargo +stable test --locked --offline --all-features --bin voteboat-counter
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

Runtime feature configurations ran sequentially. These are Linux loopback
histories; macOS and separate-host acceptance remain open. The peer-source
format can serve Rust hosts, but automatic consumption by counter Raft startup
and combined recursive movement remain next work. No persistent endpoint
publication, membership authority, transport pin installation or consensus
format change is claimed. The full P0–P7 goal remains active.
