# Slice209g — prepared native startup for host discovery

Starting revision: 30df74ca88469ce732174b71ec515fe895e47f8e.

NativeStartup::prepare_for_discovery performs the same static native validation,
bootstrap/recovery, application replay and bounded worker setup as ordinary
startup, returning public NativeNodeParts before final Node assembly. Hosts can
provision a source using the recovered local identity and wrap the original
connector with DiscoveryConnector. QUIC discovered Dial addresses are explicitly
enabled; configured Accept addresses, pins and identities remain unchanged.
Existing open methods preserve their configured routing and cleanup contracts.

Preparation opens resources and may advance a recovered store session. It does
not poll peer connections or admit Node work. Node::from_parts still validates
the complete assembly and returns original parts on rejection. Successful
preparation transfers all resources to the caller for assembly or explicit
close/join; failed preparation retains the ordinary startup cleanup handle.

Evidence:

- clippy.log: first compile diagnostics identified that NodeRejected.parts is
  boxed. The shared cleanup helper now consumes the unboxed owned parts.
- clippy-fixed.log: strict all-feature check passes after that correction.
- prepared.log: both initial cluster tests fail because the fixture had not
  created the parent directory for three replica directories. clippy-tests.log
  also records the then-unused PeerConnector import. The fixture creates its
  parent; the import is used by the later explicit cleanup check.
- prepared-fixed.log: all five new tests pass. TCP/TLS and QUIC clusters use
  correct host-discovered endpoints despite stale configured Dial defaults;
  original receipts/deduplication survive WAL reopen with fresh store sessions.
  Other tests check invalid timing/missing recovery, unchanged source/parts after
  final assembly rejection, and explicit abandoned-part worker/listener cleanup.
- startup-all.log: all50 native-member and37 startup tests pass, including the
  existing multi-group, checkpoint, peer-credential and failed-startup paths.
- default.log: all26 default-feature startup tests pass independently.
- clippy-final.log and checks.log: strict all-feature Clippy, then formatting
  plus default/all-feature/core-only/native-only strict Clippy pass with zero
  diagnostics.
- inventory.log:108 contract paths pass; metadata.log:13 obligation metadata
  checks pass. This is a new operation on the existing startup contract, not a
  new provider or completed provider-obligation review.
- source.sha256 / source-check.log: changed source and evidence fingerprints.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test startup discovery:: -- --nocapture
cargo +stable test --locked --offline --all-features --test startup --test native_member_startup
cargo +stable test --locked --offline --test startup
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

Feature configurations ran sequentially with local socket permission. These
are Linux startup/reopen histories, not physical power-loss, macOS or separate-
host acceptance. Member/multi-group convenience preparation, automatic
executable source-session bootstrap, combined recursive movement and the wider
P0–P7 goal remain open. No durable discovery cache or trust distribution is added.
