# Slice207b1 — durable peer startup binding

Starting revision6a81b08. Native static/member/multi-group startup can install
generation-guarded TCP/QUIC connectors and verify a host-loaded credential record
before socket binding or store opening. Material digests cover exact TLS bytes,
wire selection and ordered peer identities/certificates/names. The existing
credential journal format is unchanged. The host still loads files, serializes
the journal writer, persists prepared replacements and handles uncertainty.

Two three-node, three-group histories change actual node2/node3 keys and pins,
preserve original Counter receipts and new writes, then reopen all replicas.
The QUIC history checkpoints before reopening. A second durable rotation is
published in memory on only two replicas; restart recovers the original record
and keys on all three. Startup rejects eight wrong owner/request/generation/
digest cases before touching an absent directory or deliberately occupied port.
Other checks cover the material digest, static/member startup, and late
TCP/QUIC assembly failure with socket/worker/store reclamation.

This does not implement executable peer preparation/status commands, automatic
rollout coordination or protection against a trusted host discarding the
journal. Missing records represent an explicitly trusted initial generation.
Broader platform and fault acceptance remain open.

Executed evidence:

- rotation.log: initial4 startup/digest/native recovery checks pass.
- selection-error.log: an incorrect `--test tls` target is rejected before
  execution; corrected to the actual `secure` target.
- all.log:115 tests pass across connect15, credential_refresh9,
  native_member_startup50, peer_rotation7, secure14 and startup30. This run
  precedes the final two late-cleanup tests; production code is identical.
- rotation-final.log: all7 final startup rotation/digest/cleanup tests pass.
- default.log:41 tests pass across peer_rotation5, secure14 and startup22,
  including the final default-feature cleanup case.
- clippy-initial.log: strict all-feature lint passes during implementation.
- pre-push.log: formatting and strict Clippy pass for default, all features,
  no default features, and native-only configurations.
- inventory.log:106 contract entries validate; metadata.log:13 obligation
  metadata checks pass. The5-contract obligation ledger still leaves101
  interface entries unreviewed; metadata validation is not conformance proof.
- source.sha256 / source-check.log: final source and selected evidence identity.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test startup --test peer_rotation --test secure --test native_member_startup --test connect --test credential_refresh
cargo +stable test --locked --offline --all-features --test startup peer_rotation -- --nocapture
cargo +stable test --locked --offline --test startup --test peer_rotation --test secure
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
sh .githooks/pre-push
```
