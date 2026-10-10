# Slice163 — combined shared-provider lifetime

Base `f669f7c9b3fb41d58635dfb7150218cdad511c5d` plus this commit.
No production Rust, protocol, persistent format or dependency changes.

## Validation

```sh
cargo +stable test --locked --offline --all-features --test transport --test admission --test buffer
cargo +stable test --locked --offline --no-default-features --test transport --test admission --test buffer
cargo +stable test --locked --offline --all-features --test transport shared_lifetimes
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable fmt --all -- --check
node validation/check-inventory.mjs
```

All-feature profiles:28 transport,8 admission,13 buffer tests pass. Core-only:
1 transport,1 admission,6 buffer pass. The final two selected lifetime tests
pass with explicit original-ticket checks. Both strict Clippy profiles report
zero diagnostics, formatting passes and89 inventory records validate. The
inventory check is metadata/path validation, not consensus conformance.

## What the new history checks

The same bounded history selects either the independent downstream HostPolicy
and HostPool or the native AdmissionPolicy and BufferPool. Both are composed
through their public traits into two native outbound/transport instances. The
host SecureSession uses deterministic trusted test attestation and seven-byte
partial I/O; this is not new TLS/QUIC or real-network validation.

A bulk reservation leaves one bulk frame and protected control capacity. The
first connection delivers data but remains locally unflushed, retaining frame
and admission credits. The sibling's bulk submission returns its exact message
unchanged, while a control message is delivered and its local completion is
consumed. The original data still holds the sole shared admission token.

Aborting the first connection returns a Failed completion and releases frame
capacity. Its admission credit stays with that completion after the old queue
closes and drops. A replacement queue generation refuses the stale completion,
returning its original body and ticket without releasing credit. Dropping that
returned completion releases the token; the sibling then sends and completes
data. Closing/dropping separate provider views never closes those siblings.
Final shared byte/lease and admission usage are zero.

## Scope

This supplies combined ownership/lifetime evidence beyond the prior isolated
buffer/admission tests. It does not implement general client/disk/connection
admission or establish latency guarantees, cryptographic security, actual disk
failure behavior or complete P0–P7 acceptance. The previous slice's CI was
observed active on Linux/macOS with lint passed; previous-ci.json records that
observation rather than a completed platform result.
