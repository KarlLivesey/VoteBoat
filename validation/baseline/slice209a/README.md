# Slice209a — authenticated unchanged-endpoint lease renewal

Starting revision03a4849. The remote endpoint client previously published every
authenticated response through the strict local cache API. A fresh TTL at the
same generation therefore conflicted even when the peer/address was unchanged.
The executable's immutable source uses exactly this generation/lease pattern.

The native cache now has a private revalidation path used only by the remote
client after its current session/request/sequence/deadline/cancellation checks.
Same-generation renewal requires an unchanged peer and endpoint. A changed
endpoint still needs a higher generation. The public publish API remains strict.
Expiry is still calculated from local request submission, not response arrival.
No new public contract, background worker, wire format or durable state is added.

Evidence:

- before.log: the new short-I/O regression fails on its first expired lease with
  `Discovery(ConflictingGeneration)` before the production fix.
- all.log: initial affected all-feature selection passes after the fix.
- final-all.log:54 tests pass across connect, discovery, quic_connect and
  remote_discovery, including the added replay and replacement checks.
- default.log:41 affected default-feature tests pass.
- native-only.log:20 discovery tests pass with native but no TLS/QUIC features.
- core-only.log:3 public discovery-wrapper tests pass without native providers.
- checks.log: formatting and strict Clippy pass for default, all-feature,
  core-only and native-only profiles with zero diagnostics.
- inventory.log / metadata.log:106 inventory entries and13 metadata checks pass;
  these are metadata checks, not runtime certification.
- source.sha256 / source-check.log: final changed source/evidence fingerprints.

One shared scenario runs through short-I/O host, real TCP/TLS and QUIC sessions.
It repeatedly expires/invalidates leases at capacity1, renews without changing
generation, moves to a newer-generation endpoint, rejects same-generation address
conflicts and older generations, suppresses cancelled renewal, then retries.
Host tests additionally replay an old positive response during a new refresh and
renew through a replaced source session while preserving the generation floor.
Public cache tests confirm unchanged strict expiry/publication semantics.

Commands (runtime feature configurations ran sequentially):

```sh
cargo +stable test --locked --offline --no-default-features --features native --test remote_discovery short_io_authenticated_endpoint_leases -- --nocapture
cargo +stable test --locked --offline --all-features --test remote_discovery --test discovery --test quic_connect --test connect
cargo +stable test --locked --offline --no-default-features --features native --test remote_discovery --test discovery
cargo +stable test --locked --offline --test remote_discovery --test discovery --test connect
cargo +stable test --locked --offline --no-default-features --test discovery
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

Socket histories had loopback access. These are bounded Linux tests, not macOS
or separate-machine acceptance. This fixes lease renewal in a persistent client;
it does not add disk-persistent cache floors, automatic Node refresh driving or
the remaining discovery restart/parent-offline integration. Full P0–P7 remains
active with those209 integration requirements still open.
