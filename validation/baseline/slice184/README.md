# Slice184: remote manifest discovery

Source base: 75c8239 plus this slice. Linux local validation; no new macOS or
separate-host result is claimed. All native transport tests use loopback.

| Command / evidence | Result |
| --- | --- |
| `cargo +stable test --locked --offline --all-features --test remote_manifest --test lookup_discovery --test remote_discovery` (`discovery.log`) | 7 remote-manifest, 3 original-read, 14 endpoint-discovery tests pass |
| `cargo +stable test --locked --offline --all-features --test routed automatic_lookup` (`native-routing.log`) | 6 pass, including 2 new TCP/TLS and QUIC remote-manifest histories |
| `cargo +stable test --locked --offline --all-features --example native_benchmark automatic_maintenance_progresses_with_held_snapshot_recovery` (`benchmark-regression.log`) | Both TCP and QUIC pass after feature-gating the optional variant |
| `cargo +stable test --locked --offline --test remote_manifest` (`default-host.log`) | 7 pass with default features |
| `cargo +stable test --locked --offline --test remote_manifest --test routed remote_manifest` (`default-native.log`) | Native TCP test passes; this name filter selects zero host cases, which the preceding command covers separately |
| `cargo +stable test --locked --offline --no-default-features --features native --test remote_manifest --test lookup_discovery` (`native-only.log`) | 7 remote-manifest plus 3 original-read tests pass without built-in TLS/QUIC |
| `cargo +stable fmt --all -- --check` (`fmt.log`) | Exit 0 |
| Strict all-target Clippy with default, all and no-default features (`clippy-*.log`) | Exit 0, zero diagnostics in each profile |
| `node validation/check-inventory.mjs` (`inventory.log`) | 95 existing contracts and conformance paths validate; this extends C17, not a new replaceable contract |
| `sh .githooks/pre-push` (`hook.log`) | Formatting and all three strict Clippy profiles exit 0 |

The first native assertions expected two cached manifests instead of the actual
root/service/child path, and immediate release of the original Node credit after
selected-wait cancellation. The final tests explicitly verify all three fetched
levels and drain the original request before shutdown. The original read API and
ownership contract were not weakened. A first regression invocation without
socket permissions failed in the existing endpoint TLS fixture before networking;
the complete regression command then passed with loopback access.

An extra default-feature compile check found an existing unguarded reference to
the optional QUIC variant in the held-recovery benchmark. A feature-gated match
preserves TCP and QUIC behavior. The default profile is now included in the push
hook and background CI alongside all-feature and no-default-feature checks.

The remote protocol carries authorized source observations, never a reconstructed
ReadBarrier or owner/membership authority. It uses an explicitly provisioned
source/authority and bounded volatile cache. Tests cover malformed and oversized
wire input, delayed/short I/O, exact request matching, stale generations, cache
capacity, cancellation/timeout, source replacement, original read cleanup and
child service with metadata offline. These finite cases are not a full distributed
proof, executable discovery integration or general deployment certificate.

The earlier full-suite run started before slice175 remains separate historical
evidence, still running when this slice was prepared; it is not validation of
this revision. No test was restarted merely because observation timed out.
