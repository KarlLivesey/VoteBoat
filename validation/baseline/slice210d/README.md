# Slice210d — bounded disconnected output restores survivor progress

Temporary session instrumentation reproduces both210b QUIC failures (5 pass,
2 fail). The healthy survivor connection remains Ready and receives increasing
traffic while the source stops sending application streams. Additional owner
counters reproduce one failure in the original seven-history workload (6 pass,
1 fail): the source retains256 batches for the offline voter and three rejected
Send leases, pinning all three local groups. An isolated owner-instrumented
history passes; it does not erase the failed parallel workload. Exact diagnostic
patches and failed service text logs are retained. Production instrumentation
has been removed; credentials and native data stores are not included here.

The correction keeps initial connection staging and quick-reconnect ownership,
but bounds unsent retention after the first observed connection failure through
PeerDriverLimits::disconnected_send_retry_ms (default500ms, maximum60s). Repeated
failed attempts do not extend that interval; authenticated attachment resets it.
The caller sees its deadline for relevant staged output. At expiry, bounded
visits complete original outbound tickets as Failed, releasing exact queue
credits. Transport-owned work and connector terminal slots preserve their own
lifetimes. No local failure establishes remote delivery, commitment or durability.
Raft retries through the existing authenticated generation/term checks.

## Executed evidence

- `before-peer-final.log`: the new host regression fails before correction;
  the old driver omits the deadline and retains saturated disconnected output.
- `peer-core-final-complete.log`:31 peer-driver host histories pass, including
  exact failed tickets, credit release, repeated-failure expiry, healthy traffic,
  fresh-generation recovery and a reset interval after authenticated reconnect.
- `handoff-after-final.log`:7 native TCP/QUIC histories pass under the original
  deadlines and CI-temp layout, including both previously reproduced failures.
- `counter-maintenance-all.log`: full counter156 and maintenance6 pass.
- `runtime-all-native.log`: effect_owner178, outbound4, peers18 and runtime28 pass.
- `handoff-default.log`: all4 default TCP handoff histories pass.
- `clippy-first.log`: strict all-feature Clippy passes; final formatting and all
  four profiles are recorded in `pre-push.log`.
- `inventory-final.log`:108 contract inventory entries pass metadata checks.

The initial runtime selection used a nonexistent target and did not run; the
corrected sandboxed selection hit socket PermissionDenied in8 native cases.
`runtime-all-native.log` is the actual socket-enabled result. The first diagnostic
build used a nonexistent RuntimeOwner.node field; the corrected instrumentation
uses store identity. The first host-reset history withheld a pending connector's
terminal receipt during cleanup; it now explicitly permits that separate receipt
before drain. These corrections do not change production failure policy or raise
deadlines. `before-peer.log` selected the wrong integration target (0 tests);
only `before-peer-final.log` demonstrates the failing-before regression.

`ci-prior-current.json` and `ci-prior-failures.log` retain run38044091607 at
8cc6852: Ubuntu154 passes/2 failures and macOS141 passes/15 failures. These are
pre-fix results. Supported-platform feedback for this correction, remaining
operator failures and the full P0–P7 baseline stay open.

Reproduce the meaningful checks:

```sh
cargo +stable test --locked --offline --no-default-features --test effect_owner peer_driver::
cargo +stable test --locked --offline --all-features --test counter_service group_leadership::
cargo +stable test --locked --offline --all-features --test counter_service --test leadership_maintenance
cargo +stable test --locked --offline --all-features --test effect_owner --test peers --test outbound --test runtime
cargo +stable test --locked --offline --test counter_service group_leadership::
sh .githooks/pre-push
```

Run executable feature configurations sequentially. Socket histories require an
environment permitting local TCP/UDP listeners; the deterministic host regression
needs no sockets.
