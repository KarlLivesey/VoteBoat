# Slice186a: failure causes and native shutdown clocks

The original macOS failure excerpt remains in
`../slice185/macos-1b460ef-failures.log`. This slice does not establish its storage
failure's underlying cause or claim a successful macOS rerun.

## Reproductions and fixes

- `storage-cause-before.log`: one core-only injected storage-failure regression
  fails because connection accounting masks the exact cause as Runtime(Fenced).
- `storage-cause-after.log`: the same test passes; original error, fencing,
  unchanged durable term and retained connection history are checked.
- `node-storage-cause-after.log`: exact failure reaches Node recovery, while
  accepted client work remains Unknown and the application stays unchanged.
- `shutdown-clock-before.log`: a QUIC history starting at protocol time20s fails
  when single-node cleanup polls at the former fixed time10s.
- `shutdown-loop-before.log`: after correcting that first call, final cluster
  shutdown independently fails because it resets time to10s.
- `native-member-frozen-cleanup.log`: initial full-target validation (49 pass,
  1 fail) reveals a fixed cleanup clock preventing QUIC's closing timer from
  advancing. The failure records Draining with zero owner/worker/output usage.
- `shutdown-clock-after.log`: focused late-clock history passes after removing
  clock resets. `native-member-after.log` is the final full run after additionally
  advancing the cleanup clock: all50 tests pass (3.39s).
- `runtime-worker-after.log`: all-feature effect-owner156, runtime25 and worker9
  tests pass, including native storage/transport and failure/ownership checks.

No production timeout was enlarged, and no error was ignored. Tests retain the
five-second shutdown bound. The production change preserves callback results
when the callback fences its core, without releasing its connection history.

## Commands

```sh
cargo +stable test --locked --offline --no-default-features --test effect_owner connection_budget::failed_storage_keeps_its_cause_and_connection_history_until_recovery -- --exact --nocapture
cargo +stable test --locked --offline --all-features --test effect_owner node_facade::provider_failure_fences_node_and_returns_unknown_with_original_selected_resources -- --exact --nocapture
cargo +stable test --locked --offline --all-features --test native_member_startup quic_promoted_leader_loss_requires_retained_restored_witness -- --exact --nocapture
cargo +stable test --locked --offline --all-features --test native_member_startup -- --nocapture
cargo +stable test --locked --offline --all-features --test effect_owner --test worker --test runtime
cargo +stable fmt --all -- --check
cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
node validation/check-inventory.mjs
```

Native checks ran on Linux with loopback sockets and actual file stores. These
are finite histories, not power-loss or general distributed-protocol proofs.
Lint logs and the inventory result are retained separately; formatting produces
an empty log on success. The enabled pre-push hook reruns formatting and all
three strict lint profiles. Fresh macOS confirmation remains pending.
