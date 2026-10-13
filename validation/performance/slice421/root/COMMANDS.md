# Root final acceptance

Original runner51026 exits0 with37 exact compiled-source guards before/after.
Every15 recorded command exits0. Configuration and executable-using tests run
sequentially in `CARGO_TARGET_DIR=target/p9/slice421-checks` outside the restricted
localhost sandbox. Command/stdout/stderr/exit records are retained for each step.

```sh
cargo +stable test --locked --offline --all-features --test startup --test native_member_startup --test runtime --test effect_owner --test log_store --test maintenance --test leadership_maintenance -- --test-threads=1
cargo +stable test --locked --offline --all-features --test counter_service service_processes_retry_replace_leader_and_recover_native_files -- --test-threads=1
cargo +stable test --locked --offline --all-features --test counter_service maintenance:: -- --test-threads=1
cargo +stable build --locked --offline --all-features --example native_benchmark
.githooks/pre-push
```

Affected integration results are203+9+16+5+50+55+60=398 passed,0 failed. Real
process TCP/QUIC recovery passes2 and maintenance/checkpoint/reclaim passes6.
The full hook qualifies root formatting/default/all-feature/no-default/native
strict all-target Clippy and standalone matched formatting/strict Clippy.

Eight actual debug CLI histories use64 measured originals,64 warmups,window32,
startup or two-group shared assembly,TCP/TLS or QUIC,and `--ready-items 32|64`.
Each retains original samples,recovers total128,retries original IDs,joins workers
and prints the exact requested/effective selection plus unchanged64-entry Core,
1MiB/2ms visit caps. Summary and sample files are retained, without WAL stores.
These are functional controls; their incidental debug rates are not performance
comparisons. Offered-load propagation is checked by the shared configured closure
and focused parser/actual-shard getter controls, not a timed offered-load claim.

The initial selection command failed before compilation because `client_admission`
is a child of the `effect_owner` target, not a separate Cargo target. The original
failed command/error/runner are preserved in `initial-target-selection/`; removing
that nonexistent target ran the full owning target. No product source changed,
completed tests were not repeated, and no failing check was bypassed.
