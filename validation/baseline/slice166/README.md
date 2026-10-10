# Slice166 — bounded automatic checkpoint progression

Base325b887c2e46b680b024cc02b10a4127b0923af9 plus this commit. No wire/disk
format, quorum rule or durability-token change. Native snapshot and log workers
remain the only selected providers.

## Commands and results

```sh
cargo +stable test --locked --offline --no-default-features --test effect_owner
cargo +stable test --locked --offline --all-features --test effect_owner node_facade::automatic_checkpoints
cargo +stable test --locked --offline --all-features --test effect_owner snapshot_routes
cargo +stable test --locked --offline --all-features --test effect_owner native::automatic_hundred_group_checkpoints_reclaim_and_reopen
cargo +stable test --locked --offline --all-features --test log_reclaim --test snapshot --test snapshot_worker
cargo +stable test --locked --offline --all-features --test counter_service maintenance -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service
cargo +stable test --locked --offline --all-features --test counter_service maintenance::automatic_checkpoints
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable fmt --all -- --check
rustfmt +stable --edition 2021 --check tests/support/native_checkpoints.rs
node validation/check-inventory.mjs
```

143/143 core-only effect-owner tests,6/6 selected automatic Node tests with all
features,11/11 snapshot-router tests,8/8 log-reclaim,21/21 snapshot and14/14
snapshot-worker tests pass. The native100-group test passes in14.79s after its
fixture helper cleanup. All45 service tests pass in41.09s. The final focused
service run checks the two automatic checkpoint histories again after the last
production guard change. Strict profiles and formatting pass; inventory metadata
verifies91 records. Logs are kept alongside this file.

## Evidence scope

The Node policy bounds groups examined per interval and total queued/executing
requests, preserves exact tracked admission scope and skips pending/busy groups.
An ordered map cursor avoids repeatedly scanning the already-visited prefix.
A request is completed only after its successful execution is observed and the
durable local snapshot base reaches its target. Manual checkpoints use the same
path; a manual winner can reject the queued automatic request with NotApplied.
That slot is released without a false automatic completion. Clock/policy errors,
unsupported providers, held work, cross-group progress, failure ownership and
shutdown are tested through public injected providers.

The new native history checkpoints100 groups on each of three real replicas,
checks every base against the applied boundary, requires positive physical byte
reduction, drains and joins workers, reopens actual files, retries original
operations and performs a new write. The service histories use TCP/TLS or QUIC,
set both automatic options, wait for two checkpoint waves and inspect nonzero
bases in the actual stopped stores before restarting. They never send the manual
checkpoint command. Native/model storage suites independently exercise the
unchanged publication, pinning, receipt-loss and replacement failure mechanisms.
This does not enumerate every automatic schedule/crash combination or model
physical devices. General retention, incremental cleaning, fixed disk occupancy
and latency guarantees remain work. CI state is from the previous commit only;
no new macOS or separate-host test result is claimed.

## Corrected development checks

host-fixture-failure.log preserves a failed attempt to order a manual and automatic
checkpoint by assuming which traffic class a single group would visit first.
The runtime intentionally rotates classes. The final fixture uses the host's
specified FIFO group ordering to consume another group's visit first, then checks
the actual queued manual winner and automatic rejection. No production scheduling
rule was changed to satisfy it.

clippy-helper-failure.log preserves a test-only moved-borrow compile error while
splitting the native fixture for formatting. The final helper explicitly borrows
its mutable slice for each iteration. All lint levels and bounds remain intact.
