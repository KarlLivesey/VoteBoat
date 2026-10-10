# Slice167 — maintenance during bounded recovery pressure

Base `4063f19c4fdf3cf997891c432a5e5c1fc3ede3e8` plus this commit.
No wire/persistent format, quorum rule or durability-token change.

## Changed behavior and direct evidence

Retryable snapshot preparation refusal now releases unused speculative owner
reservation, retaining actual effects and their exact leases. A later attempt
reacquires the original continuation floor and full image allowance before
allocation or provider submission. Two deterministic router tests exercise
competing leases, real capacity refusal, completion, zero final reservation and
worker rejection/retry. Accepted worker work keeps its complete charge.

An ordinary snapshot rejected before outbound admission is retried for a
construction-selected host-clock interval (default50ms, range0..60000). Continued
overload then discards that unaccepted packet and releases its group visit; the
existing heartbeat path regenerates it. This does not acknowledge peer receipt
or durability. Already accepted work, non-snapshot sends and learner-repair sends
retain their previous ownership. A preallocated ledger is bounded and charged by
the driver's lease capacity. Unit tests check exact expiry, zero delay, integer
limits, ticket replacement and cleanup; a host test checks construction limits
without mutating the owner.

Both new native histories force eight stale groups beyond their actual durable
suffixes, close every transport, and reopen real files. While one accepted
recovery receipt remains held, the healthy majority must apply a foreground
write, durably checkpoint it and complete a subsequent physical reclaim. Resume
must install snapshots for every stale group; another reopen must preserve
values and original operation retries. Every observation checks checkpoint and
recovery count/byte caps. Final native log reports TCP9/QUIC11 snapshot installs,
peak recovery1, positive reclamation and23 QUIC send discards. These are finite
test observations, not throughput, bandwidth, fairness or latency guarantees.

## Commands

```sh
cargo +stable test --locked --offline --no-default-features --test effect_owner
cargo +stable test --locked --offline --no-default-features --lib runtime::snapshot_send
cargo +stable test --locked --offline --all-features --test effect_owner
cargo +stable test --locked --offline --all-features --example native_benchmark -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service native_configuration_close_deadline_and_unread_commit_recover
cargo +stable test --locked --offline --all-features --test counter_service
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable fmt --all -- --check
RUSTDOCFLAGS='-D warnings' cargo +stable doc --locked --offline --all-features --no-deps
node validation/check-inventory.mjs
```

Current focused results:146/146 core-only owner tests,152/152 all-feature owner
tests,2/2 retry-ledger unit tests and22/22 native benchmark tests pass. The final
native benchmark target took8.99s. Both corrected configuration histories pass
(18.27s) and both corrected joint-retirement histories pass (12.56s). Strict
Clippy in both profiles, formatting, warnings-denied API docs and91-record
inventory metadata validation pass. `*-current.log` files identify these runs;
`native-retry-final.log` is the final native composition run.
The final complete service target passes45/45 in43.52s, retained in
`service-all-corrected.log`; earlier unsuccessful whole-target runs remain
separate. `source-sha256.txt` records the tested implementation and fixture files.

## Retained failed observations and corrections

`maintenance.log`, `quic-diagnostic.log`, `quic-queues.log` and
`maintenance-fixed.log` preserve the original reservation deadlock and the
second blocked outbound-image path. The history was not weakened to accept
blocked foreground work. `owner-all-final.log` records four failures with
immediate image discard: brief queue bursts at fixed host time needed the
bounded retry grace, now covered directly.

`build.log` and `reservation-host*.log` retain initial test module-path/type
mistakes and a fixture whose budget did not actually cause the asserted
capacity refusal. The corrected fixture derives its budget from actual owner
reservation and explicitly requires refusal before eventual progress.

`native-owner-service.log` and `service-history-failure.txt` preserve a stale
cached-leader assumption in the faulted Counter recorder. It now refreshes the
leader, retains the same operation/payload and leaves every attempted reply
unread before the crash. The independent checker and corruption rejection stay
intact. `service-retry-final.log` and `configuration-leadership-failure.txt`
preserve a setup configuration request receiving `LeadershipChanged`. The
explicit-node retry preserves the original record and bounds retries; the
unsupported automatic-client attempt is retained in
`service-configuration-current.log`.

`service-all-current.log` preserves a later joint-retirement client write sent
to a cached leader after it became a follower. Post-transition application
writes/reads now use the existing bounded automatic client route with identical
operation IDs; the explicitly targeted leader-demotion command, unread reply,
joint/final durable records and all-node recovery assertions remain unchanged.

`previous-ci-failure.log` is previous-commit4063f19 Linux CI: one hundred-group
automatic checkpoint history timed out. Its current local owner-suite run
passes, but no remote resolution or macOS success is inferred. New CI remains
background feedback. Full P0–P7, general retention, incremental cleaning,
generated fault coverage and wider platform/device validation remain open.
