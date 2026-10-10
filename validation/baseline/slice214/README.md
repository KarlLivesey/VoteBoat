# Slice214 — observe liveness failure without changing its outcome

The existing metrics command now reports current driver/roster/outbound scalar
usage and at most8 peer binding/queue details, with explicit omitted count.
Existing counter fields remain numeric; new live gauges can decrease. The
snapshot uses the existing public providers and performs no I/O, admission or
completion. It is local volatile state, not quorum evidence. Maximum rendering,
including a pessimistic1KiB allowance for old fields, fits the unchanged4KiB
client reply buffer. No new interface seam, crypto or timeout change.

Counter fixtures capture selected group status plus node metrics/timings after
a failed direct/routed command, authenticated write, group election deadline or
administration preparation deadline. They keep the original failed result and
never repeat its write. The shared3s diagnostic deadline covers fixture-gate
waiting and child observation; at most8 nodes/24 read-only commands are issued.
Waiting caller processes are terminated and joined on expiry; each stdout/stderr
excerpt is capped at4KiB. Results are sequential post-failure observations,
not a cross-node atomic snapshot. The failure log is retained in its fixture.

Executed evidence:

- `metrics-bound.log` and `metrics-bound-default.log`: maximum-value renderer
  stays bounded, reports eight omitted peers and preserves numeric fields.
- `metrics-native.log`: actual TCP/QUIC traffic, original retries and cold
  recovery pass both metric histories; per-peer owned usage sums exactly to
  aggregate outbound usage. Old counters keep their monotonic checks.
- `counter-all.log`: full all-feature counter target passes162. This precedes
  final test-only iterator/owned-child corrections and additional failure-only
  hooks; it is not an exact final-source execution claim.
- `clippy-before.log`: one test chunks_exact finding. Use as_chunks and require
  empty remainder rather than relaxing lint levels.
- `diagnostics.log`/`diagnostics-final.log`: earlier scoped/deadline/cleanup
  selections pass; they precede the final owned-child test setup correction.
- `diagnostics-default-before.log`: the lock-contention test can exhaust the
  cleanup test's50ms budget before child admission. The probe correctly refuses
  to spawn; the cleanup fixture assumed it already owned a child.
- `diagnostics-default.log` and `diagnostics-owned-child.log`: all4 final default/
  all-feature checks pass after isolating the accepted-child phase. The50ms
  cleanup and gate deadlines remain unchanged. Original arguments, no write
  resubmission, shared deadline, bounded node count and actual child cleanup are
  independently checked.
- `configuration-pending.log`: actual TCP/QUIC pending-record recovery passes2
  after adding failure-only preparation diagnostics.
- `metrics-default.log`: default TCP metric/recovery history passes.
- `clippy-all.log`/`checks.log`: focused strict scan, then formatting and all four
  strict profiles pass with zero diagnostics.

Platform observations are previous-source evidence, not validation of this
diagnostic change. Run38047529334/629db60 macOS finishes145 pass/13 fail; Ubuntu's
passing counter158/directory19/transfer18 evidence is retained in slice213.
Run38047957086/33afe94 completes with Ubuntu counter157 pass/1 fail (original drain
handoff uncertainty) and macOS counter147 pass/11 fail, mainly shared-group QUIC
leadership/command stalls. Later targets on failed counter jobs were not run.
Raw job output and state are retained. No transport/root cause or final-platform
success is inferred from these failure counts.

This diagnostic slice supports the next focused liveness fix. Full P0–P7 remains
active, performance and platform gates open, P8/Windows deferred. Broad security
work is left to the user's Daybreak run.

Reproduce sequential feature builds in a socket-enabled environment:

```sh
cargo +stable test --locked --offline --all-features --bin voteboat-counter maximum_peer_observations
cargo +stable test --locked --offline --all-features --test counter_service
cargo +stable test --locked --offline --all-features --test counter_service configuration_pending::
cargo +stable test --locked --offline --all-features --test counter_service failure_diagnostics_
cargo +stable test --locked --offline --test counter_service metrics_are
cargo +stable test --locked --offline --test counter_service failure_diagnostics_
sh .githooks/pre-push
```
