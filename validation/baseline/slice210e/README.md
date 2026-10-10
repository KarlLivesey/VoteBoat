# Slice210e — replay the original pending configuration record

The preceding Ubuntu operator run rejected an actual response from the native
administration path: UNKNOWN exact record locally durable but not committed;
preserve original record. The fixture treated that documented uncertainty as a
permanent error. Two new real TCP/QUIC histories reproduce the refusal by taking
both other voters offline, admitting an unchanged-voter Learners record and
letting the original command wait end. Local durable status shows accepted
Learners, no committed operation and wait_for_commit. Exact replay returns the
observed unknown response; the old classifier rejects it.

Both original-record retry helpers now recognize that exact response. Their
10s outer deadlines and production behavior remain unchanged. The histories
refuse a changed target for the same operation ID, compare unchanged local status,
restore quorum and obtain a positive original-record commit. Exact duplicate
receipts, configuration completion and original Counter data survive cold
recovery. Local pending state remains a local observation: it cannot establish
quorum commit or forbid legal rollback of an uncommitted suffix.

## Executed evidence

- `before.log`: both TCP/QUIC histories fail on the old classifier after actual
  local accepted status and the exact unknown response have been observed.
- `configuration-all.log`: all11 configuration histories pass, including
  target authorization, pending recovery and canceled/unread command waits.
- `counter-all.log`: full all-feature counter158 passes with the final structured
  pending-observation fixture.
- `configuration-default.log`: all7 default TCP configuration histories pass.
- `clippy-first.log`: strict Clippy catches cognitive complexity26/25 in the new
  test history. Its pending observation is now a separate phase; no lint levels
  or thresholds change. `clippy-final.log` passes all-feature strict lint.
- `pre-push.log`: final formatting and all four strict Clippy profiles.

`ci-current.json` records the running operator job38044929785 at fe6d5ff, the
previous liveness correction. Its subsequently completed Ubuntu job114192434103
is retained in `ci-liveness-ubuntu.log`:155 counter passes and1 failure on the
exact uncommitted-record response, this time for a Joint record. No directory/
transfer acceptance can be inferred after a counter target failure. The macOS
job was still running at that observation; supported-platform acceptance for
this correction remains open.

The operator workflow already has cancel-in-progress:false and preserves running
sweeps. Initial inspection confused cancellations in the separate Platform
feedback workflow with operator cancellation; no workflow mutation was made.
CI remains background feedback and does not gate implementation or pushes.

Reproduce the meaningful checks:

```sh
cargo +stable test --locked --offline --all-features --test counter_service configuration_
cargo +stable test --locked --offline --all-features --test counter_service
cargo +stable test --locked --offline --test counter_service configuration_
sh .githooks/pre-push
```

Run feature configurations that spawn executables sequentially. These histories
need an environment allowing local TCP/UDP sockets. The full P0–P7 goal remains
active; broader provider, fault, platform and performance gates remain open.
