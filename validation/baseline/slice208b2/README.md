# Slice208b2 — native receive pressure and forced snapshot recovery

Starting revisionea452c4. The existing shared native test assembly now accepts
explicit ingress limits before starting providers. Existing profiles retain
all defaults. No production contract, wire/storage format or consensus rule
changes. Both new histories use actual TCP/TLS or QUIC, native files/workers,
three replicas and eight independent Raft groups.

The history stops one follower, advances both surviving replicas, checkpoints
beyond the old follower's durable suffix and closes every transport. All stores
then reopen; each stale group starts below the survivor checkpoint boundary.
Three ingress frames (one control reserve, one background slot) and one accepted
snapshot recovery job impose bounded contention. The stale receiver holds
message dispatch until actual admission refusal. The healthy majority then
applies8 writes and completes8 quorum reads. Resumed stale dispatch admits one
message per small owner turn every four rounds; another8 writes and8 reads
complete while snapshots repair the stale groups. At least one second-wave
write must finish before all eight groups have recovered.

All8 groups must reach both durable and application boundaries, with8 actual
snapshot-load completions. After every provider closes/joins and the files
reopen again, all32 original operation IDs must return their historical results
without changing data. Every poll checks ingress count/bytes, recovery job/image,
outbound and owner reserved-byte bounds.

Evidence:

- initial.log: both histories fail at the core-base-only completion observation.
  Core durable state advances before the final application snapshot load; this
  did not establish completed recovery. The initial check was too early.
- applied-boundary.log: both pass after requiring application applied boundaries
  as well. TCP172/QUIC289 refusals,8 installs each,8/6 second-wave writes finish
  before catch-up respectively.
- all.log: all34 native example tests pass. The final new histories record
  TCP164/QUIC341 refusals, peak3 frames, peak1 recovery job and8 installs each;
  all8 second-wave writes finish before catch-up. Existing shared lanes, drain,
  maintenance and forced-recovery histories pass too.
- default.log: the independent default-feature TCP history passes;152 refusals,
  peak3 frames, peak1 job,8 installs,8 second-wave writes before catch-up.
- pre-push.log: formatting and strict Clippy for default, all-feature, core-only
  and native-only builds pass with zero diagnostics.
- inventory.log / metadata.log:106 contract paths and13 metadata checks pass.
  These are metadata integrity checks, not runtime certification.
- source.sha256 / source-check.log: final source and evidence fingerprints.

Commands:

```sh
cargo +stable test --locked --offline --all-features --example native_benchmark receive_pressure -- --nocapture
sh .githooks/pre-push
cargo +stable test --locked --offline --all-features --example native_benchmark -- --nocapture
cargo +stable test --locked --offline --example native_benchmark receive_pressure -- --nocapture
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

Configurations ran sequentially. Socket histories had loopback access. Initial
failed roots are named in initial.log and retained under /tmp. These are selected
Linux process/file histories; they do not establish hardware power-loss,
macOS/separate-host acceptance, general liveness or a sustainable p99 result.
The full P0–P7 goal remains active; persistent discovery is next.
