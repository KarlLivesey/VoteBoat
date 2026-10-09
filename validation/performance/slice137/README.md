# Slice137 — benchmark failure retention

Source base115767a plus recorded benchmark diagnostic changes; no production
source changes. provenance.json contains source/binary hashes, Linux/filesystem
labels and workload. Private hostname/device inventory is not published.

The benchmark now retains completed useful-write samples, unresolved tickets,
original error, poll totals and replica role/term/commit/applied state after a
warmup or measurement failure. Such artifacts cannot satisfy the raw or numeric
acceptance gate: no successful summary is produced. It attempts explicit worker
shutdown and retains cleanup outcome without replacing the original failure.
Artifact writes happen after measured work stops. A complete measured sample CSV
is written before verification/recovery so a later failure cannot discard it.

Three new tests cover synthetic unknown outcome retention, accumulated loop
failure totals, and a real TCP/TLS three-replica application admission failure.
The real history applies one ID, refuses a second at DedupCapacity, retains the
first sample and three replica states, and verifies explicit worker joins. This
is not a simulated leadership-change history or recovery proof for unknown writes.
The offered mode shares warmup diagnostics; its existing owned cleanup remains.

Reference command:

```
target/release/examples/native_benchmark target/bench-slice137-tcp-serial tcp 256 1
```

Three replicas,one group,64 warmup,256 measured8-byte commands,window1,heartbeat
50ms,election1000–1999ms,Btrfs,Linux loopback. No overlapping build/bulk work;
CPU/device are not exclusively reserved. Diagnostic instrumentation does not
add per-operation I/O, retries or changed durability/timeout semantics.

This closes the evidence-loss defect. Startup stage attribution remains next;
shared observer timings cannot be substituted for the startup assembly. Full
P0–P7 remains active, P8/Windows deferred and RPL-1.5 retained.

Actual validation: 16 example tests pass (including the three new tests and real
native failure), final execution0.02s after3.50s build. Release example build
succeeded6.16s. Initial compile correction removed .get() from the existing u64
term; no redesign or production change. Reference exit0:52.394331s,
4.886 applied ops/s,p50=171.037303ms,p95=458.592734ms,p99=854.999704ms,
max=949.789032ms. Recovered320,recovery retries0,original retries verified,
workers joined. Raw validator passes256 receipts/window/history/timing; fixed
250ms gate correctly exits1. One finite run, not a tuning gain or a controlled
comparison with136. Six numerical/context checker tests pass10.82ms; formatting
and80-contract inventory pass. Archived raw-check stdout redacts only the local
workspace prefix. Full benchmark measurements and sample hashes are unchanged.

Final example all-feature Clippy -D warnings passed1.12s. All handles terminal.
