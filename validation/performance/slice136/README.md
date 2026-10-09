# Slice136 — unchanged reference and fixed p99 gate

Source revision b2a367e48e045c1776f38aafb4826d4d50462b24. No Rust production or
benchmark changes. provenance.json records source/binary hashes, toolchain,
a Linux platform label and filesystem class. Detailed hostname, kernel,
hardware/device/memory inventory and absolute workspace paths are retained only
in a local private capture. This limits independent hardware reproduction.
Linux loopback, three replicas
on one host/device; no exclusive CPU/device reservation. No overlapping build or
bulk workload during the sequential reference runs. Device power-loss behavior
is not established by this experiment.

Build: cargo +stable build --release --example native_benchmark --all-features
--locked --offline (success17.53s).

Run commands:

```
target/release/examples/native_benchmark /tmp/voteboat-slice136-tcp-serial-a tcp 256 1
target/release/examples/native_benchmark target/bench-slice136-tcp-serial-a tcp 256 1
target/release/examples/native_benchmark target/bench-slice136-tcp-serial-b tcp 256 1
```

All use startup assembly,3 replicas,1 group,64 warmup,256 measured8-byte commands,
window1,heartbeat50ms,election1000–1999ms and native durability. All handles are
terminal. The initial /tmp root was mistakenly tmpfs; tmpfs-smoke.* retains its
successful recovery/retry/join result but is excluded from disk acceptance. Its
3.988ms p99 cannot substitute for the disk gate. Context mode explicitly refuses
that substitution; this is not a controlled memory-vs-disk experiment.

Disk A: exit1, operation250 Unknown(LeadershipChanged). No completed summary;
CSV contains only its header. failure.txt records the absence of valid complete
performance/recovery evidence. No invented partial samples or preferred rerun.

Disk B: exit0,44.532562s,5.749 applied ops/s,p50=161.541945ms,
p95=256.911161ms,p99=432.366616ms,max=958.446735ms. Recovered320; original
operation retries verified,workers joined. Btrfs disk root. The independent raw
checker verifies256 receipts,window/order/latency/rate arithmetic. Numeric gate
fails432.366616ms >250ms with exit1. This is finite reference evidence, not
sustainable capacity or proof of why the latency/leadership change occurred.

New validation/check-serial-gate.mjs invokes the existing raw checker before
checking the exact workload, recovery flags, captured context/digests and fixed
250000us threshold. Context is trusted recorded host input, not a storage proof.
check-serial-gate.test.mjs has six passing direct Node tests after the privacy
revision. Workspace-relative benchmark roots are allowed only with an explicit
path-kind tag and a bounded traversal-free shape. Actual CLI checks
retain stderr in *.check.txt and JSON stdout in *.gate.json: B correctly returns
a failed numeric gate; A rejects missing complete summary; smoke rejects its
non-reference context. Empty A/smoke JSON files are rejection outputs, not results.
The tool sandbox initially denied the child Node subprocess (EPERM); actual CLI
checks completed using approved escalated read-only execution. No production or
benchmark change was made to bypass validation.

Next: partial-run outcome retention and critical-path diagnosis, then a
cause-supported safe fix and matched disk-backed revalidation. No p99 threshold,
election timeout or durability relaxation; full P0–P7 remains active.

Public *.check.txt logs replace the absolute workspace path with <workspace>;
the local private capture preserves the original logs. Summary/sample hashes and
timing/recovery results are unchanged. B raw checking and numeric failure were
rechecked after this revision.
