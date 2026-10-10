# Slice240 — exclude client ports from fixture listener allocation

Base19e4fb57cc110055fc0d963439bf9a3ed3befc45. The retained239 failure has node2
cold-recovery AddrInUse and sibling ready records identify its peer port33426.
This host's Linux automatic client range is32768–60999. The old10000..49000 pool
reaches this range and drops placeholder listeners before native children bind.
The original transient owner is gone: we do not identify which client claimed it.

The counter executable fixture now probes248 nonprivileged blocks below32768,
retaining128 stride, the exact TCP/UDP endpoints, per-process never-reused blocks,
fake-peer listener handoff and child/socket cleanup. The extracted reservation
operation permits a local late-allocation history without spawning hundreds of
services or inserting fictitious used blocks into the global allocator. The
regression uses the actual Linux range and kernel listeners, checks exclusive
reservation, then checks TCP/UDP release. It fails the old pool at33041 and passes
after correction. There is no production/provider/API/file-format or timeout change.

Eight all-feature discovery histories and the entire191-test all-feature counter
suite pass, including the originally failed native TCP history and QUIC/WAL/
checkpoint/data-retry/worker-join cases. All134 default-feature counter tests pass sequentially afterward. Formatting/four strict Clippy profiles finish zero, inventory108
and unchanged9 partial reviews/68-operation conformance metadata pass. This is
finite local functional evidence, not proof against arbitrary externally occupied
ports or custom kernel allocation ranges. macOS uses background CI; the specific
port-range regression is Linux-only. Older macOS directory AddrInUse is not assigned
the same cause. Original configuration/write/status-wait uncertainty and broader
P0–P7 acceptance remain open; functional Linux/macOS features precede tuning/security.
Windows/P8 stay deferred, and the full goal remains active.

Preceding239 run38071895206 remains in progress at both retained observations:
macOS job114270824821 and Ubuntu114270825119. No terminal result is inferred.
