# Slice139 — rejected manifest staging experiment

Source baseb2fd209. The control binary was copied before compilation and its
SHA-256 matched slice138's recorded executable. Candidate source/binary and exact
experiment.patch hashes are in provenance.json. Linux/Btrfs,three replicas on
one host/device,256 measured8-byte increments,64 warmup,window1,original startup
TCP/TLS timers. Diagnostic runs explicitly marked journal_timings=true. No
exclusive CPU/device reservation; no compilation/bulk workload overlapped the
sequential measurements. Private hardware inventory is not published, limiting
independent hardware reproduction.

The candidate changed only staging-file synchronization to sync_data, retaining
log sync_all, rename and directory sync_all. Rust describes this as content sync
with possible platform fallback; Linux preserves retrieval-essential metadata
such as size. These contracts motivated an experiment, not a weaker buffered
mode. [Rust File documentation](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_data),
[Linux fsync/fdatasync documentation](https://man7.org/linux/man-pages/man2/fsync.2.html).
The production primitive was restored to sync_all after the candidate failed.
The retained private publication helper and new interruption tests use sync_all.
experiment.patch is historical experimental evidence, not the final source.

Commands (fresh roots, sequential):

```
/tmp/voteboat-slice139-control-benchmark target/bench-slice139-control-diagnostic tcp 256 1 --journal-timings
target/release/examples/native_benchmark target/bench-slice139-candidate-diagnostic tcp 256 1 --journal-timings
```

Control: exit0,49.777019s,5.143ops/s,p50=157.185735ms,p95=392.577207ms,
p99=1030.629025ms,max=1675.857735ms,recovered320,original retries verified,
workers joined. Raw256 receipts and complete journal-stage arithmetic validate.
Per-replica mean log synchronization21.53/22.39/22.48ms; full publication
42.10/44.43/44.42ms. Marked diagnostic is not uninstrumented250ms acceptance.

Candidate: exit1,operation229 Unknown(LeadershipChanged). Retained164 receipt
rows for IDs65–228 plus original error, unresolved request CSV, replica states,
journal failure snapshot and workers_joined=true cleanup. Retained rows pass
identity/value/group/index/latency arithmetic; they cannot validate unknown229,
complete performance or subsequent recovery. Replica snapshot records follower1
at term1 and leader2/follower3 at term2 with differing commit/applied boundaries;
this is a partial observation, not a cause or safety violation diagnosis.
No candidate summary, p99, throughput or successful recovery result. Raw checker
correctly rejects the missing complete summary. Empty output files are refusal
or incomplete-run artifacts. No preferred rerun to obtain a passing result.

The experiment supplied no valid complete performance benefit, so default full
file synchronization remains. Neither causation of the leadership change nor
unsafety of data synchronization is established. No optimized mode advertised.
New tests retain useful evidence:15 publication primitive cuts (initial5,
legacy5,reclaimed5) preserve valid selection/acknowledged hard state and locks,
with initial missing selection failing closed. Existing replacement interruption
and store-failure/conformance tests also pass. These are native-file process/
primitive-boundary checks, not physical power-loss validation.

Final restored code also passes real three-process TCP/TLS leader replacement,
retry and native-file recovery. Detailed checks/times are in validation/REPORT.md.
Full P0–P7 remains active; original250ms/sustainable-capacity gates remain open.
Next deliverable is actual networked nested-source retirement/reclamation, linking
134's native-file retirement and135's networked later movement. P7 is retained;
P8/Windows remain deferred; RPL-1.5 and original design pack unchanged.
