# 480: exact temporary admission reuse

[Schema](SCHEMA.md) records the API, ordering, ownership and error/restart plan
before production edits. [Main qualification](main/README.md) retains differential
application/runtime/provider controls, actual service recovery, metadata and all
strict checks. Source is published as
`f7b0ef12709bb7e7aad4f0279851aa035b508e9e`.

[Fresh Linux qualification](linux/README.md) proves fresh native root/executable
compilation and exact source/compiler/profile identities. All three original
workloads and native validators pass. Its extra default-diagnostic checker fails
after those successes; the original controller exit1 remains and a separate
same-raw correction0 verifies the uninstrumented default FileLogIo case.
[Root archive verification](ROOT_FRESH_LINUX_REVIEW.json) checks all148 payloads
and144 selected original hashes/lengths/modes, including seven lossless gzip
files. No workload repeats or measured gain are claimed.

Mac qualification continues separately. [481's fixed paired comparison](../native481-admission/SCHEMA.md)
will decide retention using actual completed rates, latency, refusals, fairness
and physical work. FileLogIo/ready16 remain defaults. Full P9 and the forthcoming
real ext4/XFS native acceptance remain incomplete.
