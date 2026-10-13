# Linux native421 visit-policy diagnostic

Original runner15132 is terminal0. All36 rotating trials pass using one exact
1091610 source/library/executable on this host. Only requested visit size16/32/64
changes. Source1396 and actual artifact hashes match before/after. No build, test
or copy overlaps timing; remote evidence transfers were held until completion.
Every case completes256 Counter originals after64 warmups, validates each value/
index, recovers320, checks first/last duplicate receipts and joins all workers.
Actual width histograms and per-replica append/sync/publication records are retained.
No binary, journal-state directory or build tree is published.

| Transport | In flight | Visit size | Median durable PUTS/s | Request p99 |
| --- | ---: | ---: | ---: | ---: |
| TCP/TLS |32|16|1,399.046|43.517ms|
| TCP/TLS |32|32|2,684.935|18.262ms|
| TCP/TLS |32|64|1,398.575|41.711ms|
| QUIC |32|16|187.237|332.044ms|
| QUIC |32|32|251.421|219.221ms|
| QUIC |32|64|260.107|307.846ms|
| TCP/TLS |1|16|8.721|322.450ms|
| TCP/TLS |1|32|8.504|279.196ms|
| TCP/TLS |1|64|8.056|333.829ms|
| QUIC |1|16|8.393|271.112ms|
| QUIC |1|32|7.725|409.063ms|
| QUIC |1|64|8.352|316.744ms|

At window32, size16 produces sixteen16-original proposal batches; sizes32/64
produce eight32-original batches. Leader measured durability barriers fall32→16
in every trial. Window1 retains256 one-original proposals and512 leader barriers.
Size64 cannot show64-wide batches at window32 and gives no consistent benefit.
All slow singleton and tail results remain. SYNC/publish elapsed time differs
substantially between protocol cells; this does not isolate transport as a cause.
Physical method intervals are not CPU time and cannot be summed across replicas.

This is a short three-replica Counter run on one host/device, using TLS/TCP or
QUIC and native log publication. It establishes no sustained maintenance, skew,
offered-load/fairness, deployment-level capacity or minimal-framework floor.
Default visit size stays16; Mac results and broader workload evidence remain.
An initial local preflight incorrectly required executable permission for the
rlib as well as the binary; corrected before timing, with unchanged artifact bytes.
