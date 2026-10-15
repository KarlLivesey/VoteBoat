# Native ready-batch comparison 478

Both original campaigns actually ended0: Linux21706 and Mac14036. Each contains
48 serial cases and48 successful process/validator/readback checks, with three
rotating adjacent ready16/64 pairs per transport/load cell. Both hosts finished
before collection. Same published476 native executable per host, explicit
WitnessLogIo, three voters sharing one physical device,8B Counter originals,
64 warmups, active2s maintenance and unchanged native timer. See [schema](SCHEMA.md).

Bulk8192 offers at1024/s/window4096, median completed originals/s and intended p99:

| Host/protocol | Ready16→64 originals/s | p99 ms |
| --- | ---: | ---: |
| Linux TCP/TLS |260.799→591.381|15213.510→5708.929|
| Linux QUIC |310.957→667.779|13500.698→4796.659|
| M2 Max TCP/TLS |491.279→1015.083|7987.827→337.077|
| M2 Max QUIC |492.916→1011.578|8335.050→864.550|

All12 bulk total-rate pairs improve, with lower barriers/original and nearly
unchanged mean barrier time. Singleton/moderate and cold-tail regressions remain;
Linux bulkQUICt2 during-rate falls despite its faster drain-inclusive total.
Every skew case applies256 cold originals without refusal, under an explicit
2048/group fixture ceiling, but cold latency still worsens in several pairs.
Ready64 remains opt-in. FileLogIo and ready16 defaults stay unchanged.

[Linux raw archive/readout](linux/README.md) retains1138 payloads/46 lossless gzip;
[Mac](mac/README.md) retains1188/49. Root independently verifies every recursive
archive member/hash and decompressed original, and recomputes all96 raw ledgers,
rates and nearest-rank quantiles, matching both host and independent readouts.
See [archive verification](ROOT_ARCHIVE_REVIEW.json), [raw review](ROOT_DATA_REVIEW.json)
and [1491 unchanged qualified input proof](ROOT_SOURCE_REUSE.json). Initial parser
and observation errors are preserved; no successful workload is replaced.

Whole-child CPU, nested elapsed storage phases and whole-fixture host polls have
different boundaries; histograms count append units rather than proposal command
widths. These finite offered histories do not establish sustained headroom,
physical memory/debt bounds, independent placement or one-group framework floors.
The real ext4/XFS acceptance cell and full P9 objective remain open.

[479's next measured-stage schema](../native479-poll/SCHEMA.md) locates work inside
the current Counter ready64 path using headless numerical CPU profiles before
selecting a bounded change. No browser, policy change, rebuild or default change.
