# Native445 Mac measured diagnostics

OriginalSSH22892/PID2095 actually terminates0 after18 serial cases; postvalidation49739 also terminates0.15000 measured originals+64warmups percase, one group/three quorum-durable Counter voters,8-byte commands/window64. All cases recover value15064 with zero recovery retries, verify first/last retry outcomes and worker joins, and reach actual maxinflight64. Independent current native result and canonical-path publication checkers pass18/18 on the same outputs. No capture rerun or case dropped.

| Protocol | Quantum | Median originals/s | Median p99 ms |
|---|---:|---:|---:|
| tcp | 16 | 351.765 | 272.277750 |
| tcp | 32 | 544.076 | 314.788292 |
| tcp | 64 | 690.381 | 279.568875 |
| quic | 16 | 346.365 | 764.764166 |
| quic | 32 | 553.430 | 135.404416 |
| quic | 64 | 753.943 | 94.877500 |

All three q32 and q64 throughput trials improve over q16 in each protocol. TCP q32 p99 regresses in trials1 and3 (ratios1.156129/1.807737); q64 p99 regresses in trial3 (1.156103). Other TCP p99 pairs and all QUIC p99 pairs improve. Maximum retained latency is1101.806083ms in TCP q32. All pertrial rates/p99 ratios and slowtails remain in READOUT.json and original CSV/stdout; no clipping or best-case substitution.

Everytrial actual leaderwidths: q16=16×937+8×1, q32=32×468+24×1, q64=64×234+24×1. Leader sync/publication counts: TCP q16=1698/1706/1703, QUIC q16=1707/1702/1703; q32=938 and q64=470 in all trials. Median leader WAL sync/manifest publication intervals: TCP12.319/29.033s→5.640/18.457s→2.693/9.613s; QUIC12.548/29.488s→5.552/18.396s→2.504/8.229s. Separate file/directory sync deltas for every voter remain in READOUT/rawpublicationCSV. These counters are observed physical calls, not inferred batch counts. Inclusive elapsed I/O intervals may overlap/nest and live snapshots can cut calls; do not add them as CPU or client critical path.

Source/artifact/flags/compiler proofs and all earlier readiness/setup errors are in ../setup (sealed manifest f0d9c66bd6d7febaf180f134ed446972c8ea8730df1708800f674e6bddf2dc2e). All quantums use the same frozen ab091ea native opt3/debug0/assertions+overflow/default LTO/codegen artifact and source1457. Validators912cdf4 are separate committed tools, not changes to frozen build input. Pre/post campaign identity guards pass; no binary/build tree/generated WAL state is archived. Existing LMStudio/background user applications were left unchanged and disclosed.

Finite diagnostic rates do not establish sustained20M acceptance, maintenance/fairness/headroom, refusal counters or physical RSS/debt bounds. Default ready quantum remains16. Full Linux/Mac framework floors and remaining P8/P9 work remain incomplete.
