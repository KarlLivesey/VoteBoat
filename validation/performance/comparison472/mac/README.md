# Mac472 framework comparison

Original SSH78197/PID81924 ran bulk12 then singleton12 once and finished exit0/DONE. All24 original process exits, raw outcome checks and before/after artifact/source guards passed. Independent saved-output recheck and final identity guard passed; no workload repeated. Whole470+472 candidate versus original455, same host/compiler/profile. This does not establish pipeline activation or full P9 acceptance.

| Cell |455 OFF originals/s|472 OFF originals/s|455/472 p99 ms|OFF rate ratios t1/t2/t3|OFF p99 ratios t1/t2/t3|
|---|---:|---:|---|---|---|
|4096/b4|6,261,950|6,405,981|3.348125/3.315583|0.987625/1.017186/1.033533|1.050110/0.982541/0.981968|
|1/b1|66,933|66,871|0.028750/0.028667|0.993906/0.999068/1.003020|1.001489/1.004348/0.991355|

Bulk t1 rate−1.24%/p99+5.01% is retained; other two bulk pairs improved. Singleton is mixed and essentially neutral, with two rate losses and two p99 regressions. ON and OpenRaft all trials/tails remain in READOUT/raw; no uniform speedup or healthy-margin floor claim. Root owns overall retention decision after Linux evidence.

Each bulk trial requested20M and completed19,988,480 originals/4,997,120 requests; singleton1,048,576 originals/requests. All1,024 warmups completed and frontiers were19,989,505 or1,049,601 per replica. Client1/server16, empty payload, one group/three voters, volatile stores/in-process RPC, default retention/upstream envelopes and per-original outcomes are unchanged. Throughput ends at all client joins before latency flatten/sort/follower verification/shutdown. Snapshot representations differ as disclosed by COMPARISON.

Candidate OFF checkpoints per replica ranged3,903–3,904 bulk and209 singleton; bulk snapshots installed11,480 across three trials/replicas, singleton0. Peak logical retained entries22,044/6,000, peer queue9/3 and client queue3,736/1. These are logical diagnostics, not physical RSS/debt evidence. No offered-load refusal measurement is present; zero process/output failures is not a measured refusal/headroom curve. ON29/four/15 correlates actual counts; inclusive elapsed observer scopes overlap and are not additive CPU costs.

Baseline455 source bytes/artifact are frozen, but ordinary source modes were664/775 rather than requested Linux644/755; fixture keys600 preserved. This historical mode caveat is explicit. Candidate472 preserves all1,471 file+322directory modes and all1,453 common Linux strict7 inputs. Baseline/candidate executable SHA/UUID/compiler/locks/fresh4/root-script/oracle/dSYM proofs remain in CONFIG and associated qualification records.

First postprocessing attempt indexed optional OpenRaft attribution and failed KeyError; exact stderr/exit/cause retained. A focused data-only get() correction passed without rerunning workloads. No binaries/symbol bytes/buildtrees/full source/WAL included. ORIGINAL_EVIDENCE_INVENTORY lists raw files before archive compression; COMPRESSION records both hashes/lengths and independent decompression.
