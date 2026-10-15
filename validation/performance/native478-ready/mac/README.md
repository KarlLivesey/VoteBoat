#478 Mac APFS ready16/64 paired results

Original SSH handle 14036 actually terminated with exit 0 after ALL_TIMING_END. All 48 native cases, 48 frozen validators and 48 readback checks passed; no case was retried or replaced. Both hosts terminated before collection. Both arms explicitly select WitnessLogIo and reuse source 7e9068296ce00dde5bf3e03bf4d852ed2833d185 and the same fresh native476 executable. No source/build/profile/default change.

| Cell | Protocol | q16 completed/s | q64 completed/s | q16 during/s | q64 during/s | q16 p99 ms | q64 p99 ms | q16 median window refusals | q64 median window refusals |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| singleton | tcp | 7.531 | 7.406 | 7.531 | 7.406 | 158.825 | 160.021 | 15 | 19 |
| singleton | quic | 7.406 | 7.406 | 7.406 | 7.406 | 158.018 | 162.445 | 19 | 19 |
| moderate | tcp | 127.825 | 127.833 | 127.781 | 127.812 | 762.654 | 792.174 | 0 | 0 |
| moderate | quic | 127.840 | 127.852 | 127.812 | 127.812 | 776.264 | 1230.672 | 0 | 0 |
| bulk | tcp | 491.279 | 1015.083 | 463.625 | 1010.625 | 7987.827 | 337.077 | 389 | 0 |
| bulk | quic | 492.916 | 1011.578 | 482.250 | 1005.625 | 8335.050 | 864.550 | 238 | 0 |
| skew | tcp | 396.247 | 725.518 | 338.625 | 577.000 | 5757.416 | 3622.399 | 3427 | 1556 |
| skew | quic | 389.107 | 754.223 | 329.125 | 590.750 | 5803.419 | 3398.160 | 3505 | 1410 |

Three-trial arm medians. ANALYSIS.json retains every raw-recomputed trial and all 24 paired comparisons, intended/service/dispatch p50/p95/p99/max, during/drain rates, every refusal class, per-group backlog, resources, physical deltas and cold maintenance. Singleton offers 8/s with window 1; moderate 128/s with window 256; bulk/skew 1024/s with total window 4096. Skew uses two groups and weights 31:1, with the fixture enforcing 2048 pending per group. These finite offered caps are not maximum capacity or sustained 20M acceptance.

The 297984 offers reconcile to 265041 applied plus 32943 window refusals. Other refusal/unknown/pending classes are zero. Every case recovers 64+applied, verifies original-operation retries and joins workers. Active maintenance completes 624 checkpoints and 1872 reclaims. All actual groups have leader replica 1; the full leader placements are preserved in each pair. All voters and both skew groups share the same physical APFS device.

Bulk gains occur in every pair: TCP 491.279 to 1015.083 completed originals/s and QUIC 492.916 to 1011.578/s. q64 bulk refuses no offers, while q16 TCP refuses 207/560/389 and QUIC 127/238/347. Leader barriers per completed original fall roughly 0.125 to 0.060 while mean barriers remain around 14 ms. Metadata-inclusive command/unit means increase roughly 8 to 16–17; these are not literal command proposal widths. Bulk remains capped near its 1024/s offer rate, so this does not establish maximum capacity or spare headroom.

Regressions remain visible. Singleton TCP loses about 1.66% completed rate in two pairs and median p99 increases 158.825 to 160.021 ms; singleton QUIC median p99 increases 158.018 to 162.445 ms. Moderate TCP p99 rises 762.654 to 792.174 ms and moderate QUIC 776.264 to 1230.672 ms, with two slower-tail trials each. Moderate refusals remain zero; successful-throughput equality does not erase the tail losses.

| Skew group | Protocol | q16 completed/s | q64 completed/s | q16 during/s | q64 during/s | q16 p99 ms | q64 p99 ms | q16 median refused | q64 median refused |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Hot 7936 offers | tcp | 375.026 | 699.158 | 307.625 | 545.875 | 5759.050 | 3623.319 | 3427 | 1556 |
| Cold 256 offers | tcp | 21.818 | 28.502 | 31.125 | 31.250 | 453.489 | 841.038 | 0 | 0 |
| Hot 7936 offers | quic | 368.445 | 725.753 | 297.875 | 559.750 | 5805.074 | 3398.480 | 3505 | 1410 |
| Cold 256 offers | quic | 22.011 | 28.445 | 31.250 | 31.250 | 840.180 | 467.773 | 0 | 0 |

All 256 cold offers apply in every skew case with no cold refusals; cold during rates remain 31.0–31.5/s. Per-group completed rates use the same whole-case elapsed denominator and therefore include hot drain time. q64 hot refusals remain TCP 2687/1556/1146 and QUIC 1410/1216/1809. Hot backlog still approaches the 2048 per-group fixture ceiling. Cold TCP trial 1 p99 worsens 441.054 to 1695.135 ms; cold QUIC trial 3 worsens 879.274 to 1155.945 ms. TCP median cold p99 worsens 453.489 to 841.038 ms. Higher total throughput does not demonstrate uniformly preserved cold latency.

The remaining measured stage is still persistence: q64 bulk/skew leader barriers account for roughly 85–89% of native elapsed wall time. This is inclusive wall attribution, not CPU time or proof of a cause inside that scope. Whole-child skew CPU usage is roughly 0.34–0.45 core equivalent over its separate warmup/recovery-inclusive boundary. Primitive ordinary namespace counters exclude cold selectors/reclaim/repair. All per-replica counters and append-unit/group histograms remain available; no proposal-width maximum or drive-byte claim is made.

Five late offer decisions are retained, with all dispatch distributions; no explicit missed-timer counter exists. Mac whole-process RSS is in bytes and includes warmup/recovery/cleanup, not a continuous memory/debt plateau. Three observation-only missing-path probes are retained with honest tool-transcript provenance; they did not alter or replace the campaign. Original child/controller commands, streams, resource receipts and all setup outputs remain available.

Large originals are losslessly packed with original hashes/lengths in PACKING.json; SHA256SUMS seals every payload. No source trees, binaries, buildtrees or generated WAL are included. FileLogIo and ready16 remain defaults. Further real ext4/XFS, sustained acceptance, independent placement and full P9 qualification remain open.
