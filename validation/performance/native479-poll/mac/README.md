#479 Mac headless Counter skew profile

Original SSH handle 11837 and the owned samply capture actually terminated with exit 0. Frozen validation, conservation, recovery, retries, joins, active maintenance and all six capacity/six provider readbacks pass. All conversion and symbol queries began after both hosts actually terminated. No capture was repeated.

The unchanged476 native opt3/debug0/assertions/overflow/defaultLTO+codegen binary serves three voters and two skew groups on the same APFS device. Exact Mach-O UUID 1D656ECA-0526-35FD-B6B1-B19CE2F04F74 and original/copy SHA256 match. Existing samply0.13.1 records at1000Hz with save-only/no-open. No browser, SVG, policy change, tool install, presymbolication during capture, old maps or build.

| Diagnostic workload | Result |
|---|---:|
| Offers / applied / hot window refused |8192 /6587 /1605|
| Completed / during originals per second |733.223 /569.625|
| Native elapsed / drain |8.983627s /0.983627s|
| Hot backlog / cold backlog at8s |2024 /6|
| Hot p99 / max intended ms |3565.098 /3617.804|
| Cold applied / refused |256 /0|
| Cold during originals per second |31.250|
| Cold p99 / max intended ms |862.228 /893.476|
| Max whole-fixture host poll ms |21.314|

These profiled rates/tails are diagnostics, not an unprofiled paired improvement or sustained acceptance. The fixture enforces2048 pending per group. Per-group completed rates share the whole-case elapsed boundary and include hot drain time. WORKLOAD_READOUT.json contains successful-original intended/service p50/p95/p99/max, all-offer dispatch distributions and every refusal class.

CPU attribution uses each observed threadCPUDelta once, never multiplied by coalesced wall sample weight. The native PID has19 threads and20707 sample rows:4,238,938µs observed thread CPU delta,99,901 wall sample weight,15,991 positive-CPU rows and4,716 zero-delta rows. The main thread accounts for95.470% of observed delta. These are whole-launch observations including startup/warmup/recovery/cleanup; the throughput timer cannot be precisely cropped from these raw files.

| Physical sampled context | Observed CPU-delta µs | Share |
|---|---:|---:|
| Counter admission union |1772186|41.807%|
| Ready-prefix Counter admission |1119504|26.410%|
| Submission Counter admission |652682|15.397%|
| ready_batch::collect inclusive |1141180|26.921%|
| Reserved OperationId set insertion self |857597|20.231%|
| Counter.dedup outcome lookup self |764313|18.031%|
| Pending-receipt lookup self |292486|6.900%|
| write self |348978|8.233%|
| fcntl self |199090|4.697%|

Inclusive contexts overlap and must not be added. Ready admission contains569385µs under reserved-set insertion and485813µs under outcome lookup; submission contains330560µs and277947µs respectively. Full physical self/inclusive/caller rankings, stacks, exact names and original address responses are retained. This confirms repeated Counter reservation/dedup validation as a measured removable-work candidate; it does not establish a performance gain for a future batch-local context.

The interval-attribution caveat is substantial: semaphore_timedwait_trap receives1,121,775µs (26.464%) of observed delta at sampled wait stacks. That is not CPU spent sleeping. The first main-thread sample appears at373.550959ms with13418µs delta; startup/first intervals and CPU-between-samples can be assigned to the current stack. Coalesced wall weights and row counts cannot rank CPU.

Headless localhost symbolication queries1263 library/address pairs against matching476 Mach-O and matching system UUIDs:1177 resolve a function,86 remain unknown. The owned server is interrupted and joined with exit0. Unknown leaf delta is48185µs (1.137%); any unknown ancestry is1183255µs (27.914%). Observed null/malformed stacks are zero. Native debug0 limits inline/source precision; unmarked unwind truncation/capture loss cannot be quantified. Preserve unknown raw addresses and stderr; do not treat absence of a frame as proof of no cost.

Linux uses a different workload and cycles:u event weighting, with its own unwind limitations; these percentages are not cross-host comparisons. Independent raw review agrees with the Mac denominator, resolved leaves and admission split. Original commands, guards, cleanup, requests/responses and all raw ledgers remain available. A nonfatal early inline-Python formatting warning and failed source-path glob are retained with honest tool-transcript provenance; neither changed or repeated the capture.

PACKING.json preserves original hashes/lengths for packed large text; SHA256SUMS seals every payload. FileLogIo and ready16 remain defaults. No three-filesystem, sustained P9, memory/debt, placement or full-goal acceptance closes here.
