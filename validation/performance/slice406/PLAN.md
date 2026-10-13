P9 execution begins on 13 October 2026. No P10 security phase or Windows work.
Minimum framework acceptance: exceed complete OpenRaft published client curve
on matched conditions: batch1 clients1/64/256/512/1024/4096, rates
33000/912000/1808000/2543000/3006000/3548000 entries/s; batch4 clients4096,
5615000 entries/s. Pinned reference e88bb40707f2e511d8ecc44d31edcb1d814a11a8.
Published setup 3 replicas, empty requests/responses, in-memory BTree log,
no-op application, function-call RPC, client1worker/server16workers, MacM1Max,
1024 warmup/default20M rounded total. Actual code, every completion, hardware,
retention and revision must match before declaring a comparison pass.
Different framework harness or incomplete batch support is diagnostic only.
Durable application throughput/tail/headroom is a separate acceptance family.

Macro: P9.0 reproducible attributed baseline; P9.1 cheap/pipelined ordered group;
P9.2 parallel independent groups/resources and fairness; P9.3 sustainable
maintenance/skew/catch-up performance on Linux/macOS; P9.4 P8 topology cost.

Mini: current touched-group/log-core staging + comparative diagnostic, no disk
format change. Purpose removes unrelated-data and duplicate validation copies;
depends on existing atomic validators/tickets; checks independent transition,
crash/restart/fencing/unchanged-state tests and quiet interleaved comparisons;
advances P9.1, measured by P9.0. Next ready same-group proposal batches with
individual outcomes, immediate singletons; depends on admission/result ownership,
checks every original command/result under retries/config/term changes and bounded
hot/cold fairness; advances P9.1 and makes batch4 framework support real. Following
bounded accepted/durable pipeline and optional persistence/replication overlap;
depends on batch ownership+explicit ordered completion dependencies; checks prefix
holes/term/vote/configuration changes and exact old completions; advances P9.1/2.
Measure each independently before combining. Larger chunked log representation,
segmented WAL and platform async-I/O remain explicit subsequent decisions.
