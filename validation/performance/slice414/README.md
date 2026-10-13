# Larger bounded batches and native ready coalescing

Core startup `BatchLimits` exposes independent proposal/outbound counts, with
64/64 defaults and4096/1024 hard ceilings. The ordinary receive ceiling remains
independently bounded at1024, allowing different outbound choices; repair RPCs
keep64. First event locks the settings. All byte/owned-capacity, identity,
membership, exact-ticket and application-result contracts remain authoritative.
Larger-than-outbound suffixes use existing durable chunked replication.
[Schema](SCHEMA.md).

Native [routine-control coalescing](../native-ready-coalescing/README.md) lets an
already-selected Data turn consume a bounded ready proposal quantum while only
Heartbeats/same-current-term Appended inputs are queued. Controls remain queued
and untrusted. Other protected inputs anywhere in the queue and all background
work still veto batching. Class-turn fairness remains; the data quantum changes.
There is no arrival delay, extra in-flight group transition or disk format change.

The standalone harness selects primary4096 proposals/1024 replication, retains
Common64, leaves upstream defaults intact and reports exact original batch widths.
[Harness qualification](../../../benchmarks/matched/validation/performance/slice414-harness/REPORT.md).

## Linux validation

- Final all-feature library335/0 and no-default-feature library266/0.
- Affected existing integration tests246/0 across Raft, log store/reclamation,
  runtime, membership, learners, snapshot and replication-scope contracts.
- Actual three-process TCP/QUIC retry, leader replacement and native-file recovery2/0.
- Actual ModelIO cuts cover a4096-original append and its commit marker at
  append/sync/publication-before/publication-after: acknowledged prefix survives,
  unknown work never produces early results, and fresh-session recovery rebuilds
  the exact selected committed prefix. The focused crash test also passes again
  after final test refactoring.
- Formatter and all four strict repository Clippy profiles plus standalone
  formatter/strict Clippy exit0; raw records are in [linux](linux).
- Standalone7/0 and four real1-client/16-server runtime controls retain all16384
  original results and three frontiers17409. Primary observed width4096 and
  Common64 observed width64; counters include1024 warmups separately from timing.

Review caught a shared repair-range inspector incorrectly using the ordinary
limit. It now has explicit ordinary/repair bounds, with a real historical repair
regression at ordinary limits1/64/1024. An initial regression fixture contained
only the final configuration entry; a retained seven-command repair history
correctly exercises the independent cap. Initial strict checks found a test's
manual-contains idiom and excess cognitive complexity; both causes were fixed.

Paired capacity, current Mac qualification and full minimum curves remain
pending. No completed-write speedup is claimed from acceptance tests or width
counters. The standalone framework bypasses native scheduling, so the separate
native coalescing change cannot explain its future throughput results.
