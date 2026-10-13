# Bounded routine-control proposal coalescing

When the existing class scheduler selects Data, already-ready ordinary proposals may share one bounded transition despite queued Heartbeat or same-current-term Appended inputs. Inspection neither consumes nor validates those replies; normal later control processing retains all identity/context/quorum checks. Any other control anywhere in its queue, or any background work, remains a boundary.

The C,C,Data,Background cursor and selected turns remain unchanged. A Data turn can consume a bounded batch quantum; this is not a promise of the old per-command 2:1 event ratio. Existing visit/output/byte/entry budgets and per-original authorization still apply. The window now uses the Core's configured max_proposal_entries, with defaults unchanged at 64. No timer delay, extra worker request, disk-format change or relaxed Written/Durable/application gate is added.

Seven new deterministic controls cover natural consecutive control turns and subsequent control progress; untouched same-term untrusted replies with normal WrongIdentity rejection; protected higher/lower-term, vote, campaign, configuration and checkpoint boundaries including a protected tail behind a routine head; later-original refusal; duplicate/conflicting-byte/retry Counter outcomes; and visit/output/byte/deadline/per-Core count bounds. A configured 80-entry cap admits 70 ready originals while smaller limits remain enforced.

Actual scoped validation: 16 ready_batch library tests passed with all features and 16 with no default features; all-feature library strict Clippy passed. Commands/logs are in target/p9/ready-coalescing-{all,core,clippy}.log, using isolated target/p9/ready-coalescing-checks. Scoped rustfmt and diff checks passed. Root owns complete repository formatting/four strict profiles and broader real persistence/quorum validation.

The prior observer did not identify queue-selection causes; no causal gain is claimed from its singleton counts. Shared append/barrier/publication failure semantics remain unchanged.

The paired native campaign is complete on Linux and M2 Max: all 24 original cases per host passed. Each uses 64 warmup and 256 measured eight-byte Counter originals, three replicas on one host/device, actual TCP/TLS or QUIC, and the same instrumented native diagnostic. A completed write means the original Applied receipt with its exact outcome. Every case then reopens durable state at value 320, verifies an original retry, joins workers, and checks all source/artifact identities. Reference is the prior frozen observer (base cab9d6d plus recorded source); candidate is exact 306b26b. Both use independently compiled release opt3 with assertions and overflow checks, default release LTO/codegen settings, and identical benchmark source. The candidate also includes the default-equivalent Core BatchLimits API; this comparison does not isolate a single line or trace the cause of queue selection.

| Linux case | Reference writes/s | Candidate writes/s | Median paired ratio | Reference p99 ms | Candidate p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| TCP, window 32 | 274 | 1,517 | 5.53× | 212.4 | 31.7 |
| QUIC, window 32 | 37.2 | 207 | 5.57× | 1,717.1 | 262.7 |
| TCP, singleton | 9.78 | 9.07 | 0.928× | 239.1 | 255.9 |
| QUIC, singleton | 9.00 | 8.15 | 0.947× | 283.3 | 321.1 |

Rates and p99 are medians of three original trials; ratios are medians of within-trial candidate/reference ratios. All window-32 candidate trials record 16 proposal visits of width 16 for the 256 measured originals and 32 leader WAL syncs plus 32 manifest publications. Reference records 224 singleton visits plus two width-16 visits and 254–259 leader sync/publications. Singleton controls retain 256 width-one visits and 512 leader sync/publications in both variants.

Singleton performance did not improve and was variable: TCP paired ratios were 0.204, 0.928 and 0.950; QUIC ratios were 0.947, 0.835 and 1.225. The first TCP reference singleton was 44.6 writes/s, while its candidate and the remaining reference trials were about 9–10 writes/s. Its measured leader WAL sync/publication totals were 1.16/2.11 seconds versus 6.53/10.85 seconds for its candidate. These physical I/O timings explain why this small campaign cannot attribute singleton changes to the scheduling policy alone; no trial was discarded or rerun. Candidate TCP window-32 trials also varied from 604 to 1,645 writes/s.

| M2 Max case | Reference writes/s | Candidate writes/s | Median paired ratio | Reference p99 ms | Candidate p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| TCP, window 32 | 56.2 | 255.1 | 4.54× | 1,004.4 | 147.0 |
| QUIC, window 32 | 53.4 | 251.0 | 4.71× | 1,123.1 | 142.5 |
| TCP, singleton | 11.67 | 11.34 | 0.974× | 96.2 | 298.4 |
| QUIC, singleton | 11.46 | 11.67 | 1.022× | 93.8 | 95.8 |

Every M2 Max window-32 candidate trial also records 16 width-16 proposal visits and 32 leader sync/publications. Its reference records 226–241 visits, comprising 224 singles plus two width-16 visits or 240 singles plus one width-16 visit, and 255–271 leader sync/publications. All singleton trials retain width one and 512 leader sync/publications. The M2 TCP singleton p99 regression is explicit: candidate trials were 95.2, 298.4 and 348.9 ms versus reference 94.7, 97.0 and 96.2 ms. The M2 TCP window-32 candidate p99 also varied from 130.9 to 771.8 ms. No trial is discarded; the raw results retain these tails.

The M2 Max uses the exact recorded 1,353-input reference and the 1,358-file main-package 306b26b candidate. Every shared candidate source hash equals the Linux candidate; Linux additionally captured clippy.toml. The Mac qualifier originally omitted that file, then reran all four root strict profiles and the matched-target strict check with its exact recorded config supplied separately, without modifying compiled sources. An initial native candidate inherited framework-only thin-LTO/codegen1 flags; this mismatch was caught before timing and remains archived as invalid for paired-profile use. A genuine fresh isolated native build with default LTO/codegen settings replaced it. All 24 M2 native timings use the matching-profile executable (`e7e248b2`), never the initial artifact (`1db48936`). Matching compiler-artifact/source/library/executable proofs and the separately qualified test-only matched-harness overlay are recorded distinctly.

This is a focused durable service before/after campaign, not a matched OpenRaft comparison, sustained-capacity claim, or published minimum qualification. Shared-device I/O is substantial, instrumentation is enabled, there are only three paired trials, and neither broad payloads nor remote replica devices are measured. Raw commands, all 557 original files per host, original exits, receipts, physical counters, source manifests and compiler-artifact proofs are in [measurements](measurements/).
