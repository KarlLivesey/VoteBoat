# 419 ordinary incoming ownership: Linux diagnostic

Nine rotating paired cases compare frozen `32217ae` (418) with qualified
`7511e5e` (419). Original runner41285 completed once with exit0; no local builds,
tests or copies overlapped timing. Every case preserves1,048,576 measured
originals,262,144 batch-four requests,1024 warmup originals, all three applied
frontiers1,049,601 and204 leader checkpoints. Weighted proposal histograms,
4096/4096/1024 envelopes and all phase failure counters pass. Before/after guards
verify both libraries and executables and all1384/1390 frozen source inputs.

| Variant | Median originals/s | Three original trials | Median request p99 |
| --- | ---: | --- | ---: |
| Frozen418 baseline | 977,881 | 945,149 / 977,881 / 1,007,932 | 19.377ms |
| Frozen419, attribution off | 1,247,186 | 1,247,186 / 1,294,472 / 1,222,884 | 15.927ms |
| Frozen419, attribution on | 1,236,259 | 1,261,933 / 1,236,259 / 1,222,940 | 16.003ms |

All three off pairs improve: +32.0%,+32.4%,+21.3%; median within-trial ratio1.3196.
The absolute median rate increases27.5%. These are different summaries of the
same trials, not confidence bounds. Proposal batches remain2327/2302/2284 versus
2348/2328/2340; original accounting and width caps stay unchanged. No slower
trial is discarded. On/off ranges do not establish precise observer overhead.

Candidate-on median leader lifetime866.59ms includes campaign,warmup,measurement,
follower drain and shutdown. Inclusive maintenance406.25ms remains substantial;
persistence helper114.94ms contains store append9.10ms. Admission94.39ms,
results85.04ms, Core direct17.78ms and host wait133.78ms are elapsed wall
intervals with nesting and overlapping replicas. Do not sum them or compare
historical unpaired phase totals as causal evidence. The next instrumented
split measures actual checkpoint provider calls and helper residual work.

This is short volatile, zero-payload, in-process no-op quorum/application
framework work. It is not physical durability, key/value PUT throughput,
sustained capacity, an upstream comparison, or a pass of the8,422,500/s healthy
batch floor. Mac throughput and the full sustained curve remain separate.
The legacy capacity labels have a verified request-versus-original unit mismatch
against OpenRaft; [the comparison contract](../../../../benchmarks/matched/COMPARISON.md)
records it. Both VoteBoat variants here use identical original-entry limits.

`cases/` retains every raw result, command, exit, timer and guard plus the derived
full trial summary. `proof/` retains exact source maps, genuine fresh root-library
and executable compiler records, host/compiler metadata and guarded hashes.
Both matched release profiles use opt3, assertions/overflow checks, thin LTO and
one codegen unit. No executable, library or source checkout is published here.
