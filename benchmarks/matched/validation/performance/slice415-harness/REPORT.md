# Linux replication-progress comparison

The short paired campaign completed all 18 cases. Slice 415 raises concurrent
capacity by 11.27% in this run, while singleton throughput falls 2.99%. Neither
result establishes sustained acceptance or the required OpenRaft margin.

| Cell | Slice 414 | Slice 415 | Untouched OpenRaft | 415 versus 414 |
| --- | ---: | ---: | ---: | ---: |
| 1 client, batch 1 | 107,101 writes/s | 103,902 writes/s | 22,118 writes/s | −2.99% |
| 4,096 clients, batch 4 | 829,252 writes/s | 922,734 writes/s | 3,206,337 writes/s | +11.27% |

These are medians of three interleaved trials on the same Linux host. Each case
contains 131,072 measured originals and 1,024 separate warmup originals. The
candidate returns every original applied result, all three replicas reach
frontier 132,097, and weighted proposal histograms account for 132,096 originals.
Batch-four counts entries rather than the 32,768 requests. Both candidates retain
the same 4,096/1,024 proposal/replication envelopes; the untouched pinned upstream
keeps its defaults. Default maintenance remains active.

Median request p99 is 19.617→16.732 μs for singleton and 24.744→23.037 ms for
batch-four. All candidate concurrent trials observe width 4,096. Candidate
concurrent leader persistence counts are 2,337–2,380, including warmup, versus
2,394–2,445 for the baseline; these are volatile persistence transitions, not
disk synchronization calls. Snapshot installations total 14–20 per candidate
concurrent trial and 9–14 per baseline trial. These observations do not isolate
the cause of the throughput difference.

Baseline production is frozen `306b26b`, with harness SHA-256
`334b2aaec04cf14877af756710b17ddaf405ba8497dab5da76d57de4ef44e79f`.
Candidate production and harness are frozen `e4045ff`, with harness SHA-256
`9636eea2f28319e067d48d8cc988101ea3ee995f9d4ccd8a8eca5be00b05508d`.
Untouched upstream is commit `e88bb40707f2e511d8ecc44d31edcb1d814a11a8`.
All before/after source and artifact guards pass. Independent compiler records
confirm fresh compilation, optimization level 3, assertions and overflow checks.
Matched harnesses use the same thin LTO/single codegen-unit profile. The separate
native candidate explicitly restores the baseline's default native LTO/codegen
tuning; its artifact is prepared, not measured by this framework campaign.

The framework uses three in-process Raft actors, an in-memory store, empty
commands and a no-op application, with one client worker and sixteen server
workers. It does not measure real PUT payloads, network sockets or disk durability.
Bash reports elapsed/user/system time; RSS is unavailable. Queue peaks and retained
entries are present in the raw diagnostics, but these short cases cannot establish
bounded long-run maintenance debt or the complete seven-cell curve.

The concurrent candidate reaches about 28.8% of matched upstream and 11.0% of
the 8,422,500 writes/s minimum margin. P9 remains active. Next work attributes
remaining Core/host costs and restores batching through the actual Counter service
wrapper before a broader persistence pipeline redesign.

[Exact trials and guards](linux-paired131072/), [compiler/source records](artifacts-linux/),
[checked summary](SUMMARY.json), [reproduction driver](run-linux-paired.sh).

The initial export attempted an absent `specs` path and failed before compilation.
The corrected export uses verified tracked inputs. The initial compiler-record
parser also rejected Cargo build-script text; the corrected parser accepts JSON
records explicitly and verifies all three actual artifact profiles. Neither
failed preparation produced an accepted timing case.
