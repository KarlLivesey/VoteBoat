# Mac440 completed paired measurements

Original bulk27475/cc0c9e and singleton17036/b03848 actually ended0. All24
cases completed once, with identity-before/after and output checks passing.
Bulk:19,988,480 completed originals/4,997,120 requests plus1024 warmups, all
frontiers19,989,505. Singleton:1,048,576 completed originals/requests plus1024
warmups, all frontiers1,049,601. No interim copies/checks/builds/profiling or
analysis overlapped the whole Mac campaign. Preserve every trial and tail.

| Cell | Engine | Median completed originals/s | Request p99 |
| --- | --- | ---: | ---: |
|4096/batch4|435 OFF|4,707,438|4.124000ms|
|4096/batch4|440 OFF|4,718,780|4.065667ms|
|4096/batch4|440 ON|4,668,695|4.056583ms|
|4096/batch4|OpenRaft|1,987,056|10.505416ms|
|1/batch1|435 OFF|56,679|30.625µs|
|1/batch1|440 OFF|56,527|31.000µs|
|1/batch1|440 ON|52,095|32.334µs|
|1/batch1|OpenRaft|9,920|134.958µs|

Bulk OFF rate ratios are0.954204/1.002409/0.987461: two regressions. Paired
p99 is3.924250→4.034375ms,4.124000→4.065667ms,4.128500→4.096291ms.
ON p99 tails4.056583/4.042875/4.061334ms remain. Singleton OFF ratios are
0.976049/1.006622/0.986835; p99 is29.708→30.708µs,31.167→31.000µs,
30.625→31.125µs. ON tails31.292/34.750/32.334µs remain. These do not
establish repeatable improvement across both cells. Root owns the final
both-host disposition; no variant retune or performance acceptance claim.

Candidate OFF bulk checkpoint counts3884–3895 and snapshot installs0–1674
establish active maintenance/catch-up. Final logical retention1000–5776,
peak8036–22108, peer queue4–8, client queue0–4096 and proposal width0–16384
are logical counters. Singleton records209 checkpoints, retention5601/peak6000,
peer queue1–3 and client queue/proposal width0–1. No RSS, physical allocation
or separate refusal counter was measured. CLI stderr is empty; all requested
originals complete. Inclusive attribution wall intervals are not additive CPU
cost. Batch4 throughput remains below8,422,500 originals/s; singleton is a
short diagnostic, and full P9 requirements remain unmet.

CAMPAIGN_POLL_ERROR preserves a status-only missing pair-root/STAGE path
(error66438); correct singleton-results/STAGE polling resumed without campaign
restart. ANALYSIS_TOOL_WARNING preserves local lexical warnings from adapting
the inherited script; actual remote analysis37505 exited0. All timed raw JSON,
commands, elapsed/user/system times, exits, errors, identities and checker
records remain. README.md is the unchanged prepared record;
PREPARED_ONLY_ORIGINAL.sha256 inventories its sealed bytes, while SHA256SUMS
checks the full current archive. No binaries/build trees/WAL states included.
