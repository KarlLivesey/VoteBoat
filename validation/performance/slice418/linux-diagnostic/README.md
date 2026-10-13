# 418 Linux short compaction diagnostic

Original exec9545 exits0. Nine rotating cases use frozen published355cfa6
(attribution417, unchanged Core415) against frozen32217ae (compaction418),
candidate attribution off/on. Each returns1,048,576 originals in262,144 batch-four
requests after1024 warmups; all three frontiers are1,049,601. Leader histograms
account for1,049,600 originals and every trial publishes204 checkpoints. Exact
4096/4096/1024 envelopes, active default maintenance, source/executable guards and
all12 reported phase failure counters pass. No build/copy/restart occurred during
timing; other local agents confirmed a CPU-quiet boundary.

| Variant | Median originals/s | Individual trials | Median request p99 |
| --- | ---: | --- | ---: |
| Frozen417 baseline, attribution off | 885,239 | 885,239 / 721,722 / 920,703 | 21.893 ms |
| Frozen418, attribution off | 937,467 | 911,375 / 937,467 / 960,548 | 21.025 ms |
| Frozen418, attribution on | 982,040 | 982,040 / 864,101 / 982,576 | 19.578 ms |

Candidate off absolute median increases5.90%; median within-trial ratio is1.0433.
Each off/baseline pair increases, but there are only three pairs and baseline
trial2 is substantially slower. All outcomes remain in the archive. Attribution-on
ranges overlap and its higher median establishes no observer speedup or precise
overhead bound. This is a minimized volatile in-process framework diagnostic,
not actual key/value PUTs, physical durability, sustained capacity or a published
floor/healthy-margin pass. No upstream trial ran in this specific diagnostic.

Candidate-on leader whole-lifetime median is1089.24ms, including campaign,
warmup, measured work, follower drain and shutdown. Inclusive maintenance records
402.90ms/8995calls; persistence helper297.30ms/4679calls; nested store append
104.08ms/4883calls; direct Core23.17ms/8996calls; admission92.98ms/2341calls;
results84.18ms/2326calls; wait180.94ms/8996calls. These are elapsed intervals,
not CPU or critical-path totals. Store append covers helper and maintenance;
do not sum overlapping/nested phases or subtract incompatible scopes. The older
417 instrumented campaign has different schedules and is not a paired phase
before/after control. Maintenance remains a substantial measured seam.

The actual root libraries and matched/native examples are independently freshly
compiled (fresh:false), opt3 with assertions/overflow enabled, using exactly1384
copied tracked inputs. Matched uses thin LTO/codegen1; native explicitly unsets
those framework overrides and uses native defaults. Native build proof is retained
for the separately prepared artifact; no native timing ran here. Source paths,
compiler identity, original command/script, locks, profiles and executable hashes
are recorded under artifacts/. Runtime guards cover both used matched executables
and immutable source exports. Compiled libraries are identified by actual Cargo
compiler records, not inferred from a requested build command.

cases/ preserves every command/stdout/stderr/exit/Bash elapsed/user/system timer
and guard. RSS is unavailable. SUMMARY.json retains all widths and attribution
records; EVIDENCE.json hashes71 original build/diagnostic records. No executable
or source checkout is included in this archive. Current-source Mac qualification
and both-host sustained healthy-margin acceptance remain separate.
