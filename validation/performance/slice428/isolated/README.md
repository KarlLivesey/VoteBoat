# Isolated428 qualification

The selected published426 production source (exported at docs-only commit
92f8f714c30c6775cccc2369f3506e54e6929deb) plus the protected checkpoint overlay
is checked in isolation. Main production is unchanged; promotion, publication,
Mac qualification and performance measurements remain pending. No throughput or
latency gain is claimed.

[Schema](SCHEMA.md), [frozen inputs](SOURCE.json), source/overlay inventories and
[patch](PATCH.diff) identify 1,424 selected inputs, 30 changed paths and six new
files. Raw arbitrary-state equality remains full Eq. The new object-safe sealed
view permits only private owner/provider proof reuse, exact current headers and
new entries, certified append/compaction survivor transfer and full fallback for
unknown changes, replacement and recovery. No proof is a durability token.

[Qualification](QUALIFICATION.json) records commands, exits, log hashes and exact
source reuse. The final focused runner36555 terminated0 with CHECK_STAGE=DONE
and FINAL_MATCHED_STATUS=0. CHECK_STATUS=101 intentionally preserves the prior
runner's standalone fixture Clippy failure; it is not an aggregate final result.

| Covered functional scope | Passed | Failed after focused correction/retry |
| --- | ---: | ---: |
| All-feature library | 386 | 0 |
| All-feature effect_owner | 203 | 0 |
| All-feature log/provider/Raft/snapshot | 19 / 32 / 79 / 27 | 0 |
| Core library/effect_owner | 306 / 195 | 0 |
| Core provider/Raft/snapshot | 17 / 29 / 12 | 0 |
| Actual TCP/QUIC service retry/replacement/native recovery | 2 | 0 |
| Standalone matched harness | 35 | 0 |

These are exact scoped coverage counts, not a claim of one repeated clean full
suite. All-feature library384 passes are reused after correcting two new fixture
assumptions; both corrected fixtures and related controls pass. The all-feature
owner run passed202 controls and the existing hundred-group authenticated
transport control exceeded its initial10s drive deadline. An exact quiet rerun
passed in37.11s without source, timeout or budget changes. This transient failure
and original ownership diagnostics remain in attempt3/raw logs; no cause is
asserted. The other202 owner controls are not needlessly repeated. After a
fixture-only report assertion extraction, the affected matched actor control is
rerun; the other34 passes retain identical production sources.

Root formatting and all four strict Clippy profiles, plus standalone formatting
and strict all-target Clippy, pass with zero diagnostics. The metadata checker
has its own final command/status/log; its focused ledger correction changes only
documentation, so compiler/test scopes are reused by exact source hashes.
Attempts1–5 retain the original private-import compile error, two fixture
failures, drive timeout and standalone cognitive-complexity failure, their source
inventories/patches and focused fixes. No lint level or deadline was weakened. The initial metadata checker also
rejected unsupported src-unit/matched test paths; the invalid ledger remains in
METADATA_LEDGER_BEFORE.json. Its exact raw tool output was unavailable, so
METADATA_INITIAL_FAILURE_REPORTED.md records the reported terminal failure
without reconstructing or inventing a log.

A warmed protected-cache history through configured rollback or nonmatching
snapshot installation was not run explicitly. Generic same-existing-core Full
replacement and fresh-owner controls plus the existing configured/snapshot
histories cover those implementation paths; they do not establish those more
specific warmed-cache cases. Independent review found no blocking defect.

[Dev oracle proof](DEV_ORACLE.json) includes actual Cargo build-script JSON and
both root rustc invocations with voteboat_checkpoint_debug_oracle. New build.rs
selects the redundant full Eq oracle for Cargo nonrelease profiles, conservatively
including unknown/missing PROFILE; unit tests always include it. Release exactness
rests on protected ownership and mutation proof. Normal assertions and overflow
checks remain enabled in the performance profile. Actual release-profile gate
and fresh library/executable/upstream artifact proof are pending. Future source
exports and artifact guards must include build.rs.

This archive contains regular evidence files, all attempts, raw outputs,
commands/statuses, runners, schema, proof, inventories and patches. It excludes
source trees, compiler outputs and generated test/WAL states. EVIDENCE.sha256
covers the archived files; absolute paths inside raw commands are historical
provenance, not required external files or symlink dependencies.
