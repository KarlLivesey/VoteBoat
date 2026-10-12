# Frozen396 Mac startup qualification

| Original full target | Result | Duration |
|---|---:|---:|
| All features |57 passed,0 failed|98.43s|
| Default features |42 passed,0 failed|46.20s|

Each target ran once,sequentially,on exact same3961306 source as the
[Linux archive](../README.md). Formatting/allfourstrict passed. Actual fresh
release opt3 test/library records prove assertions and overflow checks enabled;
feature vectors and actual57/42 lists distinguish the profiles. Original
source/executable/library/protoc/transfer guards passed before/after; priorALL
artifacts stayed unchanged across DEFAULT build. Old393/394 source and five
binaries were independently guarded; their caches were never cloned or mutated.

See [all-feature log](original/all.log), [default log](original/default.log),
[actual profiles/guards](provenance/QUALIFICATION.json) and
[exact source hashes](provenance/SOURCE.json). Matching frozen
[startup harness](../source-fixtures/tests/startup.rs),
[local drain](../source-fixtures/tests/startup/maintenance/drain.rs) and
[membership drain](../source-fixtures/tests/startup/maintenance/membership_drain.rs)
preserve original outcomes,joins,rebinds and clocks. Production379 is unchanged;
current397/398 fixtures and fullcurrent/P8 acceptance are excluded.

The [earlier395 failure](../prior395/ALL.log) remains unknown: its non-Applied
variant was unprinted. Changed396 passes do not retroactively prove that cause.
This Mac subtree has its own [copy/hash map](COPY_MAP.json) and
[checksum manifest](evidence.sha256); existing Linux files remain untouched.
Duplicate guard logs are exact-byte aliases recorded in the map. Original
scripts/absolute commands are historical records. No binaries,caches or jobs
were added during archival work.
