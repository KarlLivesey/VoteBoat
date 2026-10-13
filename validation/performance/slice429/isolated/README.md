# Isolated429 exact zero-byte equality qualification

Fresh published428 a46da2b source plus three changed paths/one new integration
module is frozen at1,425 inputs. SOURCE.json/source/overlay inventories and
PATCH.diff (ff376b5f27d028fb89deafd885ec75ef7789918d697146880a29b77c6923ffba)
identify the complete checked source. Held427 supplied ideas, not qualification.
Schema predates edits. No main/docs edits or performance timing were made by this
qualification agent; root owns source promotion and publication.

LogEntry manual PartialEq exhaustively binds every field and payload variant,
checking index/term before exact variant/configuration/operation. Command bytes
skip comparison only when BOTH vectors are empty; otherwise full Vec equality
retains length and every byte. Eq remains. EntryPayload and GroupLog derive Eq
independently. No metadata/digest/pointer shortcut, proof removal, payload/layout,
profile, checkpoint-frequency, ownership, restart, durability or result change.
The independent tuple(index,term,&payload) oracle covers arbitrary128-bit IDs,
headers, all variants, configuration operation/expected/change mismatch, distinct
empty capacities and equal/different nonempty lengths/bytes in both directions.

Source compatibility tradeoff: manual PartialEq loses Rust's compiler-derived
structural const-pattern marker for LogEntry; ordinary equality truth and data
fields/on-disk shape remain unchanged. An explicit inline hint supports public
cross-crate embedding codegen without requiring ThinLTO. It is not inline(always).
Optimized426 bitcode did not retain a standalone equality definition from which
its original annotation could be recovered; no stronger annotation proof is claimed.

Original21041 terminated101: all-feature library386/0 (223.17s), owner201pass/
2startupdeadline failures (325.80s). The hundred-compacted initial drain at
10s and owning-native-facade initial progress at15s expired with Node3 snapshot/
persistence leases still owned. No Eq oracle/provider failure was observed.
Root held a quiet window; unchanged exact controls under original60187 pass
57.06s/77.43s with the same source, budgets and deadlines. No cause is asserted.
All raw failures/states/commands/statuses remain in attempt1 and pre-inline.

Only after that boundary, the inline attribute was added. INLINE_CHANGE.json
proves the Eq body remains byte-identical after removing the attribute and
records both source hashes. Do not describe the earlier broad386 library or203
covered owner controls as exact-final-file tests. Final original67406 is
terminal0/DONE; QUALIFICATION.json records the actual final-source scopes:

| Final source scope | Passed | Failed |
| --- | ---: | ---: |
| All-feature focused log/checkpoint/native accepted library | 37 / 2 / 18 | 0 |
| All-feature log/provider/Raft/snapshot | 19 / 35 / 79 / 27 | 0 |
| Core library/effect_owner | 306 / 195 | 0 |
| Core provider/Raft/snapshot | 20 / 29 / 12 | 0 |
| Actual TCP/QUIC services, retry/replacement/native recovery | 2 | 0 |
| Standalone matched | 35 | 0 |

Root formatting and all four strict Clippy profiles, plus standalone formatting/
strict all-target Clippy, are zero. Initial formatting1 was corrected by the
normal formatter and retained. No lint level, budget or deadline was weakened.
All1,425 source hashes match after checks. CHECK_STATUS101 keeps the original
broad failure; RESUME_STATUS0 plus scoped qualification is the final result.

This regular-file archive includes every command/status/raw output, schema,
source inventories/patch, pre/post-inline evidence and failures/quiet passes.
It excludes source/build/test-temp/generated WAL states and symlinks.
EVIDENCE.sha256 covers archived files. No429 PUTS/s/latency gain, fresh asserted
artifact or Mac qualification is claimed here; P9 full acceptance remains open.
