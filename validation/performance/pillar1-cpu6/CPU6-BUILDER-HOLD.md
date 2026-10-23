# CPU6 builder: unpublished scope hold

Base `5fda857d2f827a47925c152f227bce7cbc1ced98`, production0bf unchanged.
One prepared-batch WIP was implemented in the isolated T3 checkout, without
rejected source, unsafe, dependency/feature/lock/hook/lint changes.

First focused default entries:23 passed. Normal-size probes:2 passed. Sizes in
bytes: EntryBatch64, Span24, Suffix80, Rpc96, Message272, LogUpdate288,
OwnedNextLog720, Pending1168, Effect288. Later cfg(test) work instrumentation
was not run; these are initial receipts, not final-head qualification.

Strict default all-target Clippy exits101 at unchanged
`src/logical_voter/types.rs:305`: Protocol(Message272) versus56-byte alternatives
crosses the large-enum diagnostic. No boxing/new logical-voter allocation,
padding, lint threshold change or workaround was applied. Dependent checks
and publication stopped under the bounded task. The safe uncommitted patch,
tracked/untracked and full build-input manifests, raw commands/results and
cleanup receipts are under main's ignored
`target/claude/pillar1-20261022/cpu6-builder/`.

No commit/push/PR or enabled push-hook run; no semantic mutants/valid old-work
controls, full feature/checkpoint/native/recovery matrix, Apple proof, CPU
pairs, canonical campaign or full-five gate is claimed. No gain is known.
Parent owns the next scope decision; historical holds remain unchanged.
