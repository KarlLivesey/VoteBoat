# PR32 P2: consumed snapshot input ring destruction

24 October 2026. Focused correction relative to reviewed head
`1237b2154a3574dcc5cfa0bdad059d2f5db9b776`. Exact seed tree and all2606
frozen build-input hashes/sizes/modes matched before edits. Parent main remained
read-only at264522d5. Review A5636031778 withheld source approval for this P2.

Both LogSeam::persist arms retain exact one-effect and variant validation, take
the same genuinely owned LogUpdate/token, and now explicitly destroy the empty
input queue before provider/completion work or creation of following output.
No payload copy, output reordering, provider/barrier/ticket/fence change, new
capacity, allowance, admission reduction or release metadata is introduced.
The budget ledger now includes the previously omitted old/new simultaneous
owner sets and the reviewed four-slot1088-byte dead-input overlap.

The nearby helper-family audit found only these two consumed-single-input
persistence arms. compact_with/stage_with call them; finish-install returns the
core's new output after validated restore; supply/worker/legacy helpers return
or explicitly export owned output. No equivalent single-ring lifetime extension
was found there; no broad rewrite was made.

## Actual focused evidence

Four new unit regressions passed for durable/volatile leader compaction and
snapshot installation, using the real public helpers, real LogStore append/
barrier, actual volatile completion and following queue construction. They
assert the real input ring is destroyed, observe actual four-slot capacity, and
retain continuation shape, snapshot boundary, restored value and post-restore
acknowledgement checks.

The safe heap-free cfg(test) RAII field is declared after the actual VecDeque
owner; Rust destroys the slots before the witness. Only a scoped watched input
arms it. The thread-local observation clears on unwind; no allocator hook,
unsafe code, runtime lifecycle counter, per-write heap or release field exists.
Unit-test header accounting includes the witness; the existing downstream
doctest now independently asserts the unchanged release header. This observes
the helper owner's destruction, not RSS, allocator caching, compiler movement
or arbitrary exported/reused rings outside these watched helper paths.

The compiled mutant removes just the two explicit drops, restoring the old
implicit late destruction. All4 tests fail their actual lifetime assertion:
durable paths at provider append, volatile paths at following queue construction,
each detecting1088 still-owned dead input bytes. Child101, no compile failure,
no observer errors. All2607 normal build inputs/modes restored with zero
mismatches, normal4 passed again, and the rebuilt consumer matched the exact
normal binary hash/size/mode. The expected negative is not a healthy test pass.

| Executed check | Result |
| --- | --- |
| raft-queue | 188 passed / 0 failed |
| volatile | 11 passed / 0 failed |
| release-layout-borrow | 2 passed / 0 failed |
| providers-snapshots-workers | 211 passed / 0 failed, 5 passed / 0 failed, 2 passed / 0 failed, 42 passed / 0 failed, 57 passed / 0 failed, 27 passed / 0 failed, 14 passed / 0 failed, 9 passed / 0 failed |
| lifetime-no-default | 4 passed / 0 failed |
| matched-checkpoint-catchup | 47 passed / 0 failed |

The matched check covers all47 existing Actor tests, including all three real
applications at widths1/231/515 through two default5000 checkpoints, follower
snapshot catch-up, resumed writes, state reconstruction and owned shutdown.
The367-event integration set includes real checkpoint/native-file recovery,
provider independent inputs/terminal credits, worker failure/partial progress,
effect-owner capacity/callback/control reservations and snapshot ordering.
Counts are test events with duplicates, not unique tests or a full proof.
Strict7 passed with zero diagnostics. The actual enabled tracked hook/nonforce
push is the publication gate; its exact result is retained in the terminal
publication receipt and PR CURRENT block.

VALID/six-mutant mechanisms were not rerun: staging, actual Actor extraction/
FIFO, rollback, token identity, queue reserve/front drain and runtime capacity
charge plus their independent killing oracles are source-equivalent. The
test-only header probe is freshly qualified by downstream release layout/
borrow doctests. Earlier compiled controls remain explicitly old1237 evidence,
not changed-head runs. See MECHANISM-EQUIVALENCE.json and the new late-drop
negative. All implemented quorum DURABLE/membership/read/transfer/recovery/
lifecycle/lineage/independent-prefix/budget algorithms are preserved; this narrow
matrix does not requalify every feature independently.

Full raw logs, negative/restoration, source/build-consumer hash-size-mode
closure, final patch/tree/commit, resource receipts and cleanup are retained in
main-ignored `target/claude/pillar1-20261022/cpu9-snapshot-p2-fix/`.
Prior evidence is read-only. A final artifact receipt initially failed when consumer metadata shadowed the reader events variable; its raw error/source are retained. Renaming only that metadata variable restored terminal self-audit, and the tree, patches and build closure matched exactly. That finalization visibility gap is not retrospectively cleared. Initial uncontained reads, harmless missing paths/
no-match navigation and truncated displays are setup limitations retained under
the R4 amendment. No retrospective/global resource qualification is claimed.

## Parent-owned gates still pending

Fresh exact-changed-head source confirmations are required. Source A remains
withheld until fresh review; parallel B and the adapter audit pinned to1237
cannot approve this changed head. Parent owns reconciliation, original-R3
adapter confirmation, Apple complete-flow physical proof/VALID/semantic/
restoration, original nine CPU pairs per width, conditional64 and exclusive
full5/integration. No driver change, Apple build/profile/scored timing/heavy
relay/full5/subagent/successor/watch/poll or merge was performed. No speed claim;
physical removal, gain and CPU7 attribution remain UNKNOWN. RPL-1.5 and the
original design pack are preserved.
