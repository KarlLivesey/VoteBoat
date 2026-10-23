# CPU5 one construction-order follow-up

Pre-edit schema, 23 October 2026. Actual source starts at published
`a97b4d402b2dc0fd0e6857ffbffcf20e3c4a9639`, not the T3 seed main8c0fe77f.
PR29 stays draft on cpu/p1-borrowed-view. Parent authorizes one production
callsite revision only; all original qualification gates remain mandatory.

## Data, transition and ownership

Keep `OwnedAppend { predecessor: LogMetadata, summary: LogSummary,
suffix: Option<Suffix> }` unchanged. Compute `append_summary(view, summary,
metadata, suffix.as_ref())` into a local first; then construct Self with
`predecessor: *view.metadata()`, that summary and the suffix. The predecessor
is still an exact owned value, not a borrow. `view.rs` remains exact a97.
Other constructors, plain/commit paths, APIs/types/layouts/owners/queues and
validators/error order remain unchanged. No allocation, unsafe or Cargo change.

Independent purity audit: transition.rs append_summary reads plain Copy
metadata/flags/retained charge; LogView::appended_charge dispatches immutable
EntryLog/EntryBatch reads. Canonical appended charge reads adjoining spans,
cached shared backing charge or computes checked owned-entry payload totals.
These paths cannot mutate summary/view/predecessor or publish effects. No
interior-mutability operation or external callback is involved. Moving the
metadata read after this helper preserves its exact value. There is no new
fallible validator; existing overflow remains Option arithmetic in the charge.

Admission -> checked plan -> owned Pending -> exact completion -> checked
install -> ordered application/results is unchanged. Refusal leaves owners
unchanged; accepted-progress/FIFO failure handling and stale tokens retain the
same behavior. Restart reconstructs owned durable state, never a borrowed view.
Suffix ownership/charges, stamps, completion/retirement and cleanup are retained.

## Evidence and acceptance plan

R2 normal Apple build/five view tests passed at a97, but the specific machine
gate FAILED: summary -> stack304 before append_summary -> owned predecessor304.
Normal B SHA25625e5afcff224a4c88cbb1121f6db1fc73ed0deed96c7d4fe7df2073fac8799df.
No Rust benchmark binary ran; diagnostic overlays/controls, nine pairs,
canonical64 and full5 stayed unrun. R1 UNKNOWN/HOLD and every historical HOLD
remain unchanged. The parent revised its initial rejection only for this
precise new dataflow hypothesis; this is no gate waiver or predicted gain.

The linked current/next-two mini plan in docs/IMPLEMENTATION.md advances macro
P1 only: (1) this candidate depends on a97/R2/purity; check existing meaningful
transition/oracle/charge/failure and entries/core/native/public/matched consumers,
sequential default/all-feature plus relevant semantic matrix, strict7 and actual
enabled hook; (2) parent fresh dual reviews/new Apple elimination plus exact
old-order machine control/restoration depend on frozen publication; (3) parent
original9pairs, conditional64 and conditional full5 depend on those gates.
P2/P3 and unrelated roadmap remain paused; preserve all implemented extras
above OpenRaft, including independent groups with no ancestor-per-write duty.

No new Rust test mirrors field order/accessor counts: both orders are valid
semantically. Save an exact old-order-only machine control patch retaining the
borrowed view. Reuse the existing borrowing negative only as unchanged-source
evidence, with hashes, not new codegen proof. No Mac build/codegen, benchmark,
profile, full suite, heavy relay or subagent runs here. If the pure assumption
fails or a substantive unrelated selected test fails, stop and archive; no
broader rewrite/fixture repair/unchanged rerun. All raw checks/resources/full
patch/tree/input manifest and control patch go only to main ignored
target/claude/pillar1-20261022/cpu5-order-followup/, followed by immediate own
target/scratch/worktree/registration/T3 seed cleanup and prune.

## Actual implementation evidence

Implemented only the specified summary-local reorder in OwnedAppend::new.
The owned predecessor remains exact. The entire production delta relative to
a97 is two added lines/one removed line in that function; view.rs retains
SHA25686c1b3831a8989655e56ca72e3fac73500952d4b4982c54c90f8baf20c1bfb37.
Original CPU5-CHECKED is explicitly historical. No test source changed.

All checks below used stable1.98.1, locked/offline dependencies, jobs4,
incremental0, dev/test debug0/opt0/assertions true and one own normal target
shared by root/matched. Feature configurations executed sequentially.

| Existing meaningful checks | Default | All features |
| --- | --- | --- |
| Transition/oracle/ownership/charge/error histories |39 passed |39 passed |
| Entry backing and exact retained charge |20 passed |20 passed |
| Core raft |184 passed |184 passed |
| Native accepted overlays |23 passed |23 passed |
| Public log_owned/log_staging/log_store/raft/volatile |5/2/19/81/11 passed |5/2/19/81/11 passed |
| Normal matched Actor/application |29/2 passed |29/2 passed |

Additional default application23, learners23, logical_voter_application8,
membership32, retirement7 and scopes12 passed. No-default transition39 and
native-only transition39/accepted23 passed. Total1036 test events across the
listed configurations, zero failures/selected ignored; these are not1036
unique tests. Matched has no declared features, so its two requested graphs are
identical. No full5/heavy relay/release-codegen/performance run occurred.

Semantic coverage retains the original CPU5-CHECKED matrix with fresh listed
execution: actual ordered-prefix/durable/stale completion and full-oracle
histories; recursive/weighted/joint/logical-child and learner membership;
retirement/leadership/read-readiness scope checks in core/public targets;
full IDs/dedup/outbox/lineage and application/partial delivery in application,
scopes and Actor; maintenance/checkpoint/snapshot/recovery and accepted subset
publication; bounded public/native providers and independent groups. All other
implemented production capabilities remain source-identical a97; these bounded
checks do not recertify every networking/relay/striped/lifecycle path. Original
conditional full5 and fresh source/codegen/performance gates remain required.

An inverse old-order-only machine patch is saved in the ignored evidence and
git-apply --check passes. Its reconstructed incoming.rs matches exact a97
SHA256c90c4499ad8f8d8c35f58c60a8ddf3139d24ddc3fc20ac5245db9334b9763aad.
It retains borrowed view source and is not expected to fail ordinary tests.
Old borrowing negative/control/restoration receipts are reused by hashes on
unchanged view source only, not as new-head machine or performance proof.

Frozen build-input manifest1532 SHA256
1bdd11f6236b36ed79a52522564e232b0ac29254eca1439d75c80d7f486eaf08
records sizes/SHA256/modes/Git blobs. Only incoming.rs differs from the original
a97 build-input set;139 include targets including actual TLS concat are checked
against that membership. Source/config/include closure is inherited from the
unchanged audited inputs, not claimed as a new external dependency audit.

Strict7 passed with zero diagnostics: root formatting, all-target strict
Clippy default/all/no-default/native-only, matched formatting and its separate
locked/offline all-target strict Clippy. The actual enabled nonforce pre-push
hook is a separate publication condition; its actual result, exact published
head/tree/full patch and final resource/
cleanup closure are retained in the ignored REPORT.md and PR29 CURRENT body.
No machine elimination, CPU benefit, full feature certification or complete
Raft claim follows. Both a97 source reviews are historical; fresh dual reviews,
new normal Apple elimination plus old-order negative/restoration, original9pairs,
conditional64 and conditional full5 remain PENDING and parent-owned.

Preparation failures retained: missing initial receipt directory; nonexistent
memory/search operands corrected by bounded discovery; Cargo executable path
missing before any Cargo child, then resolved to /usr/bin/cargo. No Rust test
failure or unrelated fixture repair occurred. Raw outputs/resources/control,
purity/order delta and historical hashes are in the authorized ignored evidence.
Initial/bootstrap/tool/read, between-sample/short-lived/reparented/foreign/
inaccessible/T3-model scopes are visibility limits; later clean samples do not
clear historical HOLDs or establish whole-task/global certification.
