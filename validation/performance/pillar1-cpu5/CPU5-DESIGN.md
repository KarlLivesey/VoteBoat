# CPU5 private borrowed-view candidate schema

Recorded before production edits, 23 October 2026. Parent base
`8c0fe77fba43020ce0cdcaccdc883bbffd5502c7`; production baseline
`0bf9271ecb5a34c4089c6bb10381303232624170`. Parent independently released this
one candidate after exact frozen canonicalA attribution identified a transient
304-byte metadata snapshot. The earlier source-only NO-GO is resolved only for
that static object fact. CPU benefit and candidate machine elimination are unproved.

## Shape and ownership

`LogView` remains private and `Copy`. Reuse its existing immutable summary/stamp
association: `metadata: Option<LogMetadata>` holds `Some(snapshot)` for `new`
and `trimmed_overlay`, and `None` for `summarized`. Only constructors establish
these states. `metadata()` returns the owned snapshot reference or the exact
associated summary metadata reference. Every metadata reader in view.rs uses
that accessor. No public, matched, provider, wire, Cargo, queue or persistent
shape changes; no unsafe, new allocation owner, generation or completion seam.

The summarized view borrows its base and immutable summary for the existing
planning lifetime. Rust prevents mutation of that summary while borrowed.
Owned append/commit/full plans still take their required owned predecessor,
owned next summary and actual suffix at the same point. No borrowed reference
enters Pending, a store, recovery or install. New/materialized/accepted/trimmed
overlay views own an independent exact snapshot even when it equals another
summary. Copying the view preserves these existing semantics.

## Transitions, failures and cleanup

The lifecycle remains input admission -> checked planning -> sole owned Pending
-> exact volatile completion or admitted durable ticket -> checked install ->
ordered committed application and each original result. The view borrow ends
before staging/install. Existing summary association/stamp checks, foreign/equal
summary metadata comparison, ballot/configuration/revision/group checks, suffix
replacement, mixed/unsealed entries, trim and charge bounds remain intact.
Failures before install leave log/plan unchanged; failed following work retains
already accepted progress and FIFO order. Restart recovers owned durable state,
never a view pointer. Existing required predecessor,672-byte staging aggregate,
272-byte effect representation and336-byte next summary are outside the change.

## Acceptance and linked plans

Macro P1 remains lower CPU per completed original with every implemented extra
above OpenRaft, equal cores and unchanged original gates. P2/P3 stay paused.
The current/next-two mini plan is linked in `docs/IMPLEMENTATION.md`:

1. Builder candidate: depends on parent release/unchanged baseline. Purpose is
   one private representation candidate. Checks: complete metadata plus pointer
   identity for summarized and independent owned views; equivalent full-oracle
   planning/error/install histories, native accepted overlays, public provider
   and normal matched completion controls; compiled old-owned borrowing-negative
   and exact input restoration; sequential features, strict7 and actual enabled
   pre-push hook; frozen checked draft, source/patch/manifest/raw evidence and
   immediate own target/worktree/scratch cleanup. Advances P1, not release Done.
2. Parent source/codegen: depends on frozen publication. Purpose is independent
   exact-head review and the specific Apple copy-elimination check, including no
   replacement materialization/full-view move and retained required ownership.
   Completion: dual approvals and actual codegen evidence. Advances P1.
3. Parent qualification: depends on accepted source/codegen. Purpose is actual
   whole-path benefit and retained semantics. Completion: original9pairs,
   conditional64 and conditional exclusive full5 with owned cleanup. Advances P1.

This schema is planned work, not executed evidence. Introduced access selection,
layout/inlining and possible full-view moves are costs for the parent to inspect.
The pointer witness proves borrowing only; no timing/profile/performance/full
suite/heavy relay campaign is authorized for this builder.
