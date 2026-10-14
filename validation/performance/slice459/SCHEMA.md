# Slice459: count the smaller compaction range

Pre-edit schema from published4583793b965, with production455 restored by457. Current455 Linux folded CPU stacks place131/8395 selected sample weights under compact::retained_after → retained_payload_bytes (1.560%). Append accounting166 and outbound65 remain separate. Existing whole-launch sampling loss/limits remain; this is a measured context, not a predicted speedup. Rejected456 inline hint is not reused.

## Data, ownership and transitions

No new data field, cache, public API, dependency or durable representation. The private compaction planner already holds an immutable exact LogView and version-bound LogSummary. Eligible history is plain/clone-stable with no snapshot membership allocation. Its retained charge is nonnegative inline-entry bytes plus each immutable payload’s full conservative charge; sharing has no discount.

Today retained_after always traverses the removed prefix to subtract its charge, then falls back to counting the surviving suffix when the old sum is unavailable or subtraction fails. Select the shorter entry range: use prefix subtraction only when a cached sum exists and removed<=left; otherwise count the surviving suffix directly. Keep checked multiplication/addition/subtraction and the existing recomputation fallback. This removes discarded-prefix accounting work when most of a log is trimmed. A trimmed-overlay view joins its remaining canonical slice and accepted tail; skip must refer to that logical view, not physical base indices.

Planning remains read-only. Existing complete validation/error order precedes accounting; prepared install retains its exact predecessor/reference check, drains exactly the same prefix and installs the same metadata/charge. No checkpoint cadence, comparison, operation outcomes, fences, snapshots, memory limit or admission rule changes. No new ownership token or durability permission. Restart continues to rebuild summaries from canonical recovered state; partial failure leaves original state unchanged through the existing batch preflight.

## Acceptance and retention

Before editing, preserve original source and this schema. Compare every trim boundary of mixed empty/nonempty/noop entries against the materialized full validator and independently recomputed exact summary; include prefix-smaller, tail-smaller, equal and empty-tail, cached-overflow recovery and trimmed base-plus-tail overlays. Existing stale summary/predecessor/reference, ballot/commit/generation/error ordering, atomic multi-group failure and payload-pointer checks remain. Reuse existing tests rather than duplicate helpers.

Run affected log/core-compaction controls in all/core configurations sequentially, log-store/provider/snapshot integration and actual native TCP/QUIC retry/recovery controls, standalone matched controls, formatter/four strict profiles/standalone strict with enabled hook. Production storage planning changes require existing crash/recovery integrations in this affected scope; no manifest or crash boundary changes. Preserve all failures and focused fixes. After publication, fresh asserted framework artifacts and paired both-host455/candidate measurements decide retention; no gain is claimed from fewer source-level iterations or passing tests. Keep current Mac455 original qualification and native458 source frozen. Do not build/check/archive/copy on a host during its timing window.

This bounded optimization advances P9.1/P9.3. Native offered-load, physical memory/debt, fairness/skew, all seven framework floors, placement/shared barriers and P8 composition remain separate required work. Macro scope is unchanged.
