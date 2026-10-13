# P9 first core staging slice
Purpose: remove duplicate transition materialization while retaining every
existing consensus and storage validator. Advances P9.1 cheap ordered execution;
not sufficient to meet OpenRaft minimum curve.

Shape: Core::persist_update calls crate-internal stage_update(&durable, &update,
limits) -> owned GroupLog. This replaces the singleton map, initial durable clone
and apply_batch map clone. One original Pending still owns update/next/membership
until original admission/completion. No batching, new effect, wire or disk change.

Transitions: same validation -> membership checks -> Pending -> Persist effect.
Invalid update leaves durable/pending unchanged as before. Valid transition still
waits for exact durable completion; no earlier send/apply/client result.

Ownership and restart: original GroupLog remains durable until completion; next
owns its private cloned data. Same persisted update is written. Recovery semantics
and binding/session/generation rules unchanged. No cancellation/token shortcuts.

Checks: existing Raft/reference/storage/worker/membership/striped matrices plus
focused wrong-group and multi-group atomic staging controls. Strict four-feature
Clippy/fmt zero. Baseline source337c235 frozen in target/p9/reference before edits.
Matched volatile store diagnostic isolates copying, no real-durability claim.
Mac functional storage/core controls qualify before asserting platform support.
