# Shared validated ordinary-log deltas (409)

Purpose: stage ordinary metadata/pure appends in O(new batch), preserving the
current public GroupLog/LogUpdate contracts and original materialized validator.
Ownership: NextLog owns metadata plus normalized new entries for an eligible
plain transition, or an entire validated GroupLog for complex fallback. Its
predecessor key matches exact canonical/accepted metadata. LogView borrows a
canonical prefix plus already-admitted unsynchronized tail for synchronous
validation only; no borrowing escapes a worker or pending owner.

Admission: private summary rebuilt after validation records exact predecessor
metadata, plain-history eligibility and actual retained-byte aggregate. Snapshot,
configuration or replacement work uses legacy stage_update. Fast admission checks
all existing ballot, revision, suffix/term/index/payload, commit and exhaustion
rules before constructing a plan. Installation verifies predecessor and original
update before mutating canonical state. Caller must additionally provide its
existing exact durable-completion gate; planning is not a durability receipt.

Failure/restart: errors leave canonical log unchanged. Cache mismatch refuses
the transition before admission, never silently trusts stale summary.
Recovery rebuilds summaries from the selected validated log. Native overlays use
their own exact accepted metadata; uncertain persistence still fences outside
this helper. Full fallback preserves existing snapshot/suffix generations and
configuration validation. Only newly added payloads are cloned/normalized.

Acceptance: differential fast+fallback result/error equality against stage_update;
wrong install predecessor or payload leaves log unchanged; old payload pointers
survive plain append/metadata install; huge new payload capacity is normalized;
retained ballot origin/new ballots remain exact; tail view plans multiple accepted
updates; term_at finds canonical and pending entries; cached bytes match rebuilt
canonical summary; configuration/snapshot/replacement fallback stays active.
