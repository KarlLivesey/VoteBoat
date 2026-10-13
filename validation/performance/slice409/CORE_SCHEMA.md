# Core log delta integration

Purpose: remove retained-history copies and repeated configuration scans from
ordinary append and metadata updates. Advances P9.1; no wire/disk/API changes.

Data/API: keep authoritative durable GroupLog and state()->&GroupLog. Pending
owns the exact LogUpdate, planned Delta/Full transition, optional changed
membership, original ticket, after-action and reply. Core holds a private
revision-scoped summary built only after existing recovery validation. Ordinary
delta membership borrows the existing validated membership; complex Full retains
the original replay result. Pending metadata/term lookup replaces full views.

Transitions: plan before installing pending; use exact proposed metadata for
ticket admission. Only matching DurableLog completion installs the delta into
the canonical log and updates its summary. Wrong/stale/missing completion leaves
state unchanged. Storage failure fences and drops pending through the existing
failure path. No Written completion becomes local voter progress or a result.

Ownership: Pending owns new entries; canonical prefix remains untouched while
pending. New payloads retain the legacy clone's normalized capacity. Completion
moves only the bounded new suffix. Snapshot/configuration/replacement uses Full
and rebuilds summaries. No lazy materialized log can hide a per-event clone.

Derived state: exact plain history permits bootstrap-only rollback/connection
inspection and cached conservative retained-byte charge. Version mismatch never
uses a stale summary. All complex histories preserve the original inspection
and connection budgets until a separately qualified optimization.

Checks: original affected Raft/membership/readiness/repair/snapshot tests;
pending state unchanged + exact ticket and failure controls; payload pointer
preservation for metadata and append; summary charge against legacy scans;
configuration Full fallback and rebuild; all four strict lints and fmt. Compare
the same framework workload with the previous committed source after correctness
is terminal; report CPU diagnostic separately from durable/matched acceptance.
