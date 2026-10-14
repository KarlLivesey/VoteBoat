# Retained435 persistence ownership audit (read-only)

No new source, API, helper, benchmark or profile was introduced. This audit
checks whether sharing the current pending/effect input would actually remove
the remaining measured entry-copy work, rather than relocate it.

Actual path: proposals.rs44–55 moves command bytes into LogEntry values;
raft.rs1285 invokes OwnedNextLog::plan_copy (log/transition/incoming.rs36–55).
The raw update is validated first, then cloned for the eligible append. Core
Pending owns the copy; Effect::Persist at raft.rs1323 owns the original.
Full-field completion matching at incoming.rs87–105 rejects altered IDs, bytes
and headers. After exact durable completion, Core moves its retained entries
into its canonical history (incoming.rs237–241).

MemoryStore (benchmarks/matched/src/store.rs59, log/owned.rs159) already uses
plan_owned: raw-capacity validation precedes normalization, then the original
tail moves into its canonical history. Native (native/log_store.rs307–349,
native/accepted.rs127/197–229/362–394) preserves fence/identity/outstanding,
encode/length/plan/WAL-reservation order and promotes accepted state only after
synchronization and manifest publication.

Thus the hot append has one deep copy for two independently owned canonical
Vec histories, plus possible Vec relocation. Sharing only pending/effect input
would move that copy to later canonical materialization. Removing it needs
shared immutable canonical backing and an explicit public/legacy view, bounded
lifetime/accounting and snapshot/restart/partial-progress plan. It is a material
ownership/API design, not a proven small copy-free submission seam. Current code
has LogUpdate/LogMutation and owned PersistUnit; no PersistIntent/prepare API.

Saved435 profiles support investigating copying (Linux plan-copy5.078% and entry
clone6.156% overlap; Mac prepared-install memmove2.740%). They do not prove a
particular shared representation or speedup. Do not build a shared-intent helper
that merely relocates this copy. Preserve raw full equality for legacy providers,
forged same-header/altered-ID-or-bytes rejection, binding/error order, fencing,
raw-capacity gates, accepted/durable separation and restart reconstruction.
Full P9 remains active; this read-only audit closes no performance requirement.
