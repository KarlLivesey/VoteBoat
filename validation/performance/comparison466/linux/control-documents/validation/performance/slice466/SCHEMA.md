# Slice466: immutable batches retained by canonical log history

Before-edit schema, parent9798ed44. Full P9 objective remains active; no new
floor or completion definition. Existing455 CPU data, not a fresh recapture:
651/8395 bulk samples are entry-to-Vec self (7.755%); named clone ancestry
receive plan237/proposal plan118 (4.229% combined), append_for142/reply65
(2.466%). Contexts overlap; these are sampled weights, not allocation counts or
a predicted removable share. Client wrapper self includes construction/poll/send.

Selection: immutable batches must span both pending suffixes and canonical
history. Sharing only Suffix postpones its necessary copy into independent
canonical Vecs. Two independent audits identify that limitation. This slice
changes canonical ownership too; it does not revisit431 tail-transfer variants,
forced inlining, client channels, ready-reply fusion, or benchmark scheduling.

## Data and public composition

- EntryBatch privately owns an optional Arc backing containing the original
  Vec<LogEntry> and immutable checked accounting. Empty owns no backing. Capture
  preserves raw capacity; clone shares, readonly slices/iteration expose no
  mutable backing/Arc. Explicit to_vec gives an independent mutable copy;
  into_vec moves a unique full backing or copies when shared. Independently
  allocated batches compare complete entries; identical immutable backing may
  certify only that same batch's contents. No digest or revision substitute.
- Suffix.entries becomes EntryBatch. Canonical GroupLog.entries becomes
  EntryHistory: an owned sequence of bounded chunk descriptors over EntryBatch,
  with positional endpoints for lookup. Append takes batch ownership without
  element cloning. History clones own their descriptors and share immutable
  backing. EntryHistory edits use explicit replacement/rebuild, never a mutable
  reference into a shared backing; altered public inputs remain independent.
- Prefix/suffix truncation drops unreachable descriptors, retaining only boundary
  spans of their original batches. Full backing is charged for partial spans;
  a tiny visible range never earns a sharing discount. The number/size of pinned
  boundary batches remains bounded by already admitted allocations. Empty spans
  retain no backing. Recovery/import can preserve one already bounded raw batch.
- RPC Append/repair entries stay ordinary owned Vecs in this slice: selected
  outbound ranges materialize exactly as before. Public application apply_batch
  also stays contiguous. Borrowed completion/replay uses a contiguous borrow
  when available and a single owned coalescing fallback otherwise. Never split
  one application commit batch or alter its error/partial-progress boundary.
  Owned committed effects retain their original Vec behavior and result order.

This is one integrated log ownership change, not a prerequisite container-only
milestone. Core, native accepted/durable state and host-owned validator must all
retain shared canonical batches before a copy-removal claim. Public constructors,
borrowed views and accounting are documented; provider traits remain interchangeable.

## Ordering, authority and memory

Existing role/identity/term/configuration and original raw shape/byte validation
precede normalized capture/admission as before. Private pending planning can
share a tight immutable plain batch; spare/configured normalization and full
validation retain their explicit fallback. All headers, operation IDs, payloads,
configurations and snapshots still participate in exact altered-effect checks.
Sharing supplies no admission, quorum, application, fencing or durability authority.

Account every owner's full backing capacity, all retained payloads, Arc/control
allocation and history descriptor capacity, including hidden boundary bytes.
Cache only immutable accounting, never validation authority. Checked overflow
remains an error. Update reservation/inspection formulas for the actual public
shape and conversion/coalescing peaks; do not weaken limits or conceal additional
resource refusals. Inspect original capacity before any normalization. Memory
charging differs from encoded byte length, which stays unchanged.

Pending, provider accepted overlays and durable canonical state retain their
existing lifetimes. Written never upgrades local voting/commit/application.
Exact tickets and barriers control install, old requests/aliases and notification
order remain unchanged. Admission/publication errors retain original inputs or
fence at the same boundary; uncertain native writes are not rolled back. Queue
cancellation/replacement/shutdown drop owned handles without releasing reserved
debt early. Truncation, checkpoints, snapshots and generation changes keep exact
checkpoint-lineage invalidation/preservation rules; cached summaries must reflect
the new physical retained charge, not just visible entry count.

No wire/WAL/snapshot semantic entry format changes. Restart reconstructs new
in-process backing from validated stored entries, never reuses pointer identity
or stale tickets as authority. Configuration and P8 logical/recursive/striped
paths retain complete values; rewriting adapters explicitly capture a new batch.

## Acceptance and disposition

Independent Vec reference controls for arbitrary chunk boundaries, append,
range/lookup, prefix/suffix cuts, replacement, full equality and drop lifetime;
all payload variants/full-width IDs, spare-capacity/error-order and overflow;
partial-span physical charging and shared-to-owned/coalescing peaks. Exact
altered-effect controls must include headers/configuration/payload replacement.
Canonical Core/provider pointer controls establish retained backing across
delayed admission/Durable rather than a later equivalent detach copy.

Run relevant all/core library, log/provider/Raft/snapshot/runtime/application
integration suites, actual TCP/QUIC retry/replacement/native recovery, modeled
publication cuts and standalone matched controls. Keep formatting/four strict
profiles/standalone zero with enabled hook. Preserve every failure and focus fixes.
Commit/push the complete checked slice before fresh asserted artifacts.

Fresh profiles must demonstrate actual reduction in materializations without
equivalent new completion coalescing/detach work. Repeated matched within-host
completed originals/s, latency, refusals and bounded physical memory on Linux
and M2 Max decide retention; include singleton and nonempty/configured controls.
No changed checkpoint frequency, envelopes, clients, outcomes or timer boundary.
No speedup is claimed by this schema. Original Mac455 qualification continues;
full sustained seven-cell/durable/headroom/fairness/placement/P8 scope stays open.
