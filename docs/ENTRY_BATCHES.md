# Immutable entry ownership

`Suffix.entries` uses `EntryBatch`; `GroupLog.entries` uses `EntryHistory`.
These public values carry entries, never admission or durability authority.
Provider traits, exact tickets, quorum rules, and wire/WAL entry encodings retain
their existing contracts. Performance qualification is recorded in `P9.md`.

Capture an owned `Vec<LogEntry>` with `.into()` or collect owned entries.
`EntryBatch` preserves its input capacity, including reserved empty vectors.
Clones share immutable entries; slices, indexing and iteration are read-only.
`to_vec()` makes an independent mutable export. `into_vec()` recovers unique
backing or copies shared entries. Capture a changed vector as a new batch;
there is no mutable slice or public Arc escape. Equality checks complete values,
using identity only for the same immutable backing.

`EntryHistory` owns chunk descriptors over batches. `append_batch` and
`append_history` retain backing without copying its entries. Cloning a history
copies descriptors while retaining immutable backing. `get`, indexing and
`iter` expose logical positions; equality ignores chunk boundaries. Use
`truncate`, `discard_prefix` or `clear` to remove positions, and `replace` to
capture an independently changed entry. `replace` copies the visible contents
of that entry's chunk, leaving other owners unchanged.

`range` returns `None` for invalid bounds; `view` panics for invalid bounds.
Both return a borrowed `EntryRange`, with exact-size iteration in either
direction. `as_contiguous` borrows only a range contained within one chunk.
`to_cow` borrows that range or creates one complete contiguous vector spanning
chunks. `CompletionEffects::into_parts` and `Raft::replay_committed` now return
these contiguous `Cow` intervals. Application `apply_batch(&[LogEntry])` and
owned `Effect::Committed(Vec<LogEntry>)` retain their contracts: one commit
interval is never split into separate application calls to avoid copying.
RPC Append/repair messages still carry owned vectors.

`allocation_bytes()` returns a checked physical allocation charge, excluding
the value's inline handle. Each batch owner charges its entire backing: Arc
control/layout, entry-vector capacity and every retained payload. Each history
additionally charges its actual descriptor capacity and the full backing of
every retained span. Sharing earns no discount. A partial span pins and charges
hidden entries and spare capacity; removing all its positions releases that
backing. An empty history can retain descriptor capacity after clearing.

Repeated suffix replacement can leave several partial backing spans inside a
history, not just at its two ends. Their count is bounded by retained chunks
(each contains at least one visible entry); each backing comes from a bounded
admitted batch. This is a different physical bound from a flat vector. Output
reservations use the full retained charge; they are separate from the host's
canonical-log memory budget. Sustained and conflict-heavy memory/debt evidence
must account for these pinned allocations, not merely visible entry counts.
Prospective overlay accounting conservatively covers both trim/concatenate
orders; installed summaries read the actual owner's physical charge.

Written-but-unsynchronized entries remain invisible to durable state and
applications. Shared handles survive delayed completion, cancellation and
replacement under the same original ownership lifetimes. Restart reconstructs
backing from validated persisted values; process addresses never supply
recovery, lineage, fencing or quorum evidence. Storage failures preserve the
existing partial-progress and fencing behavior.
