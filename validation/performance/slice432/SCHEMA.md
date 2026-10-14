## Slice432 schema — exact clone construction at its destination

Measured selection: actual431 Linux entry-to-Vec self643/6,758 samples (9.515%)
all land after an out-of-line LogEntry::clone call, predominantly at a load from
the64-byte temporary subsequently copied into the destination. Interrupt skid
prevents assigning that instruction a precise cost. Static code already skips
malloc/memcpy for empty command bytes; there is no zero-byte call to remove.
Fresh Mac431 physical clone2.565% and memmove4.239% overlap broader completion/
installation contexts. Mach interval/wait bias and coalesced aliases prevent
precise instruction attribution. The selected hypothesis is better construction
codegen, not removal of all cloning or independent asynchronous owners.

Data/API: replace only LogEntry's derived Clone with an exhaustive fieldwise
implementation using an explicit inline hint. Copy index/term and delegate the
entire payload to its unchanged derived Clone. No layout, trait signature,
EntryPayload variant, Vec/Box ownership, operation ID, bytes or configuration
change. Normal Vec clone allocation/normalization and full nonempty/configuration
fallback remain identical. No extra helper, shared buffer, proof, digest or seam.

Transitions/ownership: immutable source to independently owned clone; no consensus,
provider, application, queue, effect-release or checkpoint transition moves.
Existing allocation failure/panic behavior and drop lifetimes remain. No new state
needs restart reconstruction or cleanup; wire/WAL/snapshot formats, fences,
durability tickets, retry outcomes and full equality remain unchanged. This targets
the compiler's constructor temporary while retaining the copies demanded by the
current contracts. Formatter/Clippy configuration and all build checks stay at zero.

Acceptance: independent derived top-level oracle for every payload variant,
arbitrary full-width IDs, headers, empty/spare/nonempty vectors, complete nested
configuration and independent heap ownership; include clone_from behavior.
Affected log/provider/Raft/snapshot and actual TCP/QUIC retry/recovery controls,
root four strict/standalone checks before publishing. Fresh asserted published
artifacts must show actual clone-loop codegen and report original/copy/text sizes;
paired431/432 bulk/singleton and nonempty controls decide benefit. Preserve every
failure/regression. If inlining does not improve codegen or measured throughput,
make a focused decision from that evidence rather than adding ownership redesign.


Implementation detail: explicit #[inline(always)] exhaustive Self destructure with no `..`; entire unchanged EntryPayload::clone delegate. Independent derived top-level oracle controls via existing core-visible provider conformance integration suite. Default clone_from remains assignment from self.clone(), matching previous derive behavior; no empty-byte or configuration shortcut.
