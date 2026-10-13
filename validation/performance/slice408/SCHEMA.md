# P9 Counter transactional batch overlay

Purpose: remove whole retained retry-history cloning from every application
batch. Counter remains an example bounded deterministic state machine with the
same public API, checkpoint schema, receipt semantics and permanent operation
history. This slice makes no throughput claim without separate measurements.

Data shape: immutable existing dedup BTreeMap; projected value and applied
scalars; a BTreeMap containing only previously unseen operation IDs admitted in
this batch; an exactly command-count-sized ordered receipt vector. Overlay
size never exceeds the remaining configured operation capacity. Existing IDs
are read in place, never cloned or evicted.

Transition: scan entries in their exact input order. Require the next contiguous
index before processing each entry; require exactly eight bytes for Commands.
Lookup an ID in the batch overlay or existing immutable history. Equal content
returns its original outcome, including Overflow, and different content returns
an OperationConflict receipt without changing the original effect. Novel IDs
consume one overlay capacity unit and compute checked arithmetic on projected
value. Noops/configuration entries advance only the projected applied boundary.

Ownership/install: all temporary state is owned by one apply call; no borrowed
receipt escapes. Only after every entry succeeds, insert the novel map entries
individually into the original dedup map and install the projected scalars. This
avoids cloning/rebuilding unrelated historical dedup entries and preserves
atomicity of all returned ApplicationError cases.

Failure/cleanup: index gaps, malformed later commands or novel-ID capacity
exhaustion drop the overlay/receipts and leave value, applied boundary and every
historical ID/content/outcome unchanged. Arithmetic overflow remains a retained
successful operation outcome. Empty batches are successful identity transitions.

Acceptance: existing application controls; mixed historical and in-batch retry,
conflict and overflow outcomes; permanent history after checkpoint/restart;
malformed/gapped/capacity-limited late failure with byte-identical checkpoint;
capacity counts distinct novel IDs despite repeats; operation after refused batch
remains novel. Run targeted controls and strict lint in isolated target directory.
