# P9 touched-group transactional staging

Purpose: remove unrelated accepted/durable group copying from each append and
barrier while preserving the existing storage seam, disk format and durability.

Data/API: crate-private stage_batch borrows the accepted map and returns owned
validated replacements only for touched groups. stage_update borrows one GroupLog
and returns its validated replacement for the core singleton path. Public
apply_batch installs replacements only after the complete batch validates.
NativeLogStore owns a dirty group set alongside accepted and durable maps.

Transitions: admission validates limits, duplicate groups, group identity reuse,
ballots, suffix/snapshot generations and full batch before append I/O. A completed
append installs touched accepted versions and marks them dirty, invalidating only
tickets whose group's generation changed. A barrier first synchronizes and
publishes the complete WAL boundary, then promotes all dirty accepted groups,
clears dirty and returns exactly requested dependencies. Other outstanding tickets
remain outstanding even if their states were physically synchronized.

Failure/restart: validation failure changes neither accepted state nor journal.
Any append/sync/publication uncertainty keeps the store fenced. Dirty state is
not installed before uncertain append and not cleared before successful durable
publication. Create/recovery initialize dirty empty. Recovery reconstructs the
same complete published states and increments session; reclamation remains
quiescent and rejects pending/dirty work. No new watermark, receipt or disk token.

Acceptance: invalid later batch leaves every original group unchanged; duplicate
incarnation creates and duplicate units refuse; suffix generation invalidates only
that group's old tickets; requested subset barrier promotes every dirty group,
returns only requested tickets and supports subsequent barrier without rewriting
cold groups. Recovery after partial append/sync/publication preserves existing
crash expectations. Existing log-store, reclamation, conformance, generation and
crash tests pass. Formatting and strict Clippy remain zero.
