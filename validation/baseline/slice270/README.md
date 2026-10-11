# Slice 270: supplied checkpoint codec recovery

Fixes a real storage seam mismatch: a supplied `LogCodec` could successfully
reclaim with its own checkpoint marker, then fail recovery because dispatch
recognized only native markers. `is_checkpoint` now selects the supplied decoder
from at most32 prefix bytes. Its default preserves both native checkpoint formats.
Reclamation refuses an unrecognized encoded image before replacement. Existing
mandatory state/identity/boundary checks and fencing remain authoritative.

Before-fix evidence retains the actual successful reclaim followed by corrupt
reopen (`batch header/version/checksum`), the test sources and observed663-input
manifest. That manifest was captured after the negative test, not asserted before
it. The marker codec is a test companion using native encoding/validation; this
is not a new backend or a migration implementation.

Linux and macOS each pass the five complete targeted integration binaries
(`ballots`, `log_reclaim`, `log_store`, `membership`, `raft`) sequentially in all,
default, core and native profiles:124/124/50/124 passing tests, zero failures.
Formatting and all four strict Clippy profiles have zero diagnostics on both
hosts. All663 frozen build inputs verify before and after each matrix. The
three new cases cover exact two-group model/file restart, trailing writes,
repeated reclamation, ticket sequencing, stale sessions, wrong selected codec
and inconsistent-recognition refusal without journal changes. Existing native
ballot/history and checkpoint tests remain included. Metadata checker tests pass;
those references remain partial reviews, not provider certification.

`published.sha256` hashes the path-normalized published artifacts; `raw.sha256`
records the original bytes. Frozen broader269 and old262/251 lifecycle jobs use
separate source images and remain outside this slice's acceptance. This targeted
pass does not establish whole-platform or full P0–P7 completion.
