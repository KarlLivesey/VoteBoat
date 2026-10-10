# Namespace timeout observation

The exact QUIC/WAL created-namespace split history passes in isolation on Linux
(18.75s) and macOS (294.05s). Its retained terminal observations are Applied;
the original Ready/publication/activation/import/fence predicates and 15-second
individual waits remain. The observer polls the exact client envelope without
completing/cancelling it, retaining one client credit until the original abort.
No original fault or assertion is replaced.

This does **not** explain the earlier Mac 3/1 family's readiness timeout.
The diagnostic source changes only `tests/routed/creation_source.rs` from
91576fa; all 653 inputs verify at start/terminal. `diagnostic.patch` records
the temporary observer, which is removed after evidence collection.
Formatting/all-target all-feature strict Clippy are zero on both hosts.

The initial compile error used a nonexistent Bootstrap node field; the fix
uses the existing Raft::local_node API. The first Linux run cannot allocate
its endpoint in the sandbox and fails before the history; the authorized
loopback run above is separate. Both rejected attempts remain archived.
Raw.sha256 records input bytes; published text trims trailing whitespace.
Original failed stores and original251/254d running sources are preserved.
