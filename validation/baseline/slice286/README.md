# Shared-WAL drain coordinator cancellation recovery286

The example driver retains a cancelled configuration dispatch until both the
actual full-ticket OwnerStep execution and public completion are observed.
Cancellation ends a wait; it does not remove queued owner work. Local drain
readiness also does not establish every remote replica's commitment: the original
25s loop now includes its required all-replica Final predicate. Cold readiness
uses normal bounded polling for restored drain gates. Original configuration
authorization, limits, data/retry, source shutdown and cleanup remain.

The controlled pre-fix Linux TCP/QUIC regression fails0/2 because the original
driver releases queued work immediately. The initial corrected image passes that
check but its new immediate cold-readiness assertion fails on both hosts; Mac
also exposes the original premature all-replica assertion. The intermediate
cold-wait image passes Linux9/0 and Mac7/2, retaining the remote assertion failures.
These failed images and source guards are preserved separately.

The final image passes6/all and3/default on Linux and Apple Silicon macOS,
9 executions per host, including controlled cancellation and the four original
mixed/group/checkpoint histories. Formatting/four strict all-target Clippy
profiles are zero; all669 inputs verify before/terminal. Actual original Joint,
all-replica Final, shared-WAL cold retry and owned cleanup remain mandatory.

This qualifies the example's public coordinator/Node composition, not every
authenticated executable runner or all P0–P7. No production core/API, persistent
format, deadline, capacity limit or journal-error waiver changes. Final runtime
patch is relative to eb0ffb9;285's independent lifecycle patch is separate.
