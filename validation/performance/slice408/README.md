# Counter batch-local staging

Replace Counter's full permanent retry-history clone with an immutable historical
lookup and a bounded novel-operation overlay. Complete validation precedes
installation. Public APIs, checkpoints and original outcomes are unchanged.

Linux application12/0 and provider application3/0 pass with all features and
no default features. Real TCP/QUIC three-service retry, leader replacement and
native-file recovery pass2/0 (11.41s). Formatting and all four strict all-target
Clippy configurations pass with zero diagnostics; the enabled push hook repeats
them before publication. Agent source hashes and exact targeted commands are in
CHECKS.md; the schema is in SCHEMA.md.

The first service attempt used a nonexistent target name and ran no tests.
The subsequent sandboxed service attempt failed to allocate loopback ports;
the authorized socket-enabled run passes both controls. Retained failed records
are environmental/setup failures, not passing validation.

Mac408 now passes application/provider controls with all and no default features,
formatting/four strict configurations, and three exact source guards.
[Mac records](mac/README.md). No timed speedup or durable throughput is claimed.
This removes history-sized application copying; log-history work and native
batch/replication ownership remain separate P9 deliverables.
