# Unchanged454 cold-recovery baseline control

The original exact QUIC two-level cold-recovery test on immutable454 source exited101 after133.5s at close (`host.rs197` progress timeout). This differs from the455 candidate upper-retry timeout; no common cause or candidate speedup follows. This was a debug functional control, not throughput qualification.

The before/after identities confirm1440 selected454 source hashes/modes with compiler records, actual command, original PID/status and full raw stdout/stderr retained. The preflight text says no test had started at that point; `SOURCE_AFTER.json` is the actual terminal result. No binary/full source/build tree is copied. `ORIGINAL_SOURCE.json` inventories copied files, `COMPRESSION.json` records the lossless policy, and `SHA256SUMS` seals all current regular files.
