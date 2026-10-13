# Configured steady-state measurement schema

Purpose: attribute slice413's unchanged-ballot append/commit delta on stable,
joint and checkpoint histories, using the existing volatile native-log diagnostic.
This advances P9.0/P9.1/P9.3; it does not measure device durability or consensus.

Data/API: preserve existing replacement and plain-append arguments. Add explicit
`append-commit` mode and `plain|stable|joint|checkpoint` initial-history selection.
History means retained entries, including any retained configuration record;
checkpoint history retains the same requested count above a base of1. Each update
contains one original command; append-commit commits through that new command.
Record mode, history type and snapshot base in every output row.

State/ownership: initialization installs a genuine validated configuration (or
checkpoint membership), then the existing native provider owns all subsequent
states. Timed regions include append and exact simulated barrier, excluding the
independent full-validator oracle. Expected final states are reconstructed by that
oracle after measurement, rather than using optimized provider output as truth.

Failure/restart: require one exact ticket and simulated publication per update;
verify revision, generation, term, last index, every neighbor and complete final
state. Drop the store, discard unsynchronized device bytes, recover a new session,
reject the old ticket and compare every recovered state with the oracle. Refuse
unsupported mode/history combinations and fresh-output collisions. Errors retain
their original output; do not publish invalid rows as timings.

Acceptance: existing plain/replacement controls remain valid; new stable/joint/
checkpoint append and append-commit controls execute the real provider and exact
recovery oracle. Formatting and all four strict Clippy profiles stay zero. Frozen
baseline/candidate receive identical diagnostic source, compile independently,
retain source/artifact guards and interleaved raw trials on Linux and M2 Max.
