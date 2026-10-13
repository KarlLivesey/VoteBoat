# Ready original-request batches

Already-ready Counter invocations share one ordered Core persistence unit,
without an arrival delay. Every original admission, position and result remains
distinct; Written still cannot produce a committed/applied success. Custom
applications opt in explicitly, with a default-false admission capability.

Selection respects control ordering, existing visit/step/byte budgets, Core
limits and currently available owner output space. A smaller fitting prefix
preserves progress when a full batch would overflow. Client validation now
indexes original admissions and scans only the unapplied retained-log tail.
Public singleton stepping, wire/storage formats and security contracts remain.

[Ownership schema](OWNER_SCHEMA.md), [runtime schema](RUNTIME_SCHEMA.md) and
[acceptance review](ACCEPTANCE_REVIEW.md) record the design and boundaries.

Linux final-source commands:

```sh
cargo +stable test --locked --offline --all-features --lib --test admission --test application --test effect_owner --test provider_conformance --test raft --test runtime --test worker
cargo +stable test --locked --offline --no-default-features --lib --test admission --test application --test effect_owner --test provider_conformance --test raft --test runtime --test worker
.githooks/pre-push
```

All-feature691/0; core-only523/0; formatting and all four strict Clippy profiles
are zero. Original-result, duplicate/conflict, cancellation, Written failure,
context-sensitive default-false, step caps and constrained/held-space controls
pass. Initial budget-fixture failures are retained: the fixture submitted to a
busy one-request worker; its drain now retains leases until that worker is free.
The final broad runs include both budget controls and that focused fixture fix.

Real TCP/QUIC retry, leader replacement and native-file recovery pass2/0 before
the final adaptive reservation refinement; this earlier scope is identified by
the retained service log. Mac original broad qualification passes689/521 before
the output-space refinement. The final exact four changed inputs pass7 ready
batch controls and9 scheduler controls in both configurations, with formatting,
four strict Clippy profiles, genuine isolated compilation and three1336-input
source guards clean. [Original broad scope](mac-ready410/RESULTS.json) and
[final focused scope](mac-ready410-final/RESULTS.json) remain separate.
This is a checked performance enabler. No timed throughput or OpenRaft-floor
claim is inferred from functional checks; subsequent benchmarks cover both hosts.
