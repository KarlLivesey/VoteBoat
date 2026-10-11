# Shared scheduler replacement obligations

Extend the existing ReadyScheduler checker and actual independent HostReady /
native FairScheduler providers. No new production scheduler or runtime. Shared
assertions cover exact-incarnation cancellation, absent/repeated cancellation,
idempotent duplicate enqueue at capacity, atomic overload refusal, empty pop,
32 fair tail-requeue rounds and independent instance/drop/reconstruction.
Eight deterministic256-action traces per provider compare enqueue/cancel/pop,
capacity/length/is_empty and final draining with an independent queue model.

Three defective host providers trigger the same assertions: incarnation-blind
cancellation, destructive overload refusal and unfair front insertion. These
are real public-trait implementations, not metadata-only negative cases.

| Selected profile | Linux passed/failed | macOS arm64 passed/failed |
| --- | ---: | ---: |
| All features |5/0|5/0|
| No default features |4/0|4/0|
| Native only |5/0|5/0|
| Default |5/0|5/0|
| Named provider metadata tests |24/0|24/0|

Formatting and default/all/core/native strict Clippy report zero diagnostics on
both hosts. All654 Rust build inputs and separate metadata inputs verify at
terminal. Production is unchanged fromf2467a9; only the runtime test module and
new checker file change Rust build inputs. The inventory adds the already-public
default is_empty and checker anchor; one partial six-operation review raises
metadata coverage to14 reviews/88 operations, with108 inventory entries total.
The existing reviewed entries and assertions remain unchanged.

The initial sandbox Node invocation reports one passing file-level test; it does
not supply named24-test evidence. The preserved subsequent invocation reports
all24 named tests. The first result's cause is not inferred. Runtime feature
profiles execute sequentially. Mac uses an independent checkout and clone-file
cache from an idle source; live original/corrected recovery checkouts stay intact.

Finite capacity-three queues and two selected incarnations do not certify every
allocation limit, arbitrary callback responsiveness, shared host executors or
complete Shard construction/visit/recovery behavior. The readiness queue owns
no core/durable state or external resource. Failed Mac lifecycle evidence has
its own source-preserved [family scope](../slice254d/mac-family/README.md).
Raw.sha256 hashes input bytes; published.sha256 hashes path-normalized text with
trailing whitespace removed. Linux stable1.98.1/Mac stable1.98.0 conditions remain.
