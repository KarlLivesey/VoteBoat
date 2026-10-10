# Existing timer-provider obligations

Reuse the existing shared TimerService checker rather than add another provider
or fixture. It exercises HostTimers and native DeadlineQueue, eight128-action
modeled schedules per provider and an intentionally defective wrong-owner
cancellation provider. Scoped cancellation, stale generations/incarnations,
capacity refusal, replacement at capacity, zero-budget clock observation,
earliest deadline/lateness, length/is_empty and owner reconstruction are checked.
Remaining construction/sequence-exhaustion coverage keeps the review partial.

| Feature profile | Linux passed/failed | macOS arm64 passed/failed |
| --- | ---: | ---: |
| All features |3/0|3/0|
| No default features |2/0|2/0|
| Native only |3/0|3/0|
| Default |3/0|3/0|
| Provider metadata checker tests |24/0|24/0|

Formatting and default/all/core/native strict Clippy report zero diagnostics on
both hosts. The653 Rust build inputs remain identical to91576fa, verified at
terminal. Metadata/checker inputs are bound separately: the actual omission was
the public default is_empty method, already described and behaviorally checked.
Add its name to the existing inventory/review and link the existing checker.
Inventory remains108 entries,13 partial reviews now name82 operations. These
metadata checks do not certify provider behavior or close full platform coverage.

The initial Linux Node test invocation reports only a file-level failure; its
cause was not established. The retained subsequent invocation passes24/0 and
updates the expected operation count from81 to82. A duplicate-review guard
rejects an attempted redundant TimerService review before writing metadata.
Setup guards also reject treating JSON as Rust build inputs before upload;
the separately hashed metadata copy and all Rust source checks pass. No runtime,
API, format, queue, clock, durability, dependency or timer limit changes.

Raw.sha256 hashes input bytes; published.sha256 hashes path-normalized text
with trailing whitespace removed. The original Mac lifecycle failures remain
independent; this scope is selected timer and metadata evidence only.
