# Linux startup qualification396

| Original full target | Actual result | Duration |
|---|---:|---:|
| All features |57 passed,0 failed|52.18s|
| Default features |42 passed,0 failed|26.44s|

Formatting and all/default/core/native strict Clippy passed. Each startup target
ran once sequentially, using freshly compiled release opt3 test/library artifacts
with assertions and overflow checks enabled. Original source/executable/library
guards passed before/after; all-feature artifacts also stayed unchanged across
the default rebuild.

See [original result](provenance/RESULT.md), [commands and profiles](provenance/QUALIFICATION.json),
[all-feature log](linux/ALL.log), [default log](linux/DEFAULT.log),
and [complete1306-input fingerprint manifest](provenance/SOURCE.json).
Frozen [startup harness](source-fixtures/tests/startup.rs),
[local drain](source-fixtures/tests/startup/maintenance/drain.rs) and
[membership drain](source-fixtures/tests/startup/maintenance/membership_drain.rs)
preserve the cooperative caller/helper and original outcomes. Production379 is
unchanged; new unchecked397 cluster checkpoint work is excluded. This packet
archives evidence and those three fixtures, rather than a complete build checkout.
Original runner scripts/absolute command paths are historical records.

The [earlier395 failure](prior395/ALL.log) remains preserved:56passed/1failed in
the QUIC membership-drain19723 Applied assertion. Its actual non-Applied variant
and cause were unprinted and remain unknown. Changed396 passed while retaining
original19723 Applied,19722 duplicate and resource cleanup checks; no failed enum
printed. No retrospective runtime-cause claim.

[Copy fingerprints](COPY_MAP.json) map archived bytes to originals;
[evidence.sha256](evidence.sha256) covers the packet. No Mac, full-current-repository
or P8 completion claim is made.
