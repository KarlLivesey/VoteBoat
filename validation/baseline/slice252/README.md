# Slice252 — selected application conformance

One private scenario adapter drives the same obligation checker through public
traits with native Counter and an independent host Set application. The host
uses set commands, owned String receipts/query/results and its own schema77
checkpoint; its implementation does not use Counter. Application semantics are
provided by each scenario rather than imposed as a shared command schema.

The checker verifies contiguous ordered application, exact command receipt
identity/count/order, no Noop receipt, original outcomes and cold retry/conflict
behavior. Native deployment bounds validate without mutation; a host lacking that
optional capability refuses deployment validation. The selected providers declare
BoundedStateMachine: malformed/gap/capacity failure must leave the complete state
and applied boundary unchanged. Basic StateMachine partial failure remains an
owner-fencing/recovery responsibility, not an invented atomicity promise.

Receipt bounds are computed before execution without mutation. Actual vector
capacity plus provider-reported nested capacity must fit the original bound;
the host's String capacity is explicit and insufficient nested limits refuse.
Applied reads stay immutable and future boundaries refuse. A fresh compatible
instance restores the complete checkpoint and replays original IDs/content,
conflicting content and a fresh operation. Every truncation, trailing bytes,
wrong schema/boundary and incompatible configured capacity refuses atomically.
A mutable checkpoint clone does not alias the original selected state.

Four faulty providers must fail the exact expected assertion: wrong receipt
index, partially applied failed batch, future-boundary read and zero receipt
bound. Unrelated panics cannot satisfy the negative test. No production core,
provider, API, format, dependency, timer, quorum or native IO change.

| Focused checks | Linux | Mac |
| --- | --- | --- |
| All-feature application / provider_conformance |10 /29 passed|10 /29 passed|
| Core-only application / provider_conformance |10 /14 passed|10 /14 passed|
| Native-only application / provider_conformance |10 /29 passed|10 /29 passed|
| Formatting and default/all/core/native strict Clippy |Zero diagnostics|Zero diagnostics|
| Accepted build/source inputs |651 verify|651 verify|

Feature tests execute sequentially per host in isolated checkouts. Mac arm64/
macOS27.0 uses installed stable1.98.0 and the existing child-shell4096 descriptor
condition. The original251 full suites stay in their separate source-preserved
checkouts. Their reported Mac QUIC/routed failures remain open and cannot inherit
this focused result. These tests provide no physical latency or P7 acceptance.

The inventory still has108 contracts. Four selected source-linked operation
reviews make13 partial reviews/81 operation groups; all24 metadata-checker tests
pass. Metadata integrity is not provider certification. Remaining scopes identify
physical databases, richer applications/queries, all allocator/owner lifetimes,
group-bound deployments and distributed read authority. Snapshot/WAL and owning
router faults retain their separate histories. Full P0–P7 remains active.

Initial test-only compiler failures used Result conversion on an Option and an
assumed GroupIdentity constructor; inspected public types fix both. A stale
metadata expectation and strict fixed-chunk lint were corrected without weakening
checks. Earlier raw logs remain. Those failed source variants were not fully
snapshotted; final accepted651 input hashes bind the reported passing runs.
Published logs normalize checkout paths/trailing whitespace; exact original raw
log digests and task-local paths are in raw.sha256. Final zero-context patch (apply with --unidiff-zero) and metadata hashes
record the source-bound change.

Reproduce the focused feature configurations sequentially:

```sh
cargo +stable test --locked --offline --all-features --test provider_conformance --test application
cargo +stable test --locked --offline --no-default-features --test provider_conformance --test application
cargo +stable test --locked --offline --no-default-features --features native --test provider_conformance --test application
sh .githooks/pre-push
node validation/check-inventory.mjs
node --test validation/check-provider-conformance.test.mjs
```
