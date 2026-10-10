# Slice169 — unresolved creation cancellation

Base: `5b2c41fbb558bd1a31f1785e61837f111aa239a4`, plus this commit.

Opt-in metadata schema16 adds a fixed56-byte cancellation command and a bounded
original-creation status query. Existing schemas retain their history formats.
Cancellation consumes the reservation's publication credit, retains identity
tombstones and prevents later namespace publication, insertion or plain-transfer
reuse. Active/published owners and previously claimed transfer targets refuse
cancellation. Parent lifecycle locks are released without deleting provisioned
storage or revoking a direct embedded group's independent authority.

## Checks

```sh
cargo +stable test --locked --offline --all-features --test directory --test group_creation --test namespace_creation --test insertion --test deletion --test retained_insertion --test reparenting --test reparent_guards --test metadata_transfer -- --nocapture
cargo +stable test --locked --offline --no-default-features --test directory --test group_creation --test namespace_creation --test insertion -- --nocapture
cargo +stable test --locked --offline --all-features --test namespace_creation --test insertion cancellation -- --nocapture
cargo +stable test --locked --offline --all-features --test routed namespace_cancellation -- --nocapture
cargo +stable test --locked --offline --all-features --test routed native::creation -- --nocapture
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable fmt --all -- --check
RUSTDOCFLAGS='-D warnings' cargo +stable doc --locked --offline --all-features --no-deps
node validation/check-inventory.mjs
```

The broad lifecycle run passes189 tests; core-only runs pass55 tests. The final
seven cancellation tests include group-ID reuse under a new incarnation and
215 native WAL fault cuts (213 old reservation,2 complete cancellation), each
followed by an exact retry. Both strict Clippy profiles, formatting,
warnings-denied documentation and92-record inventory validation pass.

Two new native TCP/TLS and QUIC histories pass with actual native WAL/snapshot
files and owning Nodes. They leave readiness/cancellation receipts unread,
recover a lagging metadata replica, checkpoint/reopen the tombstone and refuse
late publication and target data admission. These are selected Linux loopback
histories, not physical power-loss, exhaustive schedules or macOS evidence.
The complete12-test native creation and created-source regression selection
passes in200.81s. The final two cancellation histories also pass on the final
source revision in1.63s. Final source hashes are in `source-sha256.txt`.

## Retained failures and limits

`core-lifecycle-first.log` records a test fixture constructing an unsupported
single-to-single transfer. It now uses a valid partitioned target to test the
canceled identity guard. `native-cancellation-first.log` records the QUIC test
using stale target leadership after metadata recovery; it now establishes
fresh target leadership before its quorum read. Neither fix weakens protocol
checks. During compilation, an omitted exhaustive query arm, a read-bound
function exceeding100 lines, and test-module re-export visibility were corrected;
their outputs were observed in the task but were not saved as raw files.

`previous-ci-failed.log` is the failed-job output of the **base commit**, GitHub
run38018887145. Its formatting and both Clippy profiles succeeded. Ubuntu service
tests reported transient LeadershipChanged write/configuration outcomes; macOS
reported ReadNotReady and an unexplained nonzero service shutdown exit. These
remain explicit follow-up work; this slice does not claim that CI is green or
that the shutdown failure is harmless. Full P0–P7 acceptance remains open.

The prior CI failures are not reclassified by these selected local successes.
