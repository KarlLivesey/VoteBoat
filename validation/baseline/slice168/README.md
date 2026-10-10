# Slice168 — bounded operational event history

Base `f7fe3c4196acbcff981bb0de87a579dcd803d3c3` plus this commit.
No consensus, persistent format, quorum or durability-token change.

## Implementation and scope

Public EventObserver/EventReporter contracts and the native preallocated ring
retain fixed Copy events under count/byte/page limits. Cursors carry exact
RuntimeOwner and EventGeneration. The native ring evicts oldest entries and
reports cumulative discards plus the exact gap after a cursor. Closed streams
remain readable, exported pages have independent ownership, and exhausted
sequence numbers refuse new records without discarding old ones.

The reporter emits at most five aggregate records after a Node poll and counts
sink refusals without changing that poll's result. It observes state changes,
failed polls, step failures, snapshot progress and pressure. The existing counter
observation adds ordinary-snapshot send refusals. The service explicitly selects
native providers, replaces per-step stderr writes with bounded history, and
exposes Inspect-authorized pages of at most16 records. New store sessions reject
old cursors. There is no hidden export worker, arbitrary event label, payload
capture, per-group trace, latency attribution or claim of durable history.

## Validation commands

```sh
cargo +stable test --locked --offline --all-features --test observability
cargo +stable test --locked --offline --no-default-features --test observability --test effect_owner
cargo +stable test --locked --offline --all-features --test effect_owner event
cargo +stable test --locked --offline --all-features --lib native::observability::events
cargo +stable test --locked --offline --all-features --bin voteboat-counter diagnostics
cargo +stable test --locked --offline --all-features --test counter_service events:: -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service remote_configuration_requires_admin -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service -- --nocapture
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable fmt --all -- --check
RUSTDOCFLAGS='-D warnings' cargo +stable doc --locked --offline --all-features --no-deps
node validation/check-inventory.mjs
```

Six all-feature and four core-only observation tests pass. All147 core-only owner
tests pass; the four selected all-feature owner tests include the new refusing
event-sink Node history. Sequence-exhaustion and maximum-value service-page tests
pass. Both new TCP/TLS and QUIC event/checkpoint/restart histories pass in1.90s;
the corrected remote-plan pair passes in4.18s. Both strict Clippy profiles,
formatting, warnings-denied API documentation and92-record inventory metadata
validation pass. This is selected local Linux evidence, not complete P0–P7 or
platform acceptance.
The final complete service target passes47/47 in41.11s, recorded in
`service-complete.log`. Source and fixture hashes are in `source-sha256.txt`.

## Retained development failures

`events-first.log` records a missing explicit service-diagnostics close method;
`events-second.log` records a test module path resolving at the test root. The
corrected paths and close delegation are exercised by the later runs.
`clippy-corrected.log` records a cognitive-complexity failure in one conformance
function. Its rejection and retention phases are now separate; lint levels and
thresholds are unchanged.

`service-events-first.log` records querying immediately after spawning a
restarted process, before its listener existed. The corrected fixture waits
only for transport availability, with a deadline; semantic error replies still
fail immediately. `service-all.log` records an older remote-plan assertion that
did not include the received reply after using a cached leader. A scoped helper
now refreshes only after explicit NOT_LEADER, preserves the operation ID and
retains unexpected replies in assertion output. Existing authorization,
missing-plan, commitment and retry checks remain.

`service-all-final.log` records a second older assumption: an unread joint
request can report Unknown after a term change instead of logging Committed on
the original leader. The final fixture keeps the exact original record, allows
at most eight unread attempts across observed leader/term changes, and requires
the original node's durable committed joint status before cutting it. No
application reply is read. It still checks the saved joint/final configuration,
demoted-node elections, original retries and full file recovery. The focused
TCP/QUIC pair passes in12.49s. This extends the allowed test schedule to include
an intervening leadership change; it does not alter the production protocol.

`previous-ci-status.json` is a snapshot of previous-commit CI only. It must not be
read as validation of this commit. All failed observations are retained separately
from successful runs; test counts do not certify untested fault combinations.
