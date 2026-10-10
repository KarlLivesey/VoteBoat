# Slice249 — functional ledger and native Mac resource requirements

The R01–R19/chapter09/P0–P7/C01–C24 acceptance tables separate mechanisms from
their remaining evidence. This review corrects specific obsolete inventory
labels: bounded event/timing diagnostics and executable live endpoint refresh
exist, voter replacement exists, and authenticated transfer retirement exists.
The original design pack is unchanged. The metadata checker validates the newly
separate `remaining_validation` names;108 contracts remain inventory records,
with only9 partial reviews/68 operation groups. No replacement-provider certificate
or complete roadmap acceptance is claimed.

Direct source/assertion references for these corrections:

| Capability | Existing source and checks |
| --- | --- |
| Retire a transferred source | `src/bin/support/transfer_client.rs`, `transfer_retirement.rs`; `tests/transfer_service/retirement.rs`, `merge.rs`; `docs/TRANSFER_SERVICE.md` |
| Bounded events and timing observations | Event/Timing observer contracts and native providers; counter `events`, `timings`, `metrics` commands; recorded168/176 and later counter histories |
| Live command discovery endpoint refresh | `src/bin/support/command_endpoints.rs`, `command_discovery.rs`; counter endpoint/discovery histories209e/209j/209k |
| Voter replacement and planned drain | Native Node administration and counter drain runner; original interrupted replacement/drain histories247/248 |

General segment cleaning, optional wider admission policy, tracing/exporters,
durable authorization audit, bounded compression, P8 transactions and automatic
global orchestration are still absent. They do not imply that the existing bounded
WAL, admission, diagnostics, membership or explicit lifecycle operators are absent.
Performance/security work and P8 are kept separate from the user's feature-first
priority; remaining combined-fault/provider/platform acceptance stays explicit.

Identical all-feature ten-target checks cover provider_conformance, raft, runtime,
effect_owner, transport, quorum, activation_model, log_store, application and
snapshot. Linux passes365. The authorized Mac initially passes358/fails7; every
failure reports EMFILE under its inherited256-file soft limit. A single unchanged
three-node/100-group snapshot history also fails at256: each snapshot store owns
an exclusive lock descriptor, requiring300 before WAL/transport/publication files.

With4096 descriptors in the child shell only, the identical Mac suite passes361/
fails4. EMFILE disappears, but snapshot catch-up, owning facade, automatic
checkpoint and shared-peer-pressure histories time out. The failed broad results
are retained; no deadline, concurrency, group count, lock, assertion or production
behavior is changed. This is a resource-condition correction, not complete Mac
acceptance or a storage/performance fix. The unchanged isolated snapshot history
passes1 at4096 in93.62s overall, retaining its original per-progress deadlines.
This supports further investigation of concurrent progress; it does not establish
the exact cause or certify the other failed histories.

All647 build/source inputs match the published3071fc2 source on both hosts; Mac
uses installed stable1.98.0/arm64/macOS27.0. Formatting and four strict Clippy
profiles pass on both hosts. CI and documented local commands set the Mac test
process limit explicitly, retaining sequential feature profiles; host-wide
settings stay unchanged. Full P0–P7 remains active. Investigate the remaining Mac
native progress failures before adding another conformance helper.

Raw logs remain under `target/slice249*`; `raw.sha256` records their exact bytes.
Published logs normalize task paths and trailing whitespace. The original Mac
failed stores remain in the task-owned remote test root.
