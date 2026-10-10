# Slice197b4b2b4 — foreground multi-group drain runner

Base revision: `3fe3df9`. Local Linux execution; full P0–P7 remains active.

`group-drain-run` drives the existing authenticated source, leadership and
membership commands with the original source sequence/operation and complete
mixed-role plan. It validates original row identities and digest, finds each
group's current leader, drives original joint/final operations and stops only
through fresh source readiness and explicit stop acceptance. No public provider,
consensus format, storage format or dependency is added.

The runner is bounded by256 rows,4096 requests,120 seconds total and five
seconds per request. Cancellation remains explicit. Killing it preserves accepted
work; rerunning uses the same IDs. A historical completed handoff whose target
has lost leadership requires explicit operator resolution rather than inventing
a new operation or treating history as current authority.

- `focused.log`: all five process tests pass (9.89s), covering TCP/QUIC completion,
  reordered explicit peer endpoints, wrong scope/identity, missing target,
  cancelled intent, and interrupted runner/source recovery with uncommitted
  joint membership through TCP/WAL and QUIC/checkpoint histories. Source stop,
  preserved data retries and surviving writes are checked.
- `service.log`: final full counter regression passes119 service tests (53.23s)
  and14 command unit tests, with no failures or ignored tests. Parser tests bind
  count/digest/rows and handoff receipts; request and elapsed-time budget refusal
  is checked before a connection is attempted. This is not a full-crate sweep.
- `authority-before.log`: the interrupted QUIC path exposed a known
  LeadershipChanged reply during an original-ID handoff. That exact response,
  and existing read-authority transition replies, now return to bounded state
  observation. Other unknown/error replies remain terminal.
- `fixture-before.log`: a later QUIC run hit the same unknown response while
  seeding initial data, before creating its drain plan or runner. The fixture
  now uses the existing same-ID authenticated-write retry helper for setup and
  final data checks. Generic client unknown-write behavior is unchanged.
- `busy-before.log`: interrupted QUIC recovery exposed `not_proposed=Busy` in
  a handoff. This exact no-admission refusal now returns to bounded observation.
  The classifier test rejects lookalikes and unrelated errors.
- `focused-initial.log`, `unit-focused.log` retain narrower intermediate passing
  runs; `clippy-before.log` retains the initial clean implementation lint run.
- Final formatting, all four strict Clippy profiles, warnings-denied API docs
  and the105-contract inventory check pass. The inventory checker validates
  metadata/paths, not runtime behavior.

`commands.txt` records commands and `source-sha256.txt` identifies validated
sources. Startup/listener, source gate, ownership and durability checks retain
their existing contracts. These finite tests do not prove arbitrary power loss,
macOS execution, separate-machine deployment, broader lifecycle coverage or
P7's still-unmet250ms p99 gate. Assignment listing is the next feature.
