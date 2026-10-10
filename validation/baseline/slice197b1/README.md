# Slice197b1 — durable local drain intent and cancellation

Base revision: `748a93e`. Linux execution. The full P0–P7 objective and
coordinated drain197 remain active; this implements its recovery requirement.

`DrainJournal` records an exact owner, positive local sequence, original
operation, sorted expected assignments (at most1024), and Active/Cancelled.
Exact retries are idempotent. Only the latest local operation is retained;
sequence is not a consensus watermark or a global audit history.
`NativeDrainJournal` publishes versioned SHA-256 records through public atomic
`DrainRecordIo`. File publication writes/synchronizes staging, renames and
synchronizes the directory. The caller owns exclusive directory access and
runs blocking publication outside Node polling. Uncertain publication fences
reads/writes until reopen. Explicit `recover` requires a present valid record;
initial `new` permits an uninitialized journal. No provider is created implicitly.

`Node::restore_drain` reads the confirmed, nonblocking journal view before the
host exposes a recovered Node. Active intent gates every actual assignment,
including assignments absent from the old manifest; mismatch cannot report
quiescence. Cancelled intent queues explicit campaign enables and opens data
admission only after their exact owner admission tickets finish successfully.
Old queued disables cannot certify enables. This does not retract a delivered
leadership signal or cancel a separate replicated handoff record.

## Evidence

- `regression.log`:196 passing tests: existing credential journal4, drain
  journal6, owning Node168, startup18. These include four new host-Node tests and
  two native TCP/WAL and QUIC/checkpoint histories with pending handoff, active
  restart, cancellation, second restart and ordinary writes.
- `recovery-required.log`: after adding the explicit required-record constructor,
  all7 drain journal tests and18 startup tests pass. The journal tests cover
  exact/stale/conflicting sequences, rejection and uncertain old-or-whole-new
  recovery, all truncated/corrupted bytes, rechecksummed malformed frames,
  maximum retained size, wrong owner, missing required record and interrupted
  native staging. This overlaps the previous run.
- `minimal-tests.log`:163 no-default-feature Node tests pass, including the
  host-supplied journal, stale manifests, cancelled enable ordering and failure
  handling. No native provider is linked in this configuration.
- `focused.log`, `journal.log`, `native.log`: earlier focused runs; the first
  filtered command selected18 existing/new Node drain tests and zero journal
  tests. The journal target was subsequently run without that filter.
- `fmt.log`, `clippy-all.log`, `clippy-default.log`, `clippy-minimal.log`:
  formatting and strict all-target Clippy pass with zero diagnostics.
- `inventory.log`:102 contract metadata/path checks pass, not a safety proof.

`dependency.log` adds an accepted-persistence cancellation check. Its initial
fixture incorrectly assumed a queued client proposal ran before control work;
`dependency-before-order-fix.log` and `minimal-before-order-fix.log` retain that
failure. The corrected setup confirms persistence is pending before installing
the gate; no production check or priority rule was weakened.

The14 new tests and repeated regression counts overlap. Commands and source
hashes are retained alongside these logs. Native restart tests use joined
shutdown/reopen; they do not claim arbitrary process-kill or power-loss
coverage. Host record-I/O tests model uncertain atomic publication separately.

Remaining197b2 work: orchestrate placement/membership and handoffs, authenticate
executable drain commands, reconcile exact original operations and record
completion. Local journal state is not remote-quorum or decommission evidence.
No macOS execution or full P0–P7 completion is claimed.
