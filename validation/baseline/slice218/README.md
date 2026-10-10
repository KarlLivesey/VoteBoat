# Slice218 — shared-group transport contract review

Shared semantic checks use the same public boxed PeerTransport interface with
an independent downstream logical-frame provider and NativePeerTransport over
controlled HostSessions. Actual NativeOutbound owns every original ticket and
queue lease. The host reference uses the public native codec for frame sizing;
its finite logical pipe does not implement encrypted byte framing or OS I/O.

Three groups share one original batch; a later differently ordered batch waits
behind immutable receive metadata. Partial seven-byte progress, zero/three-call
budgets, invalid budget refusal, foreign queue generation, held flush, retained
terminal slots, drain/abort and exact original credit release are checked. Abort
drops transport frame ownership before its original queue batch is completed.
The same shared checker catches an intentionally early completion before flush.
These are selected obligations, not certification of arbitrary host replacements.
No production transport, batching, deadlines or security scope changes.

Validation:

- `shared-initial.log`: compile failure from a local variable shadowing queue().
  Declaration ordering fixes it; `shared-second.log` passes3 before the final
  invalid-budget/foreign-queue checks were added.
- `transport-all.log`:30 pass/1 sandbox-only PermissionDenied on the existing
  real TCP/TLS socket test. `transport-final.log`: all31 pass with socket access,
  including final shared checks, byte framing, buffer lifetime and actual TLS.
- `transport-default.log`:31 pass; `transport-core.log`:1 object-safety/limits
  check without native dependencies. Independent host/native shared histories
  are selected under native, not claimed as core-only replacement conformance.
- `ledger.json`/`inventory.log`:8 partial contracts,57 reviewed operations and100
  other contracts unreviewed. `ledger-cases.log`:21 metadata cases pass. The
  `node --test` invocation separately reports one passing file; direct execution
  records individual cases. Metadata paths/assertion names are not test proof.
- `clippy-initial.log`: strict all-target/all-feature check passes. Final fmt and
  four strict configurations are retained with final source hashes; no allowance
  or threshold changes. Hook checks remain enabled.

Platform feedback at preceding source0d80216:
[run38051334047](https://github.com/KarlLivesey/VoteBoat/actions/runs/38051334047)
is terminally failed. Ubuntu passes counter166/directory19/transfer21. macOS
counter161/5 fails, so later targets do not execute. Raw job logs and both the
transitional observation and terminal JSON are retained. Five QUIC failures:
assignment authentication deadline, configuration preparing expiry before queued,
busy-history candidate stall, pending/canceled history Busy proposal and ordinary
shared-group write deadline with live candidates/queued peer batches. These are
postfailure sequential observations; poll duration is not owner inter-poll time.
They do not establish one common cause or validate the new test-only provider.

Macro progress: P0 composition obligations advance; actual framed QUIC/cadence
diagnosis is the next linked P2 deliverable. Supported-platform, broader fault
coverage and original P7 gates remain open. Broad security is reserved for the
user's Daybreak run; the full P0–P7 goal remains active.
