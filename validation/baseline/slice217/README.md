# Slice217 — bounded original drain caller continuation

Source base: cf7bf90. Production runner/protocol/timers/storage are unchanged.
The caller confirmation is test-only and reuses a single Command with the same
source, sequence, operation, endpoint file and administrator. At most four complete
invocations are permitted, only after empty stdout and the exact observed initial
admission unknown/no-local-record error. It does not treat absence as rollback,
skip readiness/accepted stop, retry another failure or create a new identity.
Each CLI keeps its original128-request/45s budget; four caller attempts are not
a new global wall-clock deadline guarantee. The production refusal contract is
still directly checked over authenticated channels.

- `caller-before.log`: old single-attempt caller fails the scripted absent-then-
  confirmed-original sequence. The initially unwired wrapper also generates two
  unused-code warnings; wiring both actual callers resolves them, no suppression.
- `caller-final.log`:3 checks pass for original identity, exact classification,
  maximum attempts, terminal other failures and wrong/duplicate success fields.
- `classifier-match.log`: the literal classifier exactly matches the actual
  native CLI error retained in slice216/counter-final.log, including escaping.
- `runner-binary.log`: all19 unchanged production runner checks pass, including
  the explicit source refusal, request/time budgets and exact plan identity.
- `runner-native.log`:13 selected caller/single/group runner tests pass. The
  new QUIC loss/reopen case shares the existing TCP accepted-drain cut. Both
  compare the original journal and plan bytes before loss, after reopen and
  after checked stop. Existing confirmed original data/retry assertions remain.
- `retirement-native.log`: both actual TCP/QUIC unknown learner-retirement and
  stale-source recovery histories pass. Original configuration/retry IDs and the
  restored old-source Draining gate remain required.
- `counter-all.log`: complete final all-feature counter target166 pass.
- `runner-default.log`: final default TCP/caller/group selection9 pass, after all
  all-feature executable processes finish.
- `clippy-initial.log`: strict all-target/all-feature Clippy passes.
- `checks.log`: the enabled pre-push hook passes formatting plus all four strict
  default/all-feature/core-only/native-only Clippy profiles with zero diagnostics.

The prior-source run38050412675 is terminal. Ubuntu passes counter162/directory19/
transfer21. macOS has counter148 pass/14 fail, with later targets unexecuted.
Full raw logs and status JSON are retained; this is not current-source platform
acceptance and does not establish the cause of remaining group liveness failures.
The linked PeerTransport review must distinguish ownership/budgets and controlled
progress traces before any batching change. Full P0–P7 stays active; the schema
and implemented/planned distinction are in docs/IMPLEMENTATION.md. Daybreak
handles broad security review.

Tests use `cargo +stable test --locked --offline --all-features` with
`--test counter_service` (selections: confirmation, drain_runner, drain_retirement)
or `--bin voteboat-counter drain_runner`. Default checks omit `--all-features`.
Native sockets require the socket-enabled environment. Source/artifact hashes
preserve this finite evidence, not a proof for arbitrary providers or schedules.
