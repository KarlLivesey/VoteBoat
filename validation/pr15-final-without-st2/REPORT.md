# PR15 independent DR1 reconciliation — source preparation only

Existing PR15 (`lb/dr1-session-ledger`), not a merge or a completed LB milestone.
Task brief/client request: `winddown-dr1-without-st2-reconcile-sol-20261021`;
no returned taskId was exposed to this implementation agent. No delegation.

## Pinned source and retained history

- Original independently reviewed DR1: `79829e871b2c4131700f51a7c2df978db8fc7618`,
  base `4920534e81195eeefb71dd940d6ee895381fa48e`. All eight review rounds remain.
  ST2 `bbb81f33` is not an ancestor of that original source.
- Production main: `bd714771e181d59c2cfaece3fc93dc411ce41ddb`; parent docs-only
  main `7dbd932db307f3c2915bcc6def26c2e1de0ef2ff` is included. Its PR14 closure,
  independent DR1 scope and NT2 wind-down deferral are preserved.
- Real reconciliation merge `46664109` has parents original DR1 and production
  main. Only documentation conflicted. Parent disposition merge `12a3bf5e`
  has parents `46664109` and `7dbd932d`; its sole conflict retained the parent's
  corrected mini-plan rows. Desired tree was committed and audited first.
- History-retention merge / checked source head:
  **`b6d9f9112971ff6c3ab1ed17bbb73cc65c82e660`**, parents
  `12a3bf5eb0db253115adfd975c75b3bee056ffff` and published
  **`b0ff8c0fa9b2f777363352ebc38ae4f477030431`**. Explicit `ours` strategy
  retains all published ancestry while its tree equals the independently
  constructed desired tree `14e28f59cda9ecbd55e7e006d3f4e0d9a4c2e6e5` exactly.
  This is not a revert of b0ff followed by an ineffective merge of old main.

ST2 remains in historical ancestry intentionally, **not in this candidate's
source tree**. Its WAL admission/default-maintenance/storage changes and
startup retention/backpressure tests are excluded. Of all 68 source/test paths
changed by ST2 against pinned main, 53 match main or are absent; 15 overlap
independent original DR1 changes, incorporated by the real original+main merge.
The raw path audit and construction/ancestry checks make that distinction
machine-readable. PR14 is closed without merge; its failed gate is not cleared.

Historical reports remain in their original Git objects, including
[b0ff's focused evidence](https://github.com/KarlLivesey/VoteBoat/blob/b0ff8c0fa9b2f777363352ebc38ae4f477030431/validation/pr15-final-reconcile/REPORT.md).
Their old passes and source approvals do not qualify this head.

## Contracts and minimal integration

The small schema/API/ownership/failure/partial-progress/acceptance plan was
recorded before conflict edits, then included in `docs/IMPLEMENTATION.md`.
Ten invariant sources (ledger and its tests, Counter, readiness verifier,
first-claim codec/binding, logical/transaction traits and opening images) are
byte-identical to approved original DR1. Its exact acknowledgement floor,
allocation watermark across closure/eviction, pure command-window/closure,
wrapper Undeclared and complete identities/plans, committed bootstrap binding,
no premature positive readiness and original retry outcomes are preserved.
No indispensable ST2 semantic dependency was found.

Necessary adaptations reuse the previously reviewed b0ff adapters: readiness
fixtures use EntryBatch/EntryRange/apply_entries (explicit StateMachine call
where an inherent slice API exists); enrollment clones its sole decoded entry.
EO1 and N0 warmup/offering use existing session-1 IDs and 16-byte Counter
commands. Index, window, outcome, continuation and resource assertions remain.
The required first-committed-bootstrap consequence sentence is retained verbatim.

The initial plan named a snapshot-anchor adapter for investigation. The audit
showed that anchor fixture and combined-manifest behavior belong to ST2, absent
from main; neither was imported. Existing main/DR1 snapshot-worker fixtures are
retained. No ST2 test or replacement maintenance was copied. Existing padding
and bounds are unchanged wherever present.

Live cluster and offered summaries now derive `payload_bytes` from
`COUNTER_COMMAND_BYTES` (16); the existing cluster receipt/counter check also
requires 16. Genuine historical 8-byte campaign records are unchanged.

DR1 bounds application-session retention. Default end-to-end automatic lifetime
maintenance remains deferred without ST2; existing opt-in scheduled checkpoint
and reclaim APIs remain. Static-bootstrap/checkpoint-less configuration admission
and the recorded DR2/TX1 work remain deferred. No new prerequisite or feature.

## Actual bounded validation

- **Default:** 223 passed / 0 failed / 0 ignored, 18 nonempty focused commands.
- **All features:** 179 passed / 0 failed / 0 ignored, 15 nonempty focused commands.
- Both configurations: an additional offered-mode 100-write execution verifies
  actual `payload_bytes=16`, all 100 Applied, zero Unknown/pending, recovery/retry
  verification and joined workers. Both cluster TCP runs and all-feature QUIC
  retain their existing receipt/count/term/read/lifecycle assertions plus 16B.
- Coverage: exact ledger floors/allocation/closure/eviction/restore, independent
  model histories and the one-million-write soak; full learner suite with exact
  identities, activation maps, opening images, bootstrap and relay charges;
  both rr7 bootstrap-voter/candidate regressions and exact first-claim bound;
  runtime queue charging, aggregate admission, Counter sessions and opt-in
  checkpoint/reclaim restart, leadership wrapper/profile identity, EO1 and
  snapshot-worker faults, wire checks, and U2's 1,000 seeded schedules.
- Exact step argv/env/counts/statuses are retained in `default-results.json`,
  `remaining-results.json`, `focused-summary.json` and per-step command/log files.

**Enabled qualification hook: 7/7 checks passed, exit 0, zero diagnostics.**
Root commands:

```sh
cargo +stable fmt --all -- --check
cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
```

The actual unchanged hook generated all seven audited per-Cargo rc0 records;
`core.hooksPath=.githooks` remains enabled. Its matched checks use that workspace's
comparison-only lock and the same owned target. Production/test/Cargo/matched
input manifest (2,611 files) remains unchanged through these checks:
`1d9e1d54dbc4c87cce4dfdcecd2b0d7e0ef90208d795572ee28d2d534025ac26`.
The hook separately matches pinned main byte-for-byte. The history-only merge
and subsequent report/docs changes do not alter any checked source input.

Linux stable, locked/offline, configurations sequential; only this checkout's
`target/`, also selected for matched. Four Cargo jobs, CARGO_INCREMENTAL=0,
affinity 0–3, absolute kernel nice19 before each launch and observed-thread
resource audits. Stock diskguard before every Cargo launch including each hook
step; own target cap 60 GiB, shared caps unchanged. No assertion, lint level,
resource bound or deadline was weakened. Aggregate status is nonzero on any
failed/empty acceptance test or failed check, independently of suites.sh's
potential final-status masking; suites.sh was not used.

An initial format launch was aborted for a resource-priority audit failure
before any build/test qualification. Its raw audit is retained. The launcher
resets absolute priority after the guard and before child exec; subsequent
focused/check evidence uses clean observed-thread audits. No test failure is
reclassified as a pass.

## Remaining qualification and publication

Fresh independent pinned-object source reviews and the exclusive whole
five-configuration final matrix are **parent-owned and still required**.
No full matrix, exhaustive relay, performance gate or macOS test was run here.
The old all-feature port-overflow failure is historical and not cleared by
these focused tests; no ST2-only transfer/port repair was imported. No CI watch,
new research, roadmap slice, PR, main write, merge or PR closure was performed.

Publication is nonforce to the same PR15 branch through the unchanged enabled
`.githooks/pre-push`; the exact final published head and actual push-hook result
are recorded in the durable PR report and raw final artifact. This report-only
commit preserves the checked production/test/Cargo/hook input hashes.

Raw exact argv/env/logs/statuses, patches, source-preservation and ancestry
reports: main checkout `target/claude/research-lv/pr15-final-without-st2/` (reports
only; no main source or memory edits). After publication/report, mandatory
`tools/sweep.sh --worktree` removes this owned checkout/target; process/scratch
and final stock diskguard results are recorded there. No detached or scheduled
work remains authorized by this task.
