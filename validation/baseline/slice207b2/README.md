# Slice207b2 — counter executable peer credential rollout

Starting revision5fe802b. The counter's explicit `--peer-credentials FILE`
manifest selects a generation and immutable TLS directory. A shared internal
typed worker reads bounded material and durably records its exact digest before
the generic native Node publisher replaces keys. Command-channel material and
its journal remain independent. Static/member/multi-group startup uses207b1's
exact record checks. Configure/Inspect authorization covers every actual local
group, including deployments with no group1. Uncertain preparation fences later
requests until restart; shutdown/drop retain the worker through its outcome.

Executable TCP/QUIC histories replace CA/leaf/key material while retaining
command access, original data receipts and quorum reads. An unread request is
followed by process death, original-ID status recovery and exact retry. Stale,
changed, newer unrecorded and omitted startup selections are refused. Invalid
preparation keeps the current generation usable. Dedicated tests cover partial
group permissions, actual groups without group1, and explicit member recovery.

Worker tests cover one held preparation, exact busy retry, command revocation
on uncertainty, refusal of subsequent preparation, and a joined-but-unpublished
peer record installed on restart. These are process/file histories, not hardware
power-loss certification. Transfer/directory peer-command integration remains
207b3; no cluster-wide atomic rollout or general audit history is claimed.

Evidence files:

- peer-initial.log: the first4 TCP/QUIC single/multi-group histories pass.
- all.log:200 broader all-feature tests pass (27 counter unit,5 transfer unit,
  135 counter service,17 directory service,16 transfer service). This predates
  the final actual-scope authorization and sticky-fence changes, extra worker
  tests and explicit member profiles; later checks cover that final source.
- worker-peer.log: initial2 peer worker tests pass, with the unused-mut warning
  retained. lint-final.log records the warning and a needless borrow; both fixed.
- lint-initial.log / lint-final-fixed.log / lint-complete.log: passing strict
  all-feature snapshots during implementation.
- final-counter.log: retained failed run (136 counter histories pass, two new
  member fixtures incorrectly try explicit deployment creation). peer-final.log
  retains the subsequent refusal-helper mode failure; peer-verified.log retains
  an overly strict first-reply duplicate assertion after an unknown write.
  The fixture now creates legacy membership before explicit recovery, uses the
  actual recovery mode, tracks original-ID retries and verifies a quorum read.
- peer-checked.log: all7 final peer rollout histories pass.
- counter-verified.log: all172 final all-feature regressions pass (29 counter
  unit,5 transfer unit,138 counter service), including both shared-worker fixes.
- default-*.log:39 default-feature tests pass (34 binary unit,4 peer rollout,
  1 command-access reload), run after the all-feature executable processes finish
  so Cargo does not replace their binaries.
- pre-push-verified.log: final formatting plus strict default/all/core/native-only
  Clippy all pass. Earlier pre-push logs are passing intermediate snapshots.
- inventory.log / metadata.log:106 entries and13 obligation metadata checks.
  The5-contract/35-operation conformance ledger is unchanged;101 entries remain
  unreviewed by that ledger. Metadata checks do not prove runtime conformance.
- source.sha256 / source-check.log: final source and selected evidence identity.

```sh
cargo +stable test --locked --offline --all-features --bin voteboat-counter --bin voteboat-transfer --test counter_service --test transfer_service --test directory_service
cargo +stable test --locked --offline --all-features --bin voteboat-counter --bin voteboat-transfer --test counter_service
cargo +stable test --locked --offline --bin voteboat-counter --bin voteboat-transfer
cargo +stable test --locked --offline --test counter_service peer_credentials
cargo +stable test --locked --offline --test counter_service credential_reload
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```
