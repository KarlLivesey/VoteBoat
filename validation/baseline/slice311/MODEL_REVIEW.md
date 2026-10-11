# Independent two-participant transaction model evidence

Owned source: `tests/transaction_model.rs` and four files in `tests/transaction_model/`. Schema was written before edits in `TRANSACTION_MODEL_SCHEMA.md`. No production transaction helpers are imported; only standard-library collections and local independent protocol/oracle modules are used. No production, manifest, shared documentation or git mutations were made by this task.

## Declared model and completed coverage

Exactly two immutable transactions (amounts 5 and 7), two accounts initially (20, 0), both durable coordinator Starts already admitted, and at most two volatile messages (one slot per transaction). Coordinator state is Started → Decided(Commit/Abort) → AppliedAll → Finished. Participants are Absent → Prepared → DecisionApplied → Released; a refused participant may apply Abort from Absent. Locks survive decision application and release only after the original coordinator AppliedAll. Received prepare/refusal, application and release observations are volatile collections in Started, Decided and AppliedAll respectively. Restart drops these collections and all volatile messages while preserving every durable coordinator phase/proof, original participant fact, balance and lock. Decided copies its exact prepare/refusal proof; AppliedAll copies its exact application proof; Finished copies its exact release proof, clearing each now-unused collection. No receipt arrival itself is treated as a durable coordinator command. The model abstracts committed/applied protocol commands, not separate Raft log/quorum/recovery steps.

The initial depth-40 run passed but was truncated: 38,280 states, 223,863 transitions. Root inspection identified that both sequential commits require about 60 Send/Deliver actions, so this was not adequate terminal search coverage. The final declared depth limit is 80 transitions and state ceiling is 250,000. The final test now requires exhaustion without reaching either cap, so future truncation cannot silently pass this acceptance claim.

Final exploration: **126,818 distinct reachable states**, **827,613 checked transitions**, **222,264 loss edges**, **126,415 restart edges**; `depth_limited=false`, `state_limited=false`, `exhausted=true`. Distinct state counts (not independent histories or transaction-completion totals): **15,774** contain Finished(Commit); **6,964** contain Finished(Abort); **242** have both transactions Finished(Commit); **704** have a Finished commit/abort pair. The reported categories may overlap.

The independent oracle checks immutable decisions, phase-scoped volatile observation provenance and durable committed proofs, retained original lock ownership, matching decisions at participants, both applications before AppliedAll, both releases before Finished, nonnegative balances and conservation when both accounts are readable. It receives no weakening selector and invokes no transition implementation. The trusted message generator scopes all receipts to exact durable original facts/content. This assumes root's trusted host-driver provenance and does not certify a native adapter or authenticate arbitrary remote certificates.

Seven selected controls accompany exploration: both commits finish and original Apply retries do not reapply; an actual contending commit/abort pair finishes plus insufficient-funds Abort; prepare-receipt loss/restart and timeout keep reads blocked until exact AppliedAll; application/release receipts reconstruct from durable outcomes after loss/restart; and three concrete weakening counterexamples. There are eight tests total including graph exploration.

## Concrete weakening failures

- From a valid durable Prepared prefix, timeout-abort produces `[Timeout(0, 0)]`: `participant outcome has no matching coordinator decision`.
- From a valid Commit with one participant application, premature release produces `[Send(Release(0, 0, Commit)), Deliver(0)]`: `participant released before exact AppliedAll`.
- From a valid original Commit, unstable decision produces `[Send(Decide(0, Abort)), Deliver(0)]`: `durable coordinator decision changed`.

These are suffixes discovered by a separate breadth-first search (at most three extra transitions) from explicit valid prefixes, not hard-coded success flags.

## Actual checks and retained output

All commands exited 0 against the final owned source:

```text
CARGO_TARGET_DIR=target/divide-membership/build cargo +stable test --locked --offline --test transaction_model -- --nocapture
rustfmt +stable --edition 2021 --check tests/transaction_model.rs tests/transaction_model/protocol.rs tests/transaction_model/oracle.rs tests/transaction_model/search.rs tests/transaction_model/controls.rs
CARGO_TARGET_DIR=target/divide-membership/build cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
```

Test result: **8 passed / 0 failed**, final execution 2.40 seconds. Owned-file formatting: clean. Full-target all-feature strict Clippy: **zero diagnostics**, authorized by root after the shared tree settled. Retained logs: `transaction-model-initial.log`, `transaction-model-expanded.log`, `transaction-model-final.log`, `transaction-model-format.log`, `transaction-model-clippy-initial.log`, `transaction-model-clippy-final.log`, `transaction-model-volatile-observations.log`, `transaction-model-clippy-refinement.log` (the final strict output). Frozen owned-source digests: `transaction-model-source.sha256`.

No selected check failed. The initial depth truncation was an inspection-discovered coverage limitation, corrected by changing the declared search envelope and terminal-state accounting. A second concrete root review corrected the abstract ownership of received observations: the earlier durable-mask graph exhausted at 98,494 states but did not represent volatile native-driver receipts faithfully; the final refined graph above does. Selected controls now explicitly restart after received prepare/application/release observations, requiring recollection while preserving the exact committed phase proofs. All five source files also use the full RPL-1.5 notice consistent with adjacent tests. No lint levels, safety rules or production deadlines were weakened.

This exhausts only the exact finite abstraction above. It is not general liveness, arbitrary participant/account counts, arbitrary transaction programs, moving group ownership, business API admission, host receipt provenance implementation, native quorum/crash proof, or native two-group transaction acceptance. P8 remains active; root implements and qualifies the actual embedding/service path separately. No other feature prerequisite is inferred from these explicit limits.
