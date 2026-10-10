# Slice203 — explicit native compatible merge

The v3 merge/retirement profile composes existing TransferOperation,
TransferSource, TransferTarget and RetirementGuard contracts. No consensus,
durability, application wire format or public trait is replaced.

## Scope and failures retained

- `merge-before.log`: all three new merge histories reject their first source
  write. The executable constructed a whole-responsibility RouteHint for a
  partitioned source. Source construction and hints now select its exact route.
- `merge-retirement-assertion.log`: both eight-boundary recovery histories reach
  activation and retirement; the ordinary client completes a two-image merge.
  All fail a test comparison of mutation-plus-status and later status-only text.
  The corrected assertion compares durable fields and omits advancing read_index.
- `profile-tests.log`: two negative-profile tests pass, covering identity,
  boundaries, role/completeness, incarnation, operation and version-shape refusal.
- `transfer-contracts.log`:33 tests pass across operation/source/publication/target
  contracts, including complete multi-source coverage and collision refusal.
- `pre-push.log`: formatting and default/all-feature/core/native strict Clippy pass.
- `transfer-service.log`: the complete all-feature executable target passes all10
  tests in304.60s: existing split/retirement tests, two merge recovery histories
  and the ordinary two-source merge client. Each merge recovery history reopens
  all roles at eight boundaries and interrupts admitted import/activation waits.
  Both source retry histories, non-serving staging, fencing, independent release
  and fresh target writes with metadata/sources stopped are asserted.

Tests run on the local Linux host with loopback sockets, filesystem persistence and native
child processes. Source hashes identify the final source, not the initial failed
test assertion. These are selected process-crash/receipt-loss histories, not
arbitrary power-loss, separate-machine or macOS certification.

Commands (all with `cargo +stable`, locked/offline):

```sh
cargo +stable test --locked --offline --all-features --bin voteboat-transfer profile::tests -- --nocapture
cargo +stable test --locked --offline --all-features --test transfer_operation --test transfer_source --test transfer_target --test transfer_publication
cargo +stable test --locked --offline --all-features --test transfer_service -- --nocapture --test-threads=1
sh .githooks/pre-push
sha256sum --check validation/baseline/slice203/source.sha256
```

`source.sha256` and `source-check.log` bind the final code and executable test
helpers. Initial compile checks also exposed a missing constant import and
helper visibility, both corrected before these passing tests. The resulting
client bounds image bytes by its existing64KiB command budget.

## Prior platform run, unrelated source revision

Operator run38034032131 at71da217 finished during this work. Ubuntu
job114160726976 passes122 counter,13 directory and7 transfer histories.
macOS job114160727069 passes116 counter histories and fails six QUIC histories:
assignment pages, multi-group source drain, independent group commands,
group leadership, the partial-membership drain runner and trusted startup
promotion/restart. Directory/transfer targets do not run after that failure.
The two `prior-71da217-*` files retain those raw logs. These results do not
validate slice203 or the later7fca6a4 route fix. Current macOS acceptance remains
open; no deadline is widened or failing history disabled by this slice.
