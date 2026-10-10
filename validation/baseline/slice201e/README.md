# Slice201e — shared admission provider obligations

The existing host and native admission policies run the same32 seeded histories
of256 actions: reserve, clone/drop lease handles, close/drop/replace policy
views and refuse oversized costs. A separate model sums unique reservation
identities and compares exact live credit totals independently of either
provider's counters. A negative case verifies detection of early release.

Explicit cases exhaust only batch capacity, verify message/byte rejection,
transfer the last lease owner across threads while closing the admitting view,
and count exactly one destructor for an opaque token shared by eight threads.
A deliberately faulty early-release provider is expected to fail the model.
No sleeps establish correctness; channels order the threaded ownership checks.

## Executed evidence

- `initial.log`: the six new admission tests and one existing snapshot test
  selected by the filter pass before the explicit batch-only case was added.
- `regressions-all.log`: provider22, admission8 and outbound4 pass. Transport
  records27 passes and one socket permission refusal in the sandbox.
- `transport-all.log`: all28 transport tests pass with the required local-socket
  permission, including the real TCP/TLS regression.
- `core-only.log`: provider8 and admission1 pass without native features.
- `native-only.log`: provider22 and admission8 pass without TLS/QUIC.
- `pre-push.log`: formatting and strict Clippy pass for default, all-feature,
  core-only and native-only configurations.
- `metadata-final.log`: all15 metadata tests pass. `obligations.json` records
  six reviewed contracts/39 operations and102 unreviewed inventory entries.
- `inventory-final.log`:108 contract entries have valid metadata and paths.

The initial Clippy run reports one divisibility-expression lint; the expression
was corrected. Initial ledger validation mistakenly treated the intentionally
panicking checker test as a successful provider runner. It remains a referenced
assertion/self-check, separate from the five successful host/native/token
runners. Both initial failures are retained.

Reproduce with:

```sh
cargo +stable test --locked --offline --all-features --test provider_conformance --test admission --test outbound --test transport
cargo +stable test --locked --offline --no-default-features --test provider_conformance --test admission
cargo +stable test --locked --offline --no-default-features --features native --test provider_conformance --test admission
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

The selected budget profiles accept valid uncontended costs up to declared
limits. This does not require every custom admission policy to make identical
decisions. Generated histories are bounded and sequential; threaded cases cover
specific ownership orders. Existing native queue/transport tests supply hard
limit/control and payload ownership evidence. No production behavior, protocol,
format, dependency or public seam changed.

The prior operator CI result is retained only to select the next platform work:
run38041871867 at942fd93 passes Ubuntu but has11 macOS counter failures. It is
not current-source macOS acceptance. General client/disk policies, arbitrary
concurrency, remaining provider families and full P0–P7 acceptance stay open.
