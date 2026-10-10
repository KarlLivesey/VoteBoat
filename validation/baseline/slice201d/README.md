# Slice201d — shared credential journal conformance

Starting revision d14ab17. One CredentialJournal suite runs through the public
trait on an independent core-only host provider and native memory/file journals.
It checks owner fields, rejection without mutation, exact latest-only retries,
generation floors and trusted generation gaps. Exact retries avoid extra writes.
Two independent file owners retain their state across reopen/drop of the other.

The raw memory/file record-I/O suite checks missing/exact128-byte replacement and
invalid file lengths. Injected certain/uncertain-before/uncertain-after/fenced
outcomes verify distinct retry and reopen requirements. Read errors, corruption
and wrong owner construction return the original I/O handle without replacement.
The older corruption/partial-staging tests run alongside these new cases.

No production code or format changed. Memory-I/O faults model selected schedules;
actual-file reopen does not establish hardware power-loss durability. Exclusive
writer ownership remains the host's obligation. Metadata verifies referenced
symbols/operation coverage, not the semantics of arbitrary providers.

Executed checks:

- all-initial.log: initial20 tests pass before the final strengthened assertions.
- lint-initial.log: rejects an unnecessary explicit drop in the plain host test.
  The fixture uses lexical scope instead; lint rules are unchanged.
- all.log: all20 final provider/journal tests pass with all features.
- core.log: all4 core-only provider tests pass.
- native.log: all20 native-only provider/journal tests pass.
- metadata.log: Node test-runner invocation succeeds.
- metadata-direct.log: all13 metadata checks pass with individual results.
- obligations.json:5 reviewed contracts/35 operations;100 inventory contracts
  remain unreviewed by this ledger.
- pre-push.log: formatting and default/all/core/native-only strict Clippy pass.
- source.sha256 / source-check.log: final source and evidence identity.

```sh
cargo +stable test --locked --offline --all-features --test provider_conformance --test credential_journal
cargo +stable test --locked --offline --no-default-features --test provider_conformance
cargo +stable test --locked --offline --no-default-features --features native --test provider_conformance --test credential_journal
node validation/check-provider-conformance.test.mjs
node validation/check-provider-conformance.mjs
sh .githooks/pre-push
```

Peer credential rotation, broader provider coverage, current Linux/macOS and
separate-host acceptance, lifecycle/fault combinations and P7 remain open.
