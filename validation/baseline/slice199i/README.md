# Slice199i — original directory initialization recovery

Starting revision3df2fa9. The five directory failures from older run38036099346
are retained in slice199h/prior-a41a6ec-linux.log. Those fixtures incorrectly
require immediate success despite an explicit UNKNOWN leadership-change reply.
Fixtures now retry only exact leadership outcomes, at most four times, using
the original immutable plan operations100/101 and validating exact receipts.
Other failures remain terminal. Production adds only an admission diagnostic.

Two native TCP/QUIC histories admit initialization/publication with no live
quorum, interrupt the waiting client and leader, recover all nodes and resolve
the original operations. Duplicate retries, authoritative manifest bytes and
unchanged plan bytes survive restart. QUIC checkpoints through the committed
prefix. The cut proves admission before interruption, not pre-failure commitment
or physical power-loss safety. Two policy tests reject unrelated errors, changed
receipts and unlimited retry.

Executed on Linux, in sequential executable feature profiles:

- initialization.log: all4 new tests pass.
- directory-all.log: all17 directory/recursive-route tests pass.
- directory-default.log: all14 default-feature tests pass.
- pre-push.log: formatting and strict default/all/core/native-only Clippy pass.
- source.sha256 and source-check.log: final source/evidence identity checks.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test directory_service initialization::
cargo +stable test --locked --offline --all-features --test directory_service
cargo +stable test --locked --offline --test directory_service
sh .githooks/pre-push
```

Current macOS/separate-machine acceptance, combined lifecycle/revocation and P7
remain open. No complete baseline or platform pass is inferred from these tests.
