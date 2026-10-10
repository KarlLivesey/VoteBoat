# Slice204 — drain configuration reply recovery

Starting revision: `d691c164ce6fd86c29d079155a558a49471431e2`.
Both configuration runner paths now reobserve the bound source journal after
specific request/reply timeouts and connection closure. Original identities,
the absolute deadline and request count remain unchanged. Authentication and
malformed-response failures remain terminal. No consensus/storage format changes.

## Executed evidence

- `before.log`: the authenticated withheld-reply fixture fails on the original
  production code after begin/resume/configure, with the same configuration
  reply timeout recorded on Linux and macOS. It never reaches journal observation.
- `runner-unit.log`: eleven selected tests pass. Includes real withheld-reply
  recovery through pending status, same-ID retry, ready status and shutdown;
  another real timeout exhausts the request budget with no shutdown. Refusal
  tables and existing budget/identity/plan checks also pass.
- `replacement.log`: two actual service-process histories pass, TCP and QUIC,
  retaining the exact imported learner store, original retries and restart checks.
- `runners.log`: nine actual single-/multi-group runner histories pass, including
  interrupted runner/source, partial membership/checkpoint recovery, authorization
  and changed-identity refusal. The old whole-invocation retry workaround is gone.
- `pre-push.log`: formatting and strict Clippy pass for default, all-feature,
  core-only and native-only configurations, with zero diagnostics.
- `source.sha256` / `source-check.log`: source/test bindings for these results.

Commands:

```sh
cargo +stable test --locked --offline --all-features --bin voteboat-counter configuration_reply_timeout_observes_original_journal_before_retry_and_stop -- --nocapture
cargo +stable test --locked --offline --all-features --bin voteboat-counter drain_runner -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service drain_replacement:: -- --test-threads=1
cargo +stable test --locked --offline --all-features --test counter_service drain_runner:: -- --test-threads=1
.githooks/pre-push
```

The first command is the before-change capture. An initial test-fixture module
path, TLS-readiness check and sandbox loopback permission were corrected before
that capture. Socket tests require local listener permission. These are selected
Linux results, not a full-suite/macOS acceptance certificate. Source-command
timeouts and other platform failures still need separate diagnosis; no broad
retry of authentication errors was introduced.
