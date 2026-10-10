# Slice199f — cancellation recovery and acceptance

Starting revision a41a6ec. The retained Linux failure at6f26bc1 rejected the
documented UNKNOWN LeadershipChanged outcome from cancellation. An unchanged
isolated run passes; it does not reproduce a consensus defect. The fixture now
selects a current leader, preserves the exact operation ID on the narrowly
recognized leadership-change responses, and verifies a fresh quorum-backed
Cancelled record. The production CLI still stops on uncertainty.

Two new real-service histories cut the source after cancellation admission while
both other voters are unavailable. Recovery can legitimately commit the retained
suffix, so either the original Pending or original Cancelled record is accepted;
absence, changed identity and other terminal states are rejected. Retrying the
exact cancellation must reach Cancelled, survive another restart, remain
idempotent and preserve original data retries. TCP uses WAL recovery; QUIC also
requires checkpoints through the observed committed prefix before the cut and
after cancellation. There is no claim of phase-internal hardware power loss.

Only a cancellation-admission diagnostic changes production code. It is used to
cut after actual proposal admission instead of a sleep; it proves neither
durability nor commitment. No protocol, storage format, timeout or production
client retry behavior changes.

## Executed checks

- `initial-isolated.log`: unchanged historical lost-drain test passes locally.
- `cancellation.log`: both initial source-loss histories pass.
- `counter-service.log`: all124 all-feature counter service tests pass. This run
  precedes the final stronger checkpoint-prefix assertion; the focused final
  runs below execute that assertion.
- `leadership-final.log`: all11 matching leadership/group-leadership tests pass
  with all features, including both new histories.
- `drain-final.log`: the corrected lost-drain test passes on final source.
- `leadership-default.log`: all8 default-feature leadership checks pass,
  executed separately after all-feature service execution.
- `pre-push.log`: formatting and strict Clippy pass for default, all features,
  no default features and native only. `clippy-all.log` is the earlier direct
  all-feature scan.
- `source.sha256` and `source-check.log`: relevant source/test hashes verify.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test counter_service
cargo +stable test --locked --offline --all-features --test counter_service leadership::
cargo +stable test --locked --offline --all-features --test counter_service lost_drain_reply_keeps_original_identity_and_local_cancel_is_durable
cargo +stable test --locked --offline --test counter_service leadership::
sh .githooks/pre-push
```

These are Linux executions. They include the names of the historical macOS
failures but do not establish successful macOS execution or explain all QUIC
timing failures there. Current199g retains those failures; broader combined
lifecycle/revocation, provider conformance and P7 acceptance remain open.
