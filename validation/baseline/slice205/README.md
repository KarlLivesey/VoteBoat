# Slice205 — bounded source-observation recovery

Starting revision: `6f26bc1620ebc1f8a52ba21984c60bde6ba7fccc`.
Only original-ID source status and assignment-page reads use the new retry loop.
The same source, sequence, operation and offset are retained, and every exchange
uses the original overall deadline and decrements its request count. Recognized
transport interruptions, unavailable connections and pre-command authentication
deadlines can retry. Invalid authentication, malformed data and explicit errors
remain terminal; successful replies still pass the existing status/row validators.
Mutation/stop commands retain their existing unknown-outcome behavior.

## Executed checks

- `before.log`: source-status read times out on the old one-shot behavior; the
  new authenticated fixture reproduces the recorded source-request error.
- `runner-unit.log`:18 pass, including seven new observation tests for withheld
  status/page replies, identical offset, stalled authentication, request limit,
  an absolute deadline expiring during authentication, and completed invalid
  replies. Includes the prior configuration timeout and shutdown-gating cases.
- `runners.log`: nine native service-process runner histories pass, using TCP
  and QUIC with original operation IDs, interrupted source/runner recovery,
  checkpoints, mixed group membership and refusal cases.
- `replacement.log`: both TCP/QUIC replacement-drain histories pass, preserving
  exact new-store membership and original application retry/restart checks.
- `pre-push.log`: formatting and all four strict Clippy configurations pass.
- `source.sha256` / `source-check.log`: production/test bindings for this run.

Commands:

```sh
cargo +stable test --locked --offline --all-features --bin voteboat-counter lost_source_status_reply_repeats_only_original_observation -- --nocapture
cargo +stable test --locked --offline --all-features --bin voteboat-counter drain_runner -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service drain_runner:: -- --test-threads=1
cargo +stable test --locked --offline --all-features --test counter_service drain_replacement:: -- --test-threads=1
.githooks/pre-push
```

The first command captures pre-change behavior. Actual socket tests need local
listener permission. These are selected Linux tests, not full-platform acceptance.

## Older platform evidence

`prior-446c807-operators.log` is the completed run38034753904 at
446c807502ba88efda9fe4f75358d1e114bd5474. The raw prefixes identify each platform:

- Ubuntu job114162836829:122 counter and13 directory passes; transfer nine pass,
  one failure (`quic_retirement_recovers_unread_proposal_and_retired_checkpoint`,
  initialization returned unknown after leadership changed).
- macOS job114162836707:115 counter passes/seven failures; later targets not run.
  Includes a multi-group source-request timeout without the command name. This
  does not establish whether that particular interruption was a read or mutation.

Do not transfer that run's outcomes to the current revision or claim all source
timeouts are fixed by read-only retry. Group handoff/mutation waits and other
platform failures remain open, as do provider conformance and P0–P7 acceptance.
