# Slice208a — bounded TCP connection fairness

Starting revision a60ab8a. The native TCP connector shares one socket-call
budget across anonymous prefaces, authorized outbound prefaces and accepts.
Fixed priority previously let one stalled anonymous socket consume every call.
Scanning unserved slots could also reset the anonymous cursor to the same slot.

The change rotates first access between these three phases and preserves socket
cursors when no credit remains. Zero-socket-credit polls preserve the phase.
Each phase is visited at most once per poll; original capacities, exact tickets,
TLS budgets, timeout/cancellation and close/drain contracts remain unchanged.

Four real TCP regressions keep virtual time fixed and leave a stalled anonymous
stream alive while an authenticated peer completes under one-call budgets:

- A later incoming connection gets accepted and completes.
- A later preloaded anonymous slot progresses with visits equal to capacity.
- An authorized outbound preface progresses despite the stalled anonymous peer.
- Interleaved zero-I/O polls do not consume the next positive-budget turn.

The tests require original completion identities and authenticated sessions,
check anonymous/request capacity and retain authenticated returned sessions
through connector shutdown/dial-worker join. They do not establish arbitrary
load fairness, a latency SLO, shared receive/control fairness or macOS behavior.

Evidence:

- before.log: the initial test setup exposed only one failure. Empty slots in
  the other cases let calls through; this was insufficient saturation.
- before-exhausted-capacity.log: corrected saturated fixtures; all three original
  regressions fail before the production fix.
- after.log: those three pass after phase/cursor rotation.
- regression.log/default.log/pre-push.log: passing intermediate 125-test,
  18-test and strict-lint runs before adding the zero-budget case.
- zero-before.log: the new zero-budget regression fails after three seconds
  before the positive-credit guard, with the authorized preface still pending.
- final-regression.log: all126 all-feature tests pass (connect19, peer_rotation7,
  quic_connect12, runtime28, startup32, transport28).
- final-default.log: all19 default-feature connector tests pass.
- final-pre-push.log: formatting and strict Clippy pass for default, all-feature,
  core-only and native-only configurations with zero diagnostics.
- inventory.log and metadata.log:106 contract paths and13 metadata checks pass.
  These validate metadata, not runtime conformance.
- source.sha256 / source-check.log: final source and evidence fingerprints.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test connect zero_socket_budget -- --nocapture
cargo +stable test --locked --offline --all-features --test connect --test quic_connect --test peer_rotation --test transport --test runtime --test startup
cargo +stable test --locked --offline --test connect
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

The first command intentionally failed before the guard. The same test passes
in final-regression.log. Socket tests executed with loopback access. P0–P7 stays
active; next is shared receive/control fairness, then persistent discovery.
