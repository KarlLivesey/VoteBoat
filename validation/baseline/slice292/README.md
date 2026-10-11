# QUIC close timer follows explicit caller time

Linux/macOS each pass the complete19-test QUIC target in0.03s and0.06s.
Formatting and strict all-target Clippy for all/default/no-default/native-only
configurations have zero diagnostics on both. Executed before/terminal671-input
guards verify, and current Rust publication inputs match. The zero-context
runtime patch applies over291's frozen source.

The added regression holds the actual first handshake datagram until3500ms of
caller time, observes real UDP readiness, and drives both authenticated peers
until their accepted hello output is fully flushed. After peer removal the
surviving session stays Closing at15s, then closes at its own unchanged public
protocol deadline. No application batch is admitted in this case;291 separately
checks exact framed send/input ownership. No production or protocol timer changes.

The initial exploratory control reports start3502ms/deadline39022ms,35520ms
apart. That run has no separately executed before-run source guard; retain it
as exploratory evidence only. Final671-input qualification is independent.

This demonstrates that resolved output alone does not imply a15s QUIC protocol
drain deadline. It does not diagnose the original285/288 parent shutdown fault.
The silent292-probe original histories all pass without a failed-state capture;
their separate archive does not claim a repair. Mac285 continues independently.
