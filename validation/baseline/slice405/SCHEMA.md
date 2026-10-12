# Final deep QUIC cleanup qualification405

Purpose: close P8's last full macOS cold-history failure without changing protocol
or relaxing actual owner cleanup. Original402 passes checkpoint/recovery but fails
CloseDrained15s; observed local work is drained and two original transports remain.
The failed log does not expose actual secure deadlines, so it does not prove why.

Shape: reuse routed test-only protocol-close helper via a shared test module.
Generic native nodes retain their original polling budget and diagnostic callback.
At the ordinary15s cutoff require zero local work, no accepted send/attempt/input,
exact original session identities, Closing+flushed with a future actual protocol
deadline. Pin existing bindings/deadlines once. Retained identities may only shrink;
new/reappearing peers, changing deadlines, backward time or late retention fail.
No production provider or watchdog change; no new connection or work may enter.

Ownership and failure: no abort/drop/assumed closure. Return only when each actual
node reports drained. Nested code still joins every original worker, rebinds both
TCP and UDP addresses, and removes its directory. On failure print actual mutable
peer drain status plus existing owning diagnostics. Preserve the original402 log.

Checks: shared negative controls plus existing real long-RTT QUIC test; Linux full
nested all/default sequential in asserted release; routed affected close controls.
Mac repeat only failed full QUIC on frozen source, with source/artifact guards and
assertions/overflow enabled. Existing successful native100/clock/TCP evidence stays
separate. Fmt/four strict Clippy zero; publish checked slice then final source audit.
