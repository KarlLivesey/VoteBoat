# Original protocol close qualification405

No production change. The final402 Mac QUIC cold history reached actual final
shutdown but two original transports remained after the fixture's15s cutoff.
The old diagnostic did not capture secure deadlines; no timer cause is inferred
from that record. The complete failed result is retained in
[402/mac_quic](../slice402/mac_quic/README.md).

This slice shares the existing routed test's conditional protocol-only close
handling with the nested test. The ordinary15s work cutoff stays. A continuation
requires zero local work, no accepted sends/connection attempts/unread input,
exact original bindings and flushed Closing sessions with actual future protocol
deadlines. Original deadlines cannot renew, identities may only disappear, and
caller time cannot regress. Success still requires actual Node.is_drained,
original worker joins, TCP/UDP listener rebinding and directory cleanup.

The1307-input source manifest includes no production changes from402; seven
fixture inputs changed/added and the original routed contract file moved. Source
and schema records are included. Assertion-enabled release checks:

- Shared close controls6/0, including reappearing-peer and time-regression refusal.
- Existing real routed QUIC long-RTT close1/0; original QUIC timer control1/0.
- Formatting and strict Clippy in all four feature configurations: zero.
- Full Linux nested all15/0 (124.51s), default13/0 (46.77s), run sequentially.
- Full routed regression and matching full Mac QUIC recovery: pending.

The Mac runner uses a fresh frozen source directory and cloned build cache,
checks compiler-reported assertion/overflow/optimization/features, records the
original three test executables plus library, and guards source and artifacts
before/after every selected case. Successful402 TCP/403 native100/401 clock
qualification is retained separately;405 does not claim to rerun those checks.
