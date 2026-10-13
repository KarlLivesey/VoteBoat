# Final Mac405 cold QUIC recovery

The already-running test is now terminal: exact full cold QUIC history1/0,
918.62s, all actual recovery/retry assertions and original owner joins/rebinds.
All stage exits are0, three original test executable plus library guards match,
and1307 source inputs match before/after. Shared controls6/0, routed long-RTT1/0
and original QUIC timer1/0 are included. No production change from402.
The longer test-only protocol deadline path retains original bindings and actual
future timers; no15-second shutdown speed requirement is established. The original
402 failed result remains preserved. This is not a P9 transaction benchmark.

The concurrent Linux routed regression is terminal192/0 in1192.34s, raw parent
routed-all.log. No background405 test remains to gate P9 measurement.
