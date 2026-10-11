# Temporary destruction-only shutdown diagnostic

The original QUIC checkpoint split history passes on Linux:1 passed,0 failed
in71.53s. No nonterminal closing provider reaches destruction, so this run
captures no stalled state and does not explain the failures in285/288.

The frozen669-input image records transport/session state only from temporary
Drop instrumentation, with no per-poll logging. The executed before-run and
terminal source guards verify. This probe has no four-profile strict qualification
and is not production code. Its zero-context patch applies over288; no protocol,
timer, ownership or acceptance condition changed. Do not repeat this unchanged
passing probe as a repair claim.
