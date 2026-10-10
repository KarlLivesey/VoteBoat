# Slice242 — original write after authentication timeout

Base16f01e2972d6db15874c3948579bcd41fc8eaa36. The original-write caller now
repeats only the exact authentication-deadline stdout/interrupted stderr pair,
preserving scope, operation ID, delta and the existing budget. Invalid credentials,
wrong stderr, malformed/extra replies and unrelated authentication errors stop.
Caller expiry prevents another invocation; an ongoing CLI invocation retains its
existing bound. No widened timeout or new hard end-to-end deadline is claimed.
Production CLI auto continues to stop on unknown outcomes.

Actual TCP/QUIC histories keep all three weighted data voters healthy. Only node1's
first command route encounters a bounded TLS stall; the fake listener closes after
accept, its connection observes only the selector and joins on client close. The
test restores the healthy original endpoint manifest synchronously before retry.
Both old-classifier histories fail on the exact native response. The corrected
caller invokes unchanged group7/inc3 add42 delta5 twice and receives its original
duplicate5. Independently seeded operation42 in groups1/7/8 remains isolated;
fresh group7 operation24200 adds2. Values3/7/9 and all retries survive cold WAL/TCP
and actual checked-prefix QUIC checkpoints. Service children stop and join.
This is a controlled transient command fault, not general consensus liveness.

compile-failed.log preserves the initial escaping borrowed observations; owned
String observations fix it before native checks. before.log records0/2, exit101.
after.log passes8; read-all.log passes5 after the shared fake listener cleanup.
Full all-feature counter197 and sequential default counter138 pass on Linux.
Formatting and four strict Clippy profiles are zero. Inventory108 and conformance
metadata9 partial reviews/68 operations are unchanged; no new public seam or
production/core/provider/API/wire/persistent format change.

Preceding240 run38072310155 ends Ubuntu114272594539 counter185/6 and macOS
114272594843 counter189/1. Later operator targets are unrun. Mac again fails
original group7/inc3 write42 with authentication deadline; volatile candidates
do not prove its whole cause. Linux retains six distinct operator failures listed
in provenance.json and minimal excerpts. These are preceding-source outcomes,
not current242 acceptance. User-supplied direct Mac SSH works; its arm64/macOS27.0/
Rust1.98 isolated checkout is prepared, but Mac tests are not claimed here.
Next: exact-source Mac validation, status-wait cleanup and required integration.
Broader P0–P7 remains active; feature and functional Linux/macOS work precede
performance/security. Windows/P8 remain deferred.
