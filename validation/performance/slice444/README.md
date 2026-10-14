# Pi qualification exposed two test-clock races

Published baseline6be136ae passes63 of64 Pi framework tests; snapshot catch-up
loses a response. An error-only diagnostic stalls and is explicitly interrupted,
with its baseline restored. The narrower trace establishes that the receive-paused
follower still campaigns, displaces the leader and stalls four pending responses.

Suppressing paused ticks fixes writes but fails exact final-prefix verification.
A second trace establishes the resume race: the unpaused follower campaigns before
queued contact is processed; a new leader advances every replica to12002 instead
of12001. A snapshot was installed and cleanup joined. The original RecvError
branch is not reproduced; each distinct failure remains in its own archive.

Final correction is test-only. Actor records a suspended tick and refreshes contact
on its first resumed tick before ordinary election selection. The deterministic
control checks paused state, resumed follower state and subsequent normal election
expiry. The real snapshot case still requires every12000 original response,
exact12001 frontiers, actual snapshot installation and attribution OFF/ON.
Temporary logs are removed; bounded write failure cleanup remains. Removing the
cfg(test) field, initialization and guard reproduces the published Actor exactly.
No production timer, protocol, storage, outcome or performance result changes.

Linux runner92564 passes both controls, formatting and strict standalone Clippy.
Pi runner2212 ends0: snapshot5.60s, deterministic control0.01s, pinned formatting
and strict all-target Clippy pass. Mac runner93281 ends0: snapshot0.68s,
deterministic control, formatting/strict and exact source/mode guards pass.
Other63 tests are reused only through unchanged inputs and false pause flags;
no full-suite rerun is claimed. All1434 Pi baseline hashes/permissions are restored
after each completed diagnostic. Fresh performance builds/timing have not started.

Archives: [initial diagnosis](pi/README.md), [first causal trace](pi-trace/README.md),
[tick-only failure](pi-final-attempt/README.md),
[resume causal trace](pi-final-trace/README.md),
[checked Linux final source](resume/README.md),
[Pi final qualification](pi-resume/README.md),
[Mac final qualification](mac-resume/README.md). Earlier diagnostic source and
checks remain in main/trace/final/final-trace. P9 stays active and incomplete.
