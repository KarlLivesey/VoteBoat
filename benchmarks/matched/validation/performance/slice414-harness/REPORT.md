# Linux startup-limit harness qualification

Seven standalone tests passed. They include all original results in both engines
and both profiles, controlled wide-ready VoteBoat admission with 4,096 exact
receipts, Common64 limits, 12,000-entry real maintenance and a lagging follower
recovering through verified snapshot installation. Standalone formatting and
strict all-target Clippy exited zero. Production repository qualification remains
separate and is owned by the root task.

Four actual one-client-worker/16-server-worker CLI controls passed, for each
engine and each envelope profile. Every control completed 16,384 originals in
4,096 four-entry requests, plus 1,024 singleton warmups, with all three applied
frontiers at 17,409. Primary effective capacities were 4096/4096/1024 for API,
append and replication; Common64 reported 64/64/64. OpenRaft defaults were not
reduced in the primary profile.

VoteBoat's observed maximum original proposal width was 4,096 in the primary
control and 64 in Common64. Each histogram accounts for exactly 17,408 original
commands including warmup. Followers admitted no local proposals and reported
empty histograms. The report distinguishes original-entry widths from the fixed
four-entry client requests.

Raw compiler/check logs, CLI outputs and exact qualification metadata are under
`linux`. These are functional debug controls; their rates establish no performance
claim. The independent frozen baseline artifacts remain unchanged. Linux asserted
release and short paired results are recorded in PAIRED-LINUX.md. New macOS
qualification and full capacity qualification remain pending.

Final fixture qualification passes eight Linux tests, formatting and strict
all-target Clippy on immutable306b26b production plus the recorded test-only
overlay. The three-voter test verifies exact original receipts/frontiers and an
upper capacity bound. A separate fully preloaded single-voter actor/Core/store
control guarantees exact4096/64 original widths. The initial Mac fixture and its
pause-ack-only revision both failed the scheduler-dependent minimum-width
assertion; raw failures remain under `mac-initial-failure` and `mac-ack-failure`.
The final fixture waits for the leader's observed pause before live queue preload.
No release behavior or measured executable was changed by the test-only fix.
Final raw Linux checks and precise overlay/source hashes are in
`fixture-final-linux`. New Mac qualification remains pending.
