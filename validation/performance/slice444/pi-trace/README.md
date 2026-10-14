# Pi444 causal trace diagnostic

The subsequent approved cumulative trace amendment (patch77e53ad1) ran once,
original20173 naturally terminated101 after a12.11s focused control (63filtered).
It records pausedslot2 Followerterm1/applied1 campaigning to Candidate2 while
frontiers were[11293,11293,1]. Slot0 immediately changed Leader1→Follower2 with
four pending originals. Repeated campaigns reachedterm6 without frontier progress.
The write[11293,11294,11295,11296] reached the test-only10s diagnostic guard;
cleanup returnedOk. NoActorErr was observed. This establishes the paused-fixture
campaign disruption for the traced stall; it does not reproduce or explain the
original fullsuite's specificRecvError path. No production protocol/timer/limit
changed. All1,434 baseline hashes/permissions were restored(exit0) afterward.

SHA256SUMS covers all regular files recursively except itself; no binaries, build trees, full source archive, caches, or WAL are included. Initial setup/interrupted diagnostic and original63-pass failure remain in `../pi/`.
