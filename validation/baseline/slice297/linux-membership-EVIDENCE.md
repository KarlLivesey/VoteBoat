# Membership recovery divide-and-conquer result

Changed only tests/member_recovery/older_checkpoint.rs (native-gated registration) and new older_checkpoint/election_rollback.rs. No production or shared documentation changes. Source hash guard in source.sha256 / source-final.log.

Two new actual-core histories combine the old learner checkpoint2, committed op901 and Joint4, accepted uncommitted Final5, public higher-term Append rollback of only5, native reopen and elections. The model history executes139 schedules over the exact135-byte native replacement frame:136 append cuts0..135, failed sync, publication-before and publication-after. Every failure fences the original core without an acknowledgement. Native bytes reconstruct Final for unsynced cases or Joint for publication faults; no fixture policy mutation is used for the transition under test. Verified application replay restores value10 and duplicate original op900/901 outcomes. Separate real FileLogIo/FileSnapshotIo history persists rollback, reclaims the WAL, opens both providers and elects after verified checkpoint recovery; this is a file reopen test, not a physical power-failure claim.

Each election uses actual independent peer cores and native WAL-backed ballot persistence. Surviving Final elects self4+1; surviving Joint remains Candidate with that new-only majority, rejects a stale Final-scoped ballot, and requires actual peer2's ballot. BallotOrigin matches the surviving policy. Volatile Candidate/Leader roles and pending accepted membership are never used as durability receipts: pending states retain previous durable vote/index/membership, block additional events with Busy, and release persistence-dependent output only from exact public persist_effect completion.

Final Linux target results, all using CARGO_TARGET_DIR=target/divide-membership/build sequentially:
- all-feature complete member_recovery:44/0 (member-all-final.log)
- default complete member_recovery:43/0 (member-default-final.log)
- native-only complete member_recovery:42/0 (member-native-final.log)
- core-only complete member_recovery:12/0 (member-core-final.log)
- focused strict all-feature Clippy for member_recovery:0 diagnostics (clippy-member-final.log)
- direct formatting check of owned registration/module:0 (format-member-final.log)
- owned source guard:2/2 exact (source-final.log)

Full repository strict Clippy was attempted twice, but the concurrently changing shared worktree was not clean at either observation: first shutdown test cognitive complexity28/25, then parent reference module registration before its persistence.rs existed. The first repository format check passed; final repository format attempt was also interrupted by a parent module registered before host/persistence.rs existed. Logs retained as clippy-first.log, clippy-final.log and format-final.log. These are not clean repository acceptance. Parent owns final integrated strict/fmt and macOS qualification.

Preflight failures are retained in focused-first through focused-fifth.log: incorrect test Finalize payload type; incorrect assumptions about pending accepted membership and volatile Candidate/Leader timing; and repeat sandbox PID fixture collision. They were fixed by matching existing contracts and a unique test root. No lint waiver or production semantic change.

This closes one bounded P4/chapter11 scenario6 composition, not all arbitrary membership/lifecycle/storage faults or complete P4.
