# Standalone harness qualification

Linux scoped qualification: six tests pass; formatter and strict all-target
Clippy exit0. The updated root pre-push hook also exits0, including the separate
workspace checks; [raw hook output](linux/root-hook.log) is preserved. Default Cargo cache supports locked offline checks; no task-specific
Cargo-home override is needed. Independent lock SHA256:
`e046ca2e60f1da749c7dec66c2c5e0a8b978b10b53863d61ddf50f2b06e1684d`.
Exact source hashes/toolchain/platform are in [qualification.json](linux/qualification.json)
and [source.sha256](linux/source.sha256).

Shared dependency requirements follow the pinned upstream
[minimal manifest](https://github.com/databendlabs/openraft/blob/e88bb40707f2e511d8ecc44d31edcb1d814a11a8/benchmarks/minimal/Cargo.toml).
The independently locked resolution is Tokio1.53.2, futures0.3.34, serde1.0.229,
serde_json1.0.154; OpenRaft0.10.0-alpha.37 and bench-minimal are exact revision
`e88bb40707f2e511d8ecc44d31edcb1d814a11a8`. Production Cargo/lock are unchanged.

Controls cover equal rounding, atomic partial-prefix refusal, exact historical
no-op checkpoint restore, both-engine single/four-original results,12000-entry
actual checkpoints/compaction, and a paused follower's verified snapshot install
and full catch-up. See [test-final.log](linux/test-final.log),
[clippy-final.log](linux/clippy-final.log) and [fmt-final.log](linux/fmt-final.log).

Actual separate1-client/16-server runtime CLI controls run4096 clients/batch4,
16384 originals and1024 singleton warmups under primary default-retention.
Both engines return4096 completed requests,16384 original results and all three
frontiers17409. Raw [VoteBoat](linux/cli-voteboat-4096-b4.json) and
[OpenRaft](linux/cli-openraft-4096-b4.json) reports include small debug timings;
these are functional controls, not performance comparisons or achieved floors.
Primary OpenRaft payload1024/API4096/append4096 defaults remain intact.

Original compilation failed because the pinned metrics receiver requires
`WatchReceiver::borrow_watched`; a snapshot module also had an unused import.
Both causes were fixed. [Original output](linux/test-initial-failure.log) is kept.
Initial cache write failed under the filesystem sandbox; authorized public fetch
subsequently populated the normal cache. Earlier draft dependency pins were
replaced by upstream-compatible requirements before this final qualification;
older debug controls remain ignored diagnostic data under target/p9/matched-validation.

This does not qualify Mac, full20-million-operation cells, sustained memory/debt,
all fault cases, generic application histories or performance victory. Historical
VoteBoat image-frontier versus OpenRaft latest-image/retained-log differences and
engine/host scheduling remain explicit in [SCHEMA.md](../SCHEMA.md). Independent
release artifacts, complete primary curves, hardware/flags/lock identities and
policy review remain required. The report comparison gate stays pending.
