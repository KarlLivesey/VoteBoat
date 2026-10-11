# Slice 269: corrected composed lifecycle qualification

Linux is terminal: the all-feature native delegation family passes 69 tests,
zero failures, in 2900.43 seconds. All 662 build inputs verify before and after
the run and match the published slice268 source, revision 178844a. The original
operation IDs and WAL/checkpoint phase cuts are retained.

Run:

```sh
cargo +stable test --locked --offline --all-features --test routed native::delegation:: -- --nocapture
```

[Linux evidence](linux/linux-family.log) includes the terminal result; its exit
record is zero. The source manifest binds this older image independently of the
current main branch. Raw and normalized artifact hashes are retained in linux/.

The matching Mac run is terminal:62 passed/7 failed, exit101, in11334.05s.
Its662-input start/terminal guards verify and match Linux's original manifest.
Failures are imported-partial checkpoint equality, endpoint-write duplicate,
three imported-retirement progress assertions, repeated-move duplicate and
retained-parent duplicate. Corrections273/274/285 and diagnostics275 retain
their separate sources; the current corrected family285 is still running.
This is Linux-only qualification of this older image, not completed two-platform
lifecycle acceptance. [Mac terminal evidence](mac/README.md). Older Mac262
and [terminal Mac251](../slice251/mac/README.md) retain their own source images
and failure obligations; their
failures must not be attributed to this corrected image. Later codec fixes270/271
and timer replay272 have separate both-host acceptance and are not retroactively
covered by this run. Full P0–P7 and broader combined-fault qualification stay open.
