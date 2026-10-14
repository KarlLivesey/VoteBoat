# Upstream calibration431

Purpose: answer local reference calibration using existing untouched pinned400s artifacts. Separate from matched acceptance; P9 floors/full objective unchanged. No source edits or new benchmark harness.

Shape: one process/group, three voters, empty payload, volatile stores, in-process RPC, client runtime1/server runtime16,4096 clients, batch4,20M requested originals rounded19,988,480;1024 singleton warmups. Three rotating OFF/ON, ON/OFF, OFF/ON trials. Only ON adds --latency. OFF is published driver default. Retain default runtime-stats, config{}, rate0, all delays0 and independent196-package lock.

Ownership/transitions: upstream per-original completion channels/outcomes unchanged. Timer starts after warmups/client spawning immediately before the ready barrier and ends after all client joins, before final stats and latency flatten/sort. OFF human elapsed duration/integer ns/op/op/ms; ON JSON elapsed/rate plus4,997,120 request latency samples. Process time separate. No extra follower verification/shutdown claim.

Failure/cleanup: unchanged400s timeout. Exact commands/stdout/stderr/UTC/exits retained; EXIT trap STATUS/DONE; stop on failure, no retry. Original handle/status-only polling during quiet. No builds/checks/copies/archives/other benchmarks on same host; no kernel/security/browser/image actions.

Acceptance: root explicit per-host QUIET_START before benchmark. Pre/post outside timing require1357 exact source hashes/pin, original400s source, independent lock, original/copy executable SHA and current within-host rustc identity. Existing compilerJSON fresh:false for OpenRaft/bench_minimal/exe; opt3/debug2/assertions/overflow plus verbose thinLTO/CGU1. Six terminal successes. OFF count is source-certified rounded count conditional on successful joins, not invented frontier verification; ON exact count/samples/config fields. OFF has no reported latency. No sustained acceptance or whole-gap causal inference.
