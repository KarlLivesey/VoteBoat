# Untouched upstream431 Linux calibration

Original exec81347 terminal0, all six cases succeeded. Pinned untouched400s upstream artifact30146a5688ea4c287f17dfa5c4926ea8212f0b2a11524bf1b2df24e236a0e7dd; source1357 hashes/compiler/fresh-build-profile proof passed before and after. No rebuild, retry or kernel change.

20M requested,19,988,480 completed originals plus1024 singleton warmups per case, one group/three voters,4096 clients/batch4, client1/server16, independent196-package lock, default runtime-stats/config/rate/delays unchanged. Order OFF/ON, ON/OFF, OFF/ON.

Latency OFF:3,734,557 /3,670,453 /3,578,395 originals/s; median3,670,453. Latency ON:3,535,192 /3,445,027 /3,515,230 originals/s; median3,515,230, median request p99 6.732ms (6.732/6.853/6.721). Median OFF is4.42% higher; do not infer this explains the whole published reference gap. OFF has no latency report. ON exact4,997,120 latency samples per run.

OFF rate derives from source-certified rounded completed count after successful joins/exit0 divided by original human elapsed seconds; ON is original JSON after successful joins. Timer after warmup/client spawning immediately before ready barrier through all client joins, before final stats/flatten/sort. No additional follower frontier verification; whole-process duration is separate. Calibration is separate from matched acceptance and does not reduce any P9 floor. Original raw stdout/stderr/commands/exits/UTC/PID/status preserved.
