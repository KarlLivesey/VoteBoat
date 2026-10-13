# Primary two-cell diagnostic (held until root grant)

Frozen230e037 unified harness and untouchede88bb4 minimal reference. Use already
compiled independent Linux/Mac asserted releases only:opt3/debugassertionstrue,
overflowtrue,thinLTO/codegen1. Verify source/lock/compiler/artifact guards before
and after. No Cargo, tests, fetches or source copies during timing.

Cells:c1/b1 andc4096/b4,131072 requested originals,1024 singleton warmups,3 voters,
client1/server16 workers, empty business payloads and zero simulated delays.
Primary upstream defaults stay intact:payload1024/API4096/append4096,
snapshot5000/keep1000/purge1024. Unified VoteBoat retains its own64 cap. Do not
substitute Common64 or RetainAll. Three interleaved trials with alternating order
of unifiedVoteBoat/unifiedOpenRaft/untouchedupstream; each process recreates its
cluster and warms independently. This is short attribution, not a floor win.

Validation: preserve every command, exit, stdout/stderr, wall time and artifact
hash. Unified reports must show131072 completed originals,131072/32768 requests,
all three finalfrontiers132097 and matching1/16/3 workers/members. VoteBoat must
show realcheckpoint_count>0 on every replica and bounded retained suffix within
keep1000 + pending5000 + one64-entry trigger overshoot after the run, without resets; record actual peakretention/queues/install
counts. UnifiedOpenRaft verifies all original responses/finalfrontiers but its
minimal private provider does not expose retention/checkpoint gauges. Untouched
reference must return exit0, operations131072, samples131072/32768, correct
1/16 workers,3 members,1024 warmups, zero delay/rate and empty raft_config
(overridesabsent); its driver checks each API result but exposes no finalfollower
frontiers or retained/checkpoint gauges. Mark those unavailable; do not patch an
"untouched" driver or infer invented maintenance counts. Semantic/configuration
proof plus separate unified strict frontier checks must remain distinct.

Failure invalidates that case but raw output/exit remains and the remaining
cases can run, with aggregate nonzero status. Report no claimed superiority or
sustainablecapacity from these131072-op diagnostics. Root reviews short results
before permitting full20M required curves or source changes. Mac pilot remains
held while Macnative/configured work or archive transfer remains active.
