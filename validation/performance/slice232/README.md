# Rejected bounded manifest-journal trial

Base: `9ee9eeb61311bc1746e32bea6482ad0f32a64121`. Exact measured local changes
are in `candidate.patch` and `candidate/`, bound by the SHA-256 manifests.
Production source, tests and inventories are restored to that base. The trial
constructor and metadata format are not available in the accepted implementation.

The candidate appends bounded checksummed manifest records after full WAL sync,
then performs full manifest-file sync. Initialization, compaction and generation
replacement retain full file sync, atomic publication and directory sync. Native
tests cover44 new primitive interruption cuts plus41 legacy cuts,106 exact tail
length cases, complete corruption/regression rejection and bounded metadata.
These are process/primitive interruptions, not hardware power-loss tests.

Final file tests8; timings3/reclaim8/log-store9/new public1/provider27/Raft41/
shared-barrier8 pass. Native-only timings3/reclaim8/new public1/provider27 pass.
Full Linux TCP/QUIC service suites pass counter181/directory22/transfer28.
Candidate and restored formatting/four strict profiles finish zero. Initial
compile, eager-probe, filename-assumption and Clippy failures are preserved;
`file-final.log`, `storage-core-all.log`, `storage-native.log`, `services-all.log`
and `strict-candidate.log` contain the final candidate results.

The original uninstrumented serial TCP run completes256 measured receipts after
64 warmup operations: recovery value320, exact retries and worker joins pass.
Observed10.423 applied operations/s and461.764ms p99 fail the fixed250ms gate.
Raw receipt validation passes; process exit0 and gate exit1 are distinct.
`reference.context.json`, raw CSV, summary, logs and `provenance.json` bind the
actual source, executable, workload, host and outcome. Three logical replicas
share one Linux desktop and device; no CPU/device isolation or causal comparison
is claimed. No candidate macOS execution or sustainable-capacity result.

Source231 operator run38064123576 passes Ubuntu181/22/28; macOS counter181
passes, directory21/1 fails recursive-route observation-floor lookup with an
interrupted/refused authority manifest upgrade. Transfer is unrun. Original logs
and run/job/source identity are retained separately from candidate validation.

At the user's request, subsequent work prioritizes feature completion and
functional Linux/macOS recovery. Performance tuning and security review follow.
The original performance gate is retained for that later milestone.

Published logs omit local machine identifiers and unnecessary environment details;
see `PUBLICATION.md`. Exact candidate sources, patch, synthetic receipt CSV and
measurement hashes are unchanged. Original unredacted logs remain local and ignored.
