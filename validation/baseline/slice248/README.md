# Slice248 — pace drain observations within the existing budgets

The original interrupted QUIC runner exhausts128 exchanges at7764ms despite its
45-second deadline. The actual native trace repeats follower/candidate discovery,
then the same accepted Joint at index7 while the committed prefix stays6. The
source stays unready. Original TCP passes; QUIC fails1/1 in that isolated run.
The earlier broad245/247 failures remain separate, source-bound evidence.

The foreground runner now measures the completed observation round's exchange
cost and spaces remaining rounds within its existing absolute time/request
budget, reserving a final stop exchange. It subtracts round time already spent
and handles spurious park wakes. An actual ready source skips the pause. All
original source/sequence/operation/plan/configuration/handoff checks still run;
no role hint grants authority, cached status skips a phase, or unknown becomes
success. Per-attempt5s, single45s/128 and multi120s/4096 bounds remain unchanged.
No Raft timer, policy, quorum, persistent/wire format, synchronization or public
provider/API changes. Temporary512-character exchange tracing is removed.

An authenticated delayed-ready fixture checks exact commands, budgets, actual
source readiness before stop and joined channel ownership. The old25ms cadence
compatibility stub fails0/1 at925ms, exhausting16 requests before the source is
ready at2s; the unchanged4s deadline is not extended. Final positive and unready
request-exhaustion tests pass. Existing actual stalled-observation tests retain
absolute deadline coverage. An initial200ms negative fixture wrongly assumed its
authenticated channel must complete before cancellation and panicked on Truncated;
its log is retained. An earlier600ms readiness fixture passed even with old pacing
because handshake cost supplied enough delay; it is calibration, not regression
evidence. No production deadline was changed to make either fixture pass.

Final Linux passes all197/default138 counter histories and46/44 executable contract
tests. Mac passes all196/default137 counter histories and46 all-feature executable
contracts, including both original killed-runner/source recoveries. Original data,
deduplication, journal/profile bytes and worker joins remain checked. Formatting
and all four strict Clippy configurations pass on both hosts. Mac arm64/macOS27.0
uses installed Rust1.98.0 with original12-test concurrency; all647 final source,
fixtures and build inputs verify before execution. Each host's feature builds
run sequentially; no host settings or descriptor limits change.

The separate247 TCP shared-group startup AddrInUse does not recur in this run.
Its failed log/store remains; successful recovery here does not prove its kernel
cause or a listener-specific fix. These are finite functional histories, not
statistical liveness/performance acceptance or the whole P0–P7 roadmap.
Inventory108 and metadata9 partial reviews/68 operations remain limited metadata.
Current249 audits original functional/operator requirements;250 implements a
confirmed gap;251 checks its supported baseline assembly. Full goal stays active,
features/Mac/Linux first, performance/security later; Windows/P8 deferred.

Original raw logs/applied patches remain under target/slice248 paths with recorded
digests. Published artifacts normalize task paths and trailing whitespace; source
manifests keep exact file bytes and the source patch reverse-checks the final tree.
