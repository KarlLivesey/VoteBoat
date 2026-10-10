# Slice245 — native election profiles and drained-follower routing

Counter, directory and transfer serve accept --timing-profile throughput|edge|legacy.
Throughput is the explicit durable-service default: heartbeat100ms/elections
[1000,2000)ms. Edge uses250/[1500,3000)ms; legacy retains50/[150,300)ms. Public
NativeTimingProfile produces ordinary TimerConfig; low-level Rust defaults and
host timer injection remain unchanged. Selected timers reach static/member/multi/
discovery/rotation startup; rotated static/member open now accepts explicit timers.
Unknown/duplicate CLI selection is refused before resource ownership. No hot policy
selection, stored/wire migration, quorum/barrier or caller-deadline change.

Two profile-only Linux/Mac original-configuration histories finalize but then stop
on exact ERR Draining from the old follower. Node::propose returns the owned request
before any admission ticket/append. Automatic routing now advances past that exact
known refusal for complete data writes, preserving all original command bytes and
absolute deadline. Unknown, malformed, authorization and unrelated failures still
stop. Exact Draining moves from the old negative socket test into the positive
original-command history; modified Draining remains terminal. This is an admission
contract correction, not replay of an uncertain write.

Linux final: all-feature counter197/default138, startup47, service-profile refusal1,
counter-binary44 and profile contracts2 pass. Directory22/transfer33 passed in the
preceding profile-only run; their production code is unchanged by the counter-only
routing fix and test corrections. Actual Rust embedding creates7, cold-retries
original duplicate7 and applies fresh10; all workers join. Formatting/four strict
profiles are zero. Inventory108 and conformance metadata9 partial reviews/68
operations remain finite metadata evidence.

Mac initial profile-only: counter189/7, directory22/0, transfer33/0, startup34/13 and
CLI refusal1/0 (passed/failed). Its original counter failures include both exact
Draining refusals, preparing/configuration observations and runner request budgets.
Startup failures include the fixed10000ms cleanup clock moving backwards, maintenance
uncertainty and election identity assumptions. One new test incorrectly expected the
overflow-specific timers stage; invalid TimerConfig actually returns native startup/
InvalidLimits. Initial compile/lint/error histories are retained and fixed without
suppression. First Mac strict run exposes the single-element protocol-loop lint
without QUIC; final source uses the existing configured-array pattern.

Mac post-routing full counter finishes191/5; binary44 and refusal1 pass, startup34/13
still fails. These are finite runs, not statistical performance attribution or
whole-platform acceptance. Default counter finishes135/2. After the test-only loop correction, all18
published source hashes verify, selected profile contracts2 pass, formatting/four
strict profiles are zero and actual embedding7/duplicate7/fresh10 joins workers.
No broad counter/startup pass is inferred from those selected successes.

Mac arm64/macOS27.0 uses installed stable Rust1.98.0 (CI pins1.98.1), original default
libtest concurrency and unchanged host settings. Source base26484eb plus exact
profile/follow-up/test-loop patches and verified hashes bind each execution. The
pre-edit instrumented journal run uses existing1000–2000ms timers,64 warmup plus8
operations, recovers72/verifies original retries and joins workers. It diagnoses
that selected schedule; it does not prove the cause of every broad failure or a
performance target. Original failed stores remain in the owned task roots.

Next: preserve caller-owned monotonic time in native cleanup and diagnose actual
status-wait/preparation and runner admission gaps before required operator integration.
Full P0–P7 remains active; functional features/macOS/Linux first, performance/security
later; Windows/P8 deferred.
