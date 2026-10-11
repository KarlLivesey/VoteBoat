# Root corrected ownership controls305

Owned source: src/bin/support/operator_shutdown.rs and its new child
operator_shutdown/close_limit.rs. Original isolated304 source and
control-original.log remain unchanged. Production shutdown helper and docs are
owned by root; this worker changed neither.

The actual two-native-Service QUIC history now passes through Shutdown::advance
at the explicitly consistent intended10s+1ms cutoff. At close3505/cutoff13506
the helper pins the actual original node1->2 binding/generation10000 and
deadline39033, with all local/replica/snapshot/persistence/connector/ingress work
settled. It emits exactly shutdown waiting_for_peer_close_ms=25527. At39032
actual provider inspection retains the same original timer, helper advance
accepts it and its original binding/deadline/end pin is unchanged. Actual
provider polling39033 drains; advance accepts the actually drained service;
both real Nodes setup::join and socket rebind/directory cleanup succeed.

The existing TCP/QUIC accepted leadership-transfer histories now assert actual
local-work refusal at their original held owner step. Service is Quiescing,
the exact accepted transfer is still visible, and cancellation is admitted but
not yet applied. Shutdown::advance with an explicitly expired wall-start input
returns the exact original timeout error, protocol remains None and the original
transfer remains equal. The history then completes its original cancellation
and real native shutdown/join cleanup. A focused assertion helper keeps this
shared contract readable within the unchanged lint thresholds.

Exact selected results:

- executable_retains_actual_settled_future_quic_close:1/0 in0.04s, retained
  control-corrected-root.log.
- queued_operator_cancellation_drains_after_admission_closes_tcp:1/0 in0.02s,
  retained control-local-refusal-tcp.log.
- queued_operator_cancellation_drains_after_admission_closes_quic:1/0 in0.14s,
  retained control-local-refusal-quic.log.
- Final root formatting:zero diagnostics, control-corrected-fmt.log.
- Final strict all-target/all-feature Clippy:zero diagnostics,
  control-corrected-strict.log.

All runtime/check commands used this worker's target/divide-shutdown/build.
No extra full runtime target or stress variants were launched. Initial root
build attempts identified temporary missing contract registration and a helper
MonoTime import; root fixed the helper import before any actual test execution.

These controls prove and qualify the independently demonstrated executable
guard-policy defect; they do not diagnose original Mac302's unobserved provider.
Root owns final platform/frozen-image checks. Edits are now paused.

Final discovery-owner review correction: fixture settled() now explicitly
asserts the actual public connector_is_drained predicate as well as zero
ConnectUsage. The production helper requires that same actual provider drain;
resolver publication ownership cannot be inferred from zero request counts.
Only the actual QUIC exact control was rerun after this final predicate change;
it passes1/0. Final formatting and full all-target/all-feature strict Clippy
also pass with zero diagnostics. The corrected log files and owned-source
hashes above were refreshed on this final source. No further variants ran.
