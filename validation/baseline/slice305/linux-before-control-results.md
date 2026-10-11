# Isolated executable close-limit control304

The authoritative schema is recorded at the end of root docs/IMPLEMENTATION.md.
Only this isolated copy of original302 was edited. Source comparison against
target/slice302/repo finds precisely two changed/added paths:
src/bin/support/operator_shutdown.rs (child registration), and
src/bin/support/operator_shutdown/close_limit.rs (actual native control).
src/bin/counter_service.rs and its original advance_shutdown are unchanged.

The control constructs two real NativeStartup Service/Nodes, actual file/WAL and
snapshot workers, pinned native QUIC credentials and real UDP sockets. A bounded
UDP relay holds the original encrypted first packet until the accept-side Node
first processes it at supplied MonoTime3500. Election timings are explicitly
test-only to avoid unrelated election traffic. Original provider handshake/close
timers, poll budgets, executable ten-second guard and actual is_drained are
unchanged. No roster binding, accepted work or completion is forged.

Final observed values: close begins at MonoTime3505. Both peers initially retain
their original authenticated close timers: node1->2 deadline39033,
node2->1 deadline3593, generation10000. At intended close+10001 (MonoTime13506),
node2 is actually Drained; node1 remains Draining only because its original
timer39033 is still future. Explicit assertions prove client/read/result/owner/
persistence/outbound, replica, checkpoint, snapshot-router/worker, connector,
peer-driver and ingress work settled, with no attempt, accepted send or frame.
Actual peer/session inspection is Closing/flushed/protocol_not_drained,
authorized/closed/not_fenced/not_retiring with exact original binding/deadline.

The executable guard receives explicit started=wall_now-10001ms while the actual
provider has been polled at close+10001ms: both controller inputs represent the
same intended interval, without a wall-clock wait. Original advance_shutdown
returns its exact timeout error despite owning only that settled future timer.
Its observation does not change original retained binding/deadline. At39032
(deadline-minus-one), actual Node remains Draining with the same timer; actual
Node.poll39033 drains. setup::join succeeds for both Nodes and UDP listener
addresses can be rebound; fixture directory is removed.

This independently proves an executable guard-policy defect for valid native
protocol ownership. It does not identify the original unobserved Mac302 cause.
The original302 failure and silent303 probes remain separate evidence.

Final exact unit filter: original_executable_limit_refuses_actual_settled_future_quic_close
passes1/0 in0.03s; retained output is control-original.log.
Isolated cargo +stable fmt --all -- --check passes with no diagnostics.
Isolated cargo +stable clippy --locked --offline --keep-going --all-targets
--all-features -- -D warnings passes with zero diagnostics in5.54s; retained
output is control-strict.log. All used only target/divide-shutdown/build, not the
matrix or other-agent cache. control-owned-source.sha256 records final hashes.

Preflight failures: missing OutboundQueue trait import prevented initial compile;
the import was added without changing the assertions. Restricted socket binding
then returned PermissionDenied; the exact native control was rerun with approved
local socket permission. The first actual ownership control passed1/0; a focused
strengthening added original-close pin equality, zero usage and deadline-minus-
one assertions, and final captured control passed. No stress variants or broad
runtime test target were run. Edits are paused for parent integration.
