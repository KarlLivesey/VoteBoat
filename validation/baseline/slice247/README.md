# Slice247 — establish serving authority before a deliberate quorum cut

The original loss fixtures selected a volatile Leader role immediately after
member recovery, then killed the other voters. Administration separately requires
commitment in the selected leader's current term. Retained245 failures show
term2/committed2/durable_last3 and no accepted original record; the service remains
in preparing until observation cancellation. This is consistent with cutting
quorum before the new-term no-op commits, rather than losing an accepted record.

On the current source, isolated original Mac TCP/QUIC histories pass2/0. The
unchanged broad196-test run reproduces both21101 loss failures (191/5 overall),
with term2/committed2 and phase=preparing. Other failures are both interrupted
drain runners and an original QUIC write's LeadershipChanged uncertainty. Keep
that finite schedule separately from the isolated result and earlier245 evidence.

The focused test-only helper completes an actual authenticated linearizable read
of value42 on the exact selected endpoint before removing its voters. It uses
the existing read refusal classifier and a10-second settlement deadline; the
existing role-discovery and individual command bounds remain explicit. It does
not turn a role/status hint into authority, add an application write, guarantee
future leadership, or retry an uncertain configuration as if it had succeeded.
Original21101/17920 records, quorum loss, local durable-versus-committed status,
conflict refusal, retry identity, cold recovery/data/dedup and cleanup assertions
remain. No production/API, persistence, wire, quorum or durability change.

Linux all-feature197 passes. The first default run is137/1: the Linux-only port
test fails immediate rebind after dropping placeholders. It lacked the existing
process-spawn gate around parent-owned descriptors. The small release/rebind check
now uses that gate before PORT_BLOCKS, retaining exact port-range/exclusive-bind/
rebind assertions and original concurrency. AddrInUse alone does not prove the
kernel cause or identify an external owner. Final Linux all197/default138 pass;
formatting/four strict profiles are zero. The actual current-term/read-ticket Raft
contract also passes1/0. Failed logs stay recorded.

Mac with the two-file read gate finishes all194/2. Both21101 loss histories and
17920 retained-record history pass. Remaining failures are the interrupted QUIC
runner's unchanged128-request budget and TCP shared-group cold-start AddrInUse.
The former original uncertain QUIC write passes this run; that is a finite result,
not proof of every schedule. Default137/0 and formatting/four strict profiles
pass after the all-feature suite finishes, preserving sequential executable builds.

The Mac uses installed Rust1.98.0, arm64 macOS27.0, the original12-test concurrency
and task-owned temporary checkout/failed roots. All646 tracked source, fixtures
and build inputs verify before the changed-source run. The Linux-only guard is
excluded on Mac; its separate patch and final published hashes keep that difference
explicit. No host installation, descriptor limit or global setting changed.
Published logs/patches normalize task paths and trailing whitespace; original
raw logs and applied patches remain under target/slice247 paths with recorded
digests. Source manifests retain exact file bytes; the published patch reverse
check confirms its complete changed-source scope.

Inventory108 and conformance metadata9 partial reviews/68 operations are metadata
checks, not complete provider acceptance. Current248 diagnoses original bounded
runner progress;249 reconciles required functional/operator exits and the separate
restart ownership failure. Full P0–P7 remains active, features/Mac/Linux first,
performance/security next; Windows/P8 deferred.
