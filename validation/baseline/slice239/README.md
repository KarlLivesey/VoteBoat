# Slice239 — bind the original handoff before admission

Base01a19eff02efd8338148996d84b09b566c7fe095. The move-leader request now carries
OP CONFIG SOURCE SOURCE_STORE SOURCE_INC TARGET TARGET_STORE TARGET_INC. Source
is an immutable original voter identity; it need not lead at first admission.
Exact current stable membership and receiving leader authority still gate Begin.
A target already leading can commit Begin and Complete in one term; completion
still requires that target's current-term committed prefix and applied original
intent. Pending/applied conflicting bindings fail before proposal. Data/admin IDs
remain separate. Native Raft quorum/transfer rules and file field layout remain.
The expanded same-term completion grammar and CLI deliberately change the existing
development interface; older records remain valid, mixed/downgrade use is unsupported.

The runner first quorum-reads its original operation. Only exact quorum absence
allows a new bound Begin from the plan row; existing original source identity is
preserved even when it differs from the physical drain owner. Historical Completed
permits the bound configure path, which checks actual authority. Source stop still
requires Final/applied membership and owned-work quiescence. No new runtime/provider.

Before: two native Rust histories fail InvalidCommand. Initial compilation reveals
the missing typed ApplicationError::OperationConflict variant; it is added at the
existing application seam. Intermediate differing-origin runner histories fail2
with OperationConflict, then pass after original-record observation replaces source
reconstruction. Final maintenance9/startup45/unit43/drain44/leadership22 pass;
default maintenance9/startup31/unit41/drain31/leadership15 and core-only7 pass.
Both native transports cover target election before Begin, unread replies, unchanged
source/store/incarnation, changed-binding refusal, data retries and cold WAL/checkpoint
reopen. Modeled native file-I/O accepted-suffix rollback exercises NativeLogStore;
it is not a real network rollback or power-failure proof. Same-term native frame-cut
checks permit only the old or complete state. Services/workers close and join.

The earlier broad190/0 precedes final runner composition. Final broad189/1 fails
peer_discovery::executable_consumes_peer_discovery_and_recovers_tcp with node2
filesystem/listener AddrInUse; preserve this failure for the next allocation/
reservation investigation rather than rerunning unchanged tests for green.
Formatting/four strict Clippy profiles are zero. Inventory108 and conformance
metadata pass, retaining9 partial reviews/68 operations; no whole-provider claim.

Preceding-source238 run38069931677: Ubuntu185/3 fails three configure19770 uncertainty
histories; macOS187/1 fails status-wait96301
admission. Later targets are unrun. Minimal excerpts are retained with the exact
source/job identifiers; they do not certify239 macOS. Functional Linux/macOS features
remain first, performance/security later, Windows/P8 deferred. The full goal remains
active. commands.txt, provenance.json and source.sha256 bind the finite evidence.
