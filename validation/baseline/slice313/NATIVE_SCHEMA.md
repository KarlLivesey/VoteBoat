#313 native/downstream acceptance mini schema

Implement only tests/transaction_driver.rs/** and optional
examples/embedded_transfer.rs/**. Production driver is membership_recovery's
generic actual-Node implementation; root owns docs/registry/platform freeze.
Use only target/divide-shutdown/build and sequential feature configurations.
No live302 source/cache, Git, production, Cargo or shared-doc edits.

Data/assembly: three physical native replicas, each NativeMultiStartup hosts one
coordinator and two canonical participant GroupIdentity incarnations. Exact same
TransferProfile and lifetimeN, fixed left/right account IDs/opening balances,
real TCP/TLS or QUIC, original fixture credentials, shared per-replica WAL and
per-group real checkpoints. Immutable specs carry ten stable destination IDs.
Reuse existing startup/multi construction/drain pattern; no memory message oracle.

Ownership/transitions: public driver borrows real Nodes; actual output consumption
supplies authority. Fixture local app.status is only an observation PAUSE trigger
or diagnostic, never passed to driver as prepared/decision proof. After pause,
stop_waiting cancels observations, actual driver/Node credits drain, drop borrow;
host performs fresh public quorum reads/admin/checkpoint/actual owner replacement,
then new driver.resume obtains its own real quorum status. NativeNode shutdown and
parts reclaim join real connector/WAL/snapshot workers before files removal/reopen.

Checks: actual commit, reversed debit/canonical order, durable refusal plus absent
Abort, Locked quorum account read, original retry/resume, N capacity terminal and
N+1 refusal, canceled/lost observations and output credits. Bounded recovery history
uses actual one-prepare/Decided/one-apply/AppliedAll/release pauses, old checkpoint
plus committed suffix, cold reopen and leader replacement, without supplied flags.
Compare persisted original operations/first positions/assertions from actual
fresh reads across restart. Stale actual DriverCompletion must not release a fresh
driver's credits. Nonempty Node-stream constructor refusal uses real admitted
opaque ticket and cleans original Node outputs.

API dependency: coordinate concrete public driver methods/outcomes/budgets with
membership_recovery before compile. Private wrong-Node/ticket/content matching
needs actual opaque Node completions and belongs to the production agent's private
unit/native controls; integration tests cannot obtain private retained attempt
state. Do not add a public forged-observation seam merely to test it, or claim
forged ticket scalars/receipts prove actual provenance. Root/provider reviewer
retain this boundary explicitly.

Failure/cleanup: timeout only abandons observation; it never Abort/unlocks. Preserve
durable prepared/decision records and files for resume. Test progress deadlines
remain bounded; no broad stress retries or provider timer changes. A causal failure
identifies exact source/phase/ticket/status before any fix request. Relevant targets,
formatting and strict zero-diagnostic checks complete on stable owned files; root
then qualifies one frozen source on Linux/macOS.

Focused embedding cleanup supplement: a driver/Node failure may leave Node in
RecoveryRequired, where ordinary poll/control are closed. Preserve the first run
failure while handing actual selected providers to Node::into_recovery. Do not
create a checkpoint from failed application/core state. Drain a nonfailed closed
PeerDriver via drain(outbound, now, budget), reclaim its actual connector and join
its dialer; poll terminal persistence/reclaim/snapshot worker observations without
applying them to a failed core, then reclaim/join actual stores. A failed peer
provider cannot be advertised as drained: retain its original recovery scope and
report cleanup refusal. A controlled example test uses actual accepted Start then
explicit Node abort, verifies RecoveryRequired, executes this path, cold reopens
and joins the same native files. Normal close/checkpoint remains unchanged.

Quorum-unavailable acceptance supplement: first obtain real Prepared/Locked
quorum state and settle all original replica copies. Retain two selected live
native Node owners unpolled while the driver borrows only the original
coordinator leader, which still hosts all3 configured groups. Drive real sockets,
owner timers and original status reads for a bounded2sec; absence of new majority
responses must yield no business decision. Stop only waiting, consume actual
Unknown and drain its read ownership. Surviving local status is diagnostic, not
phase authority. Restore the original owners and polling, obtain fresh quorum
state proving the original first-indexed phases and locks unchanged, then resume
by ID through actual consumed status and complete Commit. Finally join every
original native worker/socket. This is unavailable quorum processing, not an
assertion of process crash, packet-loss coverage or a synthetic Raft engine.
