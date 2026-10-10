# Slice250 — native fixture admission and actual quota-refusal evidence

The unchanged Mac native target reproduces175 passes/four failures with bounded
failure diagnostics. All four stop in initial snapshot catch-up. Third replicas
retain four snapshot jobs; failed/quarantined transport fields remain clear.
An owned one-second process sample shows six independent fixture setups doing
snapshot/WAL publication and synchronization concurrently. Original249 isolated
snapshot acceptance passes while the broad run fails. This supports correcting
test resource admission; it does not establish a kernel-level cause.

Eight independent100-group disk histories now hold one test-local fixture lease
through their original cleanup. Each still runs three concurrent nodes, shared
workers,100 groups and actual network/storage with its original internal fault
cuts. Deterministic tests retain default harness concurrency. No production lock,
provider, timer, synchronization, quota, persistent/wire format or group-count
change. Original10/15-second progress bounds remain; fixture admission waits
outside those bounds. A panic still abandons observation under original worker
contracts, so failed runs are not falsely described as joined.

The first admitted Mac target passes178/fails1, or364/1 across the ten targets.
All original initial catch-up failures pass. The remaining pressure history
assumes a blocked send must already be staged after its healthy-quorum read.
A bounded staged-send wait fails0/1 too, with empty queues and no transport error.
Unclassified input needs Bulk frame capacity; a pending replication request
waits for its correlated response. Pressure can block that incoming response
before a new data send is staged. Extra polling cannot create the assumed stage.
Both failed attempts and their exact source manifests are retained.

The fixture now injects a transparent BufferPool observer through the existing
NativeSharedTransportFactory seam. It delegates native buffers, binding, quotas,
classes, usage and close. A scoped shared counter records actual Bulk Overloaded
results only on node1->node3 views. Its baseline is sampled after the deliberate
fixture acquire/refusal and before reconnect, excluding that artificial refusal.
The later increase therefore witnesses native transport pressure. The test still
requires peer3's unchanged data while full quota is held, real durable writes/
reads through peer2, quota ceilings, catch-up after release, cold original-operation
retries, joined workers, zero returned credits and usable surviving host views.
No weaker success/authority outcome substitutes for those original contracts.

Final source-matched acceptance:

| Checks | Linux | Mac |
| --- | --- | --- |
| Ten selected all-feature core/provider targets |365 passed|365 passed|
| Core-only effect_owner |171 passed|171 passed|
| Native-only effect_owner, without TLS |174 passed|174 passed|
| Exact pressure/reconnect/cold-retry history |Included in179|1 passed, then included in179|
| Formatting / default/all/core/native strict Clippy |Zero diagnostics|Zero diagnostics|

Feature configurations execute sequentially per host. Mac arm64/macOS27.0 uses
installed stable1.98.0 and a child-shell4096 descriptor limit. All647 build inputs
verify against the final patch on both hosts. This includes the changed fixture
concurrency policy, not the old six-cluster simultaneous disk load. No performance
gain, P7 latency gate, arbitrary failure proof or complete platform certificate
is inferred. Broader251 lifecycle/operator validation and252 application-provider
obligation review follow; full P0–P7 remains active, with tuning/security later.

Initial diagnostics referenced a nonexistent trait name; the retained compiler
failure is followed by the focused SnapshotWorker correction and zero checks.
Raw logs/patches/sample remain under target paths with exact digests in raw.sha256.
Published artifacts normalize task paths/trailing whitespace; sample-excerpt is
explicitly partial. Failed Mac stores remain in the task-owned remote test root.
