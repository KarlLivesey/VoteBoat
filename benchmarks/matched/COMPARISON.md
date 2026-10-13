# Like-for-like comparison contract

The shared-driver comparison controls external conditions. The untouched pinned
OpenRaft driver remains the reference for its published curve. Neither is an
acceptance win until sustained results and the policy/unit audit pass.

| Condition | Required comparison |
| --- | --- |
| Hardware | Both engines run sequentially on the same Linux host or M2 Max; no builds/copies during timing |
| Build | Same pinned compiler, dependency lock and asserted opt3/thin-LTO/codegen1 profile; genuine root-library/executable identities |
| Workload | Same zero-payload no-op, one group, three voters, clients, entries/request, rounded total and1024 singleton warmups |
| Driver | Shared closed-loop original IDs and timer/latency collection, one client worker and16 server workers |
| Success | Every original applied after quorum commit; full batch consumed; all three final frontiers checked |
| Durability/transport | Both framework providers volatile and RPC in-process; real disk-backed TLS/TCP/QUIC results separate |
| Maintenance | Same declared5000/1000/1024 cadence/retention/purge policy; actual snapshot representation differs below |
| Observation | Attribution off for capacity comparison; on/off diagnostics separate; all trials and regressions retained |

## Capacity units and qualified correction

The legacy JSON name `api_entry_batch_capacity` reports a configuration value,
not equal units. Pinned OpenRaft `BatchRaftMsgReceiver` counts incoming messages
and extends every message's complete payload batch. Its4096-message capacity can
therefore collect up to16384 originals with four entries/request. VoteBoat's
4096 is a hard original-entry ceiling. `common64` has the same mismatch at batch4.
OpenRaft's4096 append setting limits merging additional append commands; it does
not split an already larger first command. These are source-derived upper bounds,
not observed upstream batch widths. Existing raw records remain unchanged.

Current422/423 selects a bounded VoteBoat original envelope of4096*batch for the
primary profile and64*batch for common64, while preserving upstream settings.
Explicit empty-payload staging-byte accounting and effective readback qualify on
Linux with exact original results, partial1024-entry acknowledgements and crash
controls. Mac qualification and fresh release measurements remain outstanding.
The legacy JSON field remains for historical continuity; capacity_contract records
requests versus originals, hard versus soft limits and source-derived bounds.
A separately tuned common profile is diagnostic and cannot replace the published
reference floor. Same-engine418→419 comparisons retain their original envelopes.

The provider/timing audit additionally finds our latency-vector flattening occurs
inside the throughput clock, unlike the pinned driver's post-clock aggregation.
Both drivers'400s internal guard can censor the full20M singleton workload.
The next bounded slice aligns aggregation and records a common finite campaign
budget. Any reference with an extended guard must be named guard-extended and
carry its exact patch/artifact proof; the untouched reference stays distinct.
[Read-only audit and schema](validation/performance/provider-parity-audit/REPORT.md).

## Engine and representation differences

VoteBoat's image defines its compacted log base, so its historical no-op snapshot
is at applied−1000. OpenRaft snapshots the current application and retains older
log entries. Equal declared cadence does not mean equal snapshot work. Keep actual
checkpoint/retention/catch-up evidence and this label; use a separate both-retain-all
diagnostic to isolate maintenance cost, never to qualify sustained maintenance.

Both engines create one completion channel per original, including batch writes;
OpenRaft's result stream consumes those individual receivers. VoteBoat additionally
uses its operation-ID pending map, while upstream indexes responders by log position.
Removing results is not parity. Any cheaper ownership structure must preserve exact
IDs, partial-prefix outcomes, failure/cancellation and the original completion boundary.
Election/scheduling, operation identity and engine-owned application-worker choices
are recorded engine differences; existing features are preserved.
