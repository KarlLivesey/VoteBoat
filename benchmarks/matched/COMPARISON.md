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

## Capacity units require correction

The legacy JSON name `api_entry_batch_capacity` reports a configuration value,
not equal units. Pinned OpenRaft `BatchRaftMsgReceiver` counts incoming messages
and extends every message's complete payload batch. Its4096-message capacity can
therefore collect up to16384 originals with four entries/request. VoteBoat's
4096 is a hard original-entry ceiling. `common64` has the same mismatch at batch4.
OpenRaft's4096 append setting limits merging additional append commands; it does
not split an already larger first command. These are source-derived upper bounds,
not observed upstream batch widths. Existing raw records remain unchanged.

Before a fair capacity claim, report each setting's unit and hard/soft meaning,
qualify VoteBoat's bounded equivalent original envelope while preserving upstream
defaults, and read back actual limits. A separately tuned common-original profile
is diagnostic; it cannot replace the untouched published-default floor. Same-engine
418→419 comparisons keep the same envelopes and remain valid within their scope.

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
