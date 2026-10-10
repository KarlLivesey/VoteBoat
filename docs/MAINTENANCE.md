# Durable leadership maintenance in Rust

`Maintenance<A>` wraps a host application and stores leadership intents and
outcomes in the same group's replicated log and checkpoints. Select it as the
outer application with a distinct schema and a pristine inner application;
existing unwrapped data files require an explicit migration. This does not add
a second persistence owner or change default service formats.

Use matching wire8 peer assemblies. Submit `LeadershipCommand::Begin` through
`Node::propose`, retaining its original operation ID and exact source/target
node, store and configuration identities. Admission alone is not durability;
wait for the normal applied client outcome.

On each relevant advancement, `Maintenance::leadership_action(core)` returns:

- `Transfer(request)`: submit `NodeControl::TransferLeadership(request)`.
- `Complete(command)`: submit the command through the same client router.
- `Wait`: no action is currently justified.

The host owns polling, request tickets, duplicate-action suppression, deadlines
and shutdown. The pure decision method starts no workers and performs no I/O.
Before discarding a waiting client/read, use the existing Node cancellation and
completion contracts. On a transfer deadline, cancel the exact core request
context to release local quiescence; this cannot undo a delivered signal.
The durable intent remains pending for observation or resumption.

Completion proposals are checked at both admission and execution. Only the
exact target, leading in the recorded current term and configuration after a
current-term commit, can create a new completion. A configuration change can
leave an operation pending; an operator may durably cancel it using the original
intent and prepared index. Cancellation stops that intent's retries and does
not assert the target never became leader.

`MaintenanceQuery::Leadership(operation)` through `Node::read` provides a
quorum-backed status result. `record()` and `pending()` expose only local applied
state for the serialized host driver. A completed record is historical: it
proves that the target led and its completion committed, not that it still leads.
Missing local state or a disconnected wait never proves non-execution.

Use `Maintenance::data` to frame normal inner commands. Data and administrative
operation IDs have separate namespaces. The wrapper forwards inner admission,
context checks, receipt limits, queries and checkpoint state. Every log index
reaches the inner application; maintenance entries appear as Noops. A bounded
permanent record table has one pending operation at a time, rejects conflicting
identities, and reserves space for terminal updates. Completed/cancelled records
are retained, including across checkpoint recovery; there is no eviction policy.

Native TCP and QUIC Rust histories cover pending and completed restart, exact
retry, target data writes, version refusal and quorum status. Application tests
cover malformed/checkpoint input, full capacity and torn native journal records.
See [evidence](../validation/baseline/slice196b1/README.md).

## Authenticated counter executable

On a new three-node cluster, append these options to **every** serve command:

```text
--service-access ACCESS_FILE --leadership-maintenance enabled
```

Use the same options on recovery. This explicitly selects counter application
schema2, wire8 and 64 permanent maintenance records. The existing plain counter
profile stays schema1. Do not enable the new profile on existing plain data or
mix the profiles between replicas. Membership administration uses explicitly
requested `--remote-admin-plan` operations and the wrapper's actual readiness
requirements. Automatic administration is refused with this profile. There is
no automatic migration or history eviction.

When enrolling an assigned learner from this profile, select the same schema:

```text
enroll create DESTINATION NODE BASE TLS_DIR SOURCE_DIRECTORY SOURCE_NODE --deployment DEPLOYMENT_FILE --leadership-maintenance enabled
```

Stop the source and destination first and checkpoint the committed learner
assignment. Use `enroll recover` with the same inputs after an uncertain result.
The import preserves Counter retries and replicated maintenance records; a
wrong profile refuses rather than converting data. Start the learner with
`serve recover-member`, the same maintenance flag and its access policy.
Enrollment does not copy the source's local `drain.record`: the new learner
starts without `--node-drain`. Enabling that local journal on an already
enrolled store still requires an explicit migration.

Use the ordinary authenticated client flags, selecting a current leader node:

```text
client BASE NODE move-leader OP CONFIG TARGET STORE INC --service-tls TLS_DIR --principal ADMIN
client BASE NODE leadership-status OP --service-tls TLS_DIR --principal READER
client BASE NODE resume-leadership OP --service-tls TLS_DIR --principal ADMIN
client BASE NODE cancel-leadership OP --service-tls TLS_DIR --principal ADMIN
```

`OP` is a nonzero u128. `CONFIG` is the expected stable configuration; `TARGET`,
`STORE` and `INC` are the exact target identity, not an endpoint. For the initial
demo configuration, CONFIG and INC are 1 and STORE equals TARGET. New intent
binds the current source; retries preserve its original recorded source.
The automatic `client ... auto` route still accepts only data read/add commands.

Start returns the applied Pending record, not handoff success. All replicas can
resume that durable intent after restart. Each local leader gets one attempt per
intent/term, bounded to five seconds, including an internal completion wait.
Timeout releases local quiescence and request ownership, preserving Pending.
Resume authorizes another local attempt and returns a fresh status read. Status
requires Inspect permission; the other three commands require Admin.

Cancel stops the local attempt before proposing the durable cancellation. If
completion wins the race, cancellation returns the original Completed record.
Retrying an already terminal cancellation uses a fresh read and leaves other
operations alone. Disconnecting a command releases its wait; it cannot undo an
already committed intent. Preserve the operation ID and exact fields on unknown
outcomes, then query a current leader. A Completed record is historical evidence,
not a promise that its target is still leader.

Slice196b2 tests use actual authenticated TCP/QUIC service processes, pending and
terminal restart, a killed target, lost replies, denied mutations, disconnected
and expired status waits, and incompatible plain data. See
[executable evidence](../validation/baseline/slice196b2/README.md).

## Retained-replica node drain

For a new maintenance-profile service, also pass `--node-drain enabled`.
Keep this option on recovery. Creation durably initializes `drain.record`;
recovery requires the file and rejects corruption or a changed owner. Do not
delete it or omit the profile to bypass an active drain. Existing services
without an initialized journal need an explicit migration before using this
profile; recovery does not silently create one.

Use authenticated commands against the node being drained:

```text
drain-node SEQUENCE OP CONFIG TARGET STORE INC
drain-status SEQUENCE OP
resume-drain SEQUENCE OP
cancel-drain SEQUENCE OP
drain-stop SEQUENCE OP
```

`SEQUENCE` is a positive local u64; `OP` is a nonzero u128. Preserve all fields
when retrying. A later sequence needs a cancelled predecessor and a new operation
ID. Start commits the original handoff, publishes the local drain journal and
closes ordinary data admission before releasing the local handoff driver. A
lost reply is unknown; retry the original request. Disconnecting a wait does not
cancel the operation. A journal-publication failure stops the process and
requires recovery. Journal I/O runs on one bounded, joined worker.

Status works on the source after it becomes a follower. `evidence=local_durable`
reports the local journal and locally applied handoff history; it does not claim
current remote availability. `ready=true` requires completed handoff, unchanged
committed stable configuration, sufficient remaining configured voter capacity
under the actual recursive policy, and local quiescence. `drain-stop` checks
these conditions again, then drains and joins workers. Restart restores the gate
before accepting data and never automatically exits.

`cancel-drain` durably cancels the **local gate**. Admission reopens only after
the queued campaign-enable operations complete. It does not undo an already
delivered handoff or cancel its separate replicated intent; use
`cancel-leadership OP` on the current leader for that operation. Status explicitly
reports `cancellation_scope=local_gate`. Inspect permission permits status;
administration permits start/resume/cancel; shutdown permission permits stop.

This profile retains the original replicas. It supports maintenance/reboot when
the remaining configured voters can satisfy the policy. It rejects a required
membership change. The planned workflow below supports executable membership
changes. Multi-group orchestration remains work; these commands are not a decommissioning
certificate. See [slice197b2a evidence](../validation/baseline/slice197b2a/README.md).

## Membership-aware drain in Rust

`MembershipDrainPlan` binds an explicit evacuation plan to the durable journal.
For each assigned group, supply its original `Configuration`, exact handoff
voter and original `PlannedVoterChange` joint/final records. Groups must be
sorted and unique. The plan is bounded to 1024 groups and 4 MiB retained input.
The target excludes the source from voters and retains it as a learner so the
source can observe the final commit before shutdown. New voters must already
be exact prepared learners; normal readiness and placement authorization still
apply when submitting the configuration.

Persist the original plan in host-owned configuration before publishing
`plan.record(sequence)` through `DrainJournal`. The journal stores a canonical
SHA-256 fingerprint, not the plan itself. On recovery, reload that original
plan, verify it against the recovered journal, and call `Node::restore_drain`
before polling or exposing application work. Changed policies, stores, IDs,
handoff targets or assignment sets cannot substitute for the original plan.
Local-only `DrainRecord` callers set `plan: None`; their existing V1 records
remain readable. Bound plans use V2 records, at most 32912 bytes.

Drive `plan.next(journal, core)` on each group's current leader:

- `Transfer`: submit the exact `NodeControl::TransferLeadership` request.
- `Configure`: submit the original record through `Node::configure`, retaining
  its ticket and using normal readiness and execution-time authorization.
- `Wait`: continue polling; preserve accepted tickets and original IDs.
- `Completed`: this group has the exact committed final membership.

The host owns action suppression, remote delivery, deadlines and cancellation
of waits. The decision method performs no I/O. Lost replies do not change
operation IDs or justify skipping joint consensus. Cancellation of the local
journal gate does not reverse a committed configuration or a delivered handoff.

`Node::membership_drain_ready(plan, journal)` checks the restored active record,
complete local assignment coverage, every exact committed final configuration,
application catch-up and local quiescence. Only then should the host start the
ordinary joined shutdown. This is local durable evidence, not a measurement of
current remote availability. Removing the remaining learner is a later explicit
membership operation. The retained-replica executable refuses a bound journal
without an explicit plan file. See [slice197b2b1 evidence](../validation/baseline/slice197b2b1/README.md).

## Executable membership-drain workflow

Initialize all replicas using the maintenance profile and `--node-drain enabled`.
Stop them, then use `recover-member` with the same options and an authenticated
`--remote-admin-plan ADMIN_FILE` on every peer. Add `--membership-drain DRAIN_FILE`
on the source only. Preserve both original files across recovery.

Example drain file for source1, handoff2, and final voters2/3 with source1 retained
as a learner:

```text
voteboat-counter-drain-v1
operation 19701
source 1 1 1
handoff 2 2 1
original 1 - m:3 v:1 v:2 v:3
joint 19751 1 2 3 1 m:2 v:2 v:3
final 19751 2 3
```

The admin file must contain those exact joint/final lines and the ordinary
placement/replica declarations described in [service administration](COUNTER_SERVICE.md).
Store identities must match deployment. Both files are bounded to64KiB; malformed
or incompatible plans refuse startup. Changes to an active journal's plan refuse
recovery. This is a single-group workflow using explicit operator steps:

1. On the source leader, issue authenticated `drain-node 1 19701`. It persists
   the bound intent before releasing handoff. Preserve the IDs after a lost reply.
2. On the current leader, issue `configure 19751`; inspect
   `configuration-status 19751`. Resume the same operation to finalize once the
   joint configuration has committed. Every request requires live Admin authority.
3. On the source, inspect `drain-status 1 19701`. Only `ready=true` permits
   `drain-stop 1 19701`, which rechecks final membership and joins workers.

After restart, `resume-drain` resumes any pending original handoff; configuration
execution still needs the explicit request on the current leader. Cancellation
only reopens the local gate; it does not restore removed voting rights. Restart
never automatically stops the service. Final learner removal uses the explicit
step below. Automatic multi-group coordination and broader replacement-promotion
fault coverage remain open.

### Drive the single-group workflow with one client

With the same files and service profiles above, run:

```sh
voteboat-counter drain-run BASE SOURCE SEQUENCE OP \
  --service-tls TLS_DIRECTORY --principal ADMIN
```

Use `--command-peers FILE` for explicit remote endpoints. Include the source
and eligible leaders. The source must initially lead, or already have this
durable drain. The runner starts/resumes that identity, reads the bound
membership operation and plan fingerprint from authenticated source status,
finds the current leader and drives joint/final completion. It asks the source
to stop only after `ready=true` and the source rechecks its own stop conditions.

The runner makes at most128 requests within45 seconds, with five-second request
deadlines and one owned connection at a time. It is a foreground process; no
daemon or new consensus owner is created. Killing it does not cancel accepted
work. After an interruption, rerun the exact same sequence and operation.
Malformed identities, changed fingerprints and authorization failures stop it.
An interrupted configuration or shutdown request is an unknown outcome.

Success says `shutdown_requested=true`: the source accepted shutdown after its
local readiness check. It does not certify that remote worker joining finished
or that every remote voter is currently available. The source remains a learner
until the separate, explicitly authorized removal below.

### Remove the retained learner

Before starting the drain, provision a separate learner-removal record in the
remaining peers' admin files. For the example above, add:

```text
learners 19780 3 4 - m:2 v:2 v:3
```

This expects committed configuration3, retains voters2/3 with the same policy,
and creates configuration4 without source1. Use a distinct operation ID. For
other plans preserve all other learners and the exact final voters and policy.
The existing placement authorization and expected-configuration checks apply.

After the source has completed shutdown and joined its workers, submit the
removal to a remaining leader with the ordinary authenticated client:

```sh
voteboat-counter client BASE LEADER configure 19780 --service-tls TLS_DIRECTORY --principal ADMIN
voteboat-counter client BASE LEADER configuration-status 19780 --service-tls TLS_DIRECTORY --principal ADMIN
```

Use the same `--command-peers FILE` if the endpoints are explicitly configured.
Preserve the original ID and record after a lost reply; resubmit to a remaining
leader and inspect its committed status. `action=wait_for_commit` is not finished
removal. `action=completed` reports the locally durable committed operation;
it does not certify current remote availability. A new quorum-backed write/read
can check current service availability separately.

Removal changes membership; it does not delete the source's files or journal.
Keep the original drain journal and plan if those files are restarted: their
gate remains active, and the stale source has no authority to rejoin the new
configuration. Re-enrollment is a separate, explicitly authorized operation.
Selected TCP/WAL and QUIC/checkpoint tests cover missing-quorum acceptance,
lost replies, leader restart, exact membership recovery and stale-source
admission refusal. Multi-group orchestration and arbitrary faults remain open.

## Bounded multi-group Rust driver

`MembershipDrainCoordinator::new(plan, in_flight_limit)` owns the original
multi-group plan and a rotating scan cursor. Call `poll(journal, scan_budget,
observe)` with fresh Raft views from the host's current group leaders. Missing
views are skipped; per-group errors are returned alongside independent requests.
The plan remains limited to1024 groups and4MiB; scan budgets are1–1024 and the
in-flight limit is1–group count. There is at most one outstanding dispatch per
group. Polling performs no I/O or background work and creates no runtime.

Each request contains the original transfer/configuration action and an opaque
`DrainDispatchTicket`. Retain that ticket alongside the ordinary Node request
or transfer context. After rejection or the end of that wait, call `finish` to
release the slot. Duplicate, foreign and pre-restart tickets are rejected.
Finishing a ticket proves neither commit nor rollback. An unknown configuration
result requires a fresh core observation before the next original-ID attempt.
Transfer deadlines and cancellation remain the host's responsibility through
the existing transfer context; an expired wait does not undo a delivered signal.

The host still supplies readiness, execution-time placement authorization,
normal Node polling, bounded completion processing and cleanup. Dropping the
coordinator does not cancel accepted work. On restart, recover the original
plan and journal, restore the source gate before polling Nodes, construct a new
coordinator and observe committed state again. Do not count old completions.

`observed_complete` counts only this scan. It is never an aggregate stop
certificate. Use the source's `Node::membership_drain_ready` with that original
plan and journal for the local stop check. The native shared-WAL tests exercise
three groups with different handoff leaders, incomplete joint progress, lost
waits and restart over TCP/QUIC. This Rust dispatcher does not add multi-group
commands to the single-group executable or certify remote availability.
