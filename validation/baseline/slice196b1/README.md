# Slice196b1 — durable Rust leadership maintenance

Base revision:b53b7b4b17bb95e4096fcd50e57d7378b4aeaf9c. RPL-1.5.
Changed production/test inputs are identified by `sources.sha256`.

The opt-in `Maintenance<A>` outer application preserves bounded original
leadership intent, completion and cancellation records through the group's
existing command log and checkpoints. Data commands delegate to the original
application; administrative commands advance its index as Noops. There is no
second storage owner, ancestor dependency, new WAL format or implicit migration.
An explicit distinct application schema is required.

The optional ProposalAdmission context method is checked by ClientRouter at
both admission and execution. A new completion is allowed only on the exact
target/current leader, original configuration and expected term, after a
current-term committed prefix through the original intent. Normal commitment
and applied client result ownership remain mandatory. Completion is historical,
not a claim that the target remains leader. Cancellation cannot undo delivery.
Hosts and composite applications must preserve the context check when using
lower-level APIs or translating inner commands.

Node accepts the existing transfer/cancel events through its control surface;
pre8/missing peer capability is rejected before transfer admission. A pure next
action method composes these controls with durable record proposals. The host
still owns deadlines, repeated-action limits, original tickets and draining.
No new durability token, maximum-ack watermark or restart generation is added.
Administrative positions are positions in the same contiguous committed log.

## Actual checks

| Evidence | Result and scope |
| --- | --- |
| `contracts.log` | Six tests pass: data/admin retries, conflicting identity, one pending operation, terminal capacity, malformed commands, atomic checkpoint restoration, and every torn byte of native Begin/Complete journal frames. |
| `native.log` | Three tests pass: old-wire refusal, native TCP/WAL pending/completed recovery and native QUIC/checkpoint pending/completed recovery. Both successful histories preserve original IDs, execute handoff, reject stale/incorrect completion, retry, write on the target and obtain fresh quorum status. |
| `regression.log` |301 all-feature tests pass: library71, effect-owner/Node157, leadership maintenance6, existing WAL maintenance5, Raft33, startup14, wire15. |
| `core.log` |233 core-only tests pass: library56, effect-owner151, leadership maintenance5, Raft21. |
| `fmt.log`, `clippy-{all,default,minimal}.log` | Formatting and all three all-target strict Clippy configurations pass with zero diagnostics. |
| `rustdoc.log` | All-feature rustdoc builds with warnings denied. |
| `inventory.log` |100 contract records pass metadata shape/path checks; not protocol correctness. |

Counts describe test executions per scope, with overlaps; they are not distinct
proofs. Native socket tests ran with authorized loopback access. Exact commands
are in `commands.txt`.

## Failures and focused corrections

Initial native attempts are retained. A temporary-directory collision was fixed
with an explicit fixture-local sequence. The QUIC checkpoint history then
exposed the fixture's assumption that a single requested campaign would beat
already queued automatic campaigns. Diagnostics showed independent persisted
self-votes and a later legitimate leader. The fixture now waits for authenticated
peers, a settled election and candidate catch-up before deliberately selecting
the source. It also waits for current-term commitment before status reads.
No consensus rule, timer default or timeout was relaxed.

The first journal-cut fixture retained a newer manifest while removing bytes
that it declared durable. Recovery correctly refused that corruption. Final
cuts use the prior durable manifest to model an unacknowledged frame; missing
declared-durable bytes remain an explicit refusal check. Those are journal
frame/model recovery tests, not disk-power-loss measurements or quorum proofs.

## Remaining scope

The native histories use Rust Node hosts and graceful worker drain/reopen;
they do not establish executable authorization, disconnected-client handling,
arbitrary process termination, all joint/recursive fault schedules, macOS or
separate-host deployment. Original operation context and public handoff use the
normal core quorum policy, but these new native histories use three-voter
majorities. The earlier core recursive handoff test is separate evidence.

Authenticated executable start/status/resume/cancel and their interruption
histories remain196b2. Until those pass,196 is incomplete and coordinated drain197
must not report a finished operator handoff. Full P0–P7 remains active; platform,
broader validation and original P7 performance gates are unchanged.
