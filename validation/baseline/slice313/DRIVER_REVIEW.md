# Slice313 independent driver review

Status: settled production source reviewed after the design phase. No concrete
correctness/provenance blocker was found in the driver under its declared trusted
host/provider and fresh-generation contracts. The initially reported embedding
failure-cleanup defect was corrected in the focused final re-review; the exact
new coverage and remaining limits are recorded below. Execution qualification
remains root's responsibility.
Read-only scope: root Schema313, published312 `PLANNED_DRIVER_SCHEMA.md`, qualified
transaction applications, actual generic Node/client/read contracts, every
`src/transaction/driver*` module, downstream host-provider tests, native history/
ownership/recovery helpers and the standalone transfer example. No production
edits, builds, tests or git operations were performed by this review.

The proposed exclusive borrow of three generic Nodes uses existing public
provider bounds and ownership APIs (`runtime/node.rs:177–216,483–561`). It does
not need another storage owner or observation-injection API. Client Applied
authority comes from the owning Node's original opaque completion, whose router
has already matched applied position, operation and retained command content
(`runtime/clients.rs:505–591`). Read authority comes from the original consumed
Node read, backed by its core-created live quorum barrier and original query
(`runtime/read_requests.rs:485–594,692–727`). Leader metadata alone is only a
routing hint. A returned status may include phases applied after the barrier's
captured index; that is valid under the existing application read contract.

Two ownership conditions were communicated during design:

- Consuming Unknown/Unavailable releases observation ownership, but may leave
  the underlying client reservation or read cancellation owned by the Node.
  `clients.rs:353–375,616–665` and `read_requests.rs:395–416,620–691` require
  actual takeover/cancellation reconciliation. Clearing an attempt must not
  imply drain; driver drain must check both actual routers, including read
  execution ownership. Provider failure cleanup must still consume terminal
  outputs and retain any rejected opaque output without releasing its credit.
- The proposed explicit TransferDriverGeneration requires a fresh generation
  on every attach to the same RuntimeOwner. A nonzero value alone does not
  establish freshness. Stale cross-driver tests must carry an actual old
  exported completion to a new fresh generation and prove fresh credit is
  unchanged. Reusing generation/sequence under the same owner must be described
  as outside that explicit freshness contract if not prevented by construction.

The native test agent plans actual three-replica/three-group TCP and QUIC commit,
reverse direction/canonical prepare, locked quorum reads, durable funds refusal
and absent-peer Abort, full-capacity completion, lost observation, real leader
replacement and checkpoint/suffix cold resume. Public diagnostic state is only
used to choose test interruption points, never to give the driver readiness.
Strict provenance negatives must exercise real opaque Node completions. Mutating
a private expected attempt while consuming a real completion is sufficient and
does not require a public injection API; constructed receipt/ticket scalars alone
would only demonstrate shape checks. Constructor rejection of nonempty routers
does not independently exercise driver output matching.

## Settled source findings

- `construction.rs` accepts 1..64 generic homogeneous Node handles with selective
  group inventories. It checks Running/actually drained routers, distinct router
  bindings, expected application roles/profile/incarnations, identical configured
  openings/capacities for each group copy, selected command/read/receipt envelopes,
  and full participant lifetime capacity. Submit preflights business account
  membership and all retained/pending Start namespace/capacity without using a
  local balance/status as phase authority. This matches the later composition
  refinement; the native history cohort uses three physical handles.
- `polling/consume.rs` consumes each actual opaque output on the Node from which
  it was polled. Full original ticket correlation precedes interpreting its
  result. Client interpretation checks expected Node slot, command-derived
  operation, encoded original content, current intended command and actual
  position/receipt agreement. Reads check original Node/read binding, transaction
  Status query and actual barrier group/request. Valid unexpected results are
  consumed only for cleanup, then fail the driver; rejected ownership is retained.
- `protocol.rs` builds PhaseFact from the receipt's nonzero canonical first index,
  never its duplicate invocation's current index. All command assertions derive
  from immutable spec and original retained positions. Fresh coordinator status
  reconstructs the original spec/decision and Decided/AppliedAll/Finished position;
  retries of participant phases recover their original first positions through
  actual duplicate Applied results. Prepare order is canonical slot0 then slot1,
  independent of debit direction. Only durable Prepared/Refused can decide;
  Unknown/NotProposed/Unavailable reread fresh status, absence yields Unknown,
  and admission failures produce error/cleanup rather than business Abort.
- Release requires actual AppliedAll and Finish requires both actual Releases.
  Resume after Decided repeats applications with the original decision fact;
  AppliedAll repeats releases with its original fact; Finished returns the
  original terminal position. There is no wall-clock decision rule, compensation
  or unlock on timeout/cancel. Configuration IDs are not frozen; actual Node read
  authority and existing P4 membership remain available.
- Driver requests/output slots are retained until original exported completion
  consumption, at most K active records and one observed attempt per work item.
  Encoded input and boxed command/status objects are fixed-size and bounded by
  these slots. Selected Node routers separately bound underlying reservations,
  including abandoned attempts after observation consumption; DriverUsage.pending
  is not a promise that those Node credits are already zero. Retained contract
  faults block further polling of their Node stream, bounding them by two per
  selected handle. Final `cleanup_retained` rotates unsuccessful retained outputs
  within the completion budget and derives no phase authority from cleanup.
- Failure stops intake/phase production and cancels waits. Even when advance
  continues returning the stored error, it still polls Nodes, consumes terminal
  results and reconciles stopped work. `is_drained` also checks actual client/read
  routers, ready/exported outputs and retained fault ownership. Permanently
  unconsumable provider-contract output correctly prevents a drain claim.
  Drop itself is not cleanup or rollback; the host must drive cleanup/recovery.
- Fresh TransferDriverGeneration on each attach under the same RuntimeOwner is
  explicitly documented as a host obligation in `types.rs`. Actual stale output
  tests use a different generation and check that fresh usage/credit is unchanged.
  Unconditional safety under deliberate owner+generation reuse is not claimed.

## Actual negative-control source inspection

`driver/tests/native.rs` obtains a real single-voter native Node Applied output
while driver completions/transitions are withheld, then corrupts only its retained
expected node, ticket sequence, original command or operation. It requires
ProviderViolation, coordinator remaining Started, no participant Prepare and
actual cleanup drain. Three analogous controls use actual opaque quorum-backed
read outputs with corrupted expected node/ticket/query and require no transaction
record. These seven controls are meaningful output-matching tests, not manually
constructed Applied/read observations. They exercise single-voter native storage
and original Node ownership, not multi-replica network faults.

The public native ownership histories reject construction over another consumer's
actual pending Start, export an actual Finished DriverCompletion and reject it at
a fresh-generation driver without freeing fresh credits, and resume an absent
transaction as Unknown without creating Abort state. Public-only core fixtures
also drive actual generic Node completions using host-supplied providers and
selective single-voter group inventories. That demonstrates composition, not
network quorum fault tolerance.

Native positive source assertions cover TCP/QUIC commit/reverse/refusal, lifetime
capacity and exact retry, locked fresh account/status reads, absent-peer Abort,
actual observation loss, original first facts, checkpoint plus committed suffix
cold reopen and Decided/AppliedAll/Finished resume. Observation loss withholds
driver consumption while Nodes continue healthy replication; it is not network
packet loss. Leader replacement gracefully drains the old coordinator leader
while a majority survives; it is not an abrupt process/power crash.

The added `tests/transaction_driver/unavailable.rs` retains two actual original
Node owners without polling them and drives only the remaining replica. After
actual preparation and quorum-backed locked reads, a new driver cannot obtain
a terminal result for two seconds from its fresh coordinator Status read.
Stopping returns Unknown(Stopped) and drains actual observation ownership;
the survivor's local applied states remain exactly the original states. Those
local checks are diagnostic invariants, not authority. After the two owners are
returned to polling, fresh quorum reads verify the original states and retained
locks, then a fresh driver resumes the original transaction to Commit and exact
balances. Both TCP and QUIC variants are present. This establishes the declared
unpolled-peer unavailable-quorum history, not a physical socket partition,
abrupt process termination or power-loss test. No tests were executed by this
review.

## Embedding cleanup finding and focused correction

The initial review found that `examples/embedded_transfer.rs` returns on an
advance error, dropping the borrowed driver, then calls `host::close`. The old
close submitted Checkpoint controls first, which return Closed for a
RecoveryRequired Node and therefore bypassed worker reclamation/join. Root was
notified before publication; this was separate from production driver cleanup.

The settled `examples/embedded_transfer/host/cleanup.rs` now branches on actual
RecoveryRequired before checkpoint admission and calls `into_recovery`. Existing
Node recovery entry closes the peer driver, WAL/snapshot workers and outbound
owner without claiming accepted writes rolled back. The helper drains the
transferred actual peer/outbound ownership, joins the actual dialer, polls WAL
terminal/reclaim and snapshot terminal events, and calls each worker's actual
`try_reclaim` until stores are returned and threads joined. It does not feed
discarded events back into the failed core or declare that core drained. Normal
shutdown failure explicitly aborts/transfers the Node and attempts this same
cleanup while preserving the original error. Successful joins are printed only
after all three providers report completion; drain/join timeout or provider
failure returns an error rather than success. No universal worker-panic or
cleanup-timeout join guarantee is inferred from that bounded error path.

`examples/embedded_transfer/tests.rs` obtains a real accepted Start ticket,
explicitly aborts before any observed Applied receipt, checks RecoveryRequired
and Aborted, then exercises recovery cleanup and cold reopens the actual files.
That is an actual admission/abort/worker-reclaim/reopen control. It does not claim
acknowledged Start preservation or inject physical I/O failure. Root reports the
focused example test passed 1/0; this reviewer inspected source without executing
it. No remaining concrete blocker was found in these two changes under their
declared scope.
