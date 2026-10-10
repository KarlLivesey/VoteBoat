# Native multi-group startup

`native::startup::NativeMultiStartup` opens an explicitly configured group set
as one `NativeNode<A, NativeServiceConnector>`. Groups share a WAL, scheduler,
peer endpoint and snapshot worker. Each group retains its own ordered Raft log,
application and snapshot store.

Construct it from the ordinary `NativeStartup` configuration for the first
group, a sorted `Vec<Bootstrap>` of additional groups, and the complete
`BTreeMap<NodeId, StoreIdentity>` of provisioned peers. Supply an exact
`BTreeMap<GroupIdentity, A>` of fresh applications to `open`, along with
`NativePeerProtocol`, `TimerConfig`, the host wake and current monotonic time.
The returned node uses the same group-addressed proposal, read, control and
poll APIs described in [Node ownership](NODE.md).

The startup accepts at most256 groups, subject also to the native bootstrap
batch's byte limit. Group IDs must be strictly increasing, starting with the
first configuration's bootstrap. Every voter must have its exact provisioned
store identity. Creation requires the local store to be a voter in every group;
recovery can restore later membership, including learner assignments. Use a
member-capable wire version (at least2) and compatible peer configurations.
The original single-group startup APIs retain their existing behavior.

For host-discovered peer endpoints, call `prepare_for_discovery` with the same
protocol, timers, application map, wake and time. It returns `NativeNodeParts`
after native recovery and worker setup. Wrap the returned peer connector in
`DiscoveryConnector`, then pass the parts and original limits/time to
`Node::from_parts`. All groups retain one shared store and their independent
application state. See [discovery composition and cleanup](NODE.md#driving-and-service-ownership).
This opts QUIC Dial addresses into discovery while preserving peer pins and
Accept routes; ordinary `open` keeps configured-address routing.

## Persistence and recovery

`Create` writes all original bootstraps in one bounded batch and completes its
WAL barrier before constructing the node. The first group's snapshot directory
remains `snapshots`; additional groups use `snapshots-<group>-<incarnation>`.
`Recover` requires the exact complete durable group inventory and the original
bootstraps. Supply fresh applications even on recovery: verified snapshots and
WAL replay restore them before the node is returned. Changing or omitting a
group is rejected. This API does not migrate layouts or add groups to an
existing store.

Opening enables the existing member replication path; it grants no authority
to change membership or activate an application owner. Restore any host-owned
drain intent before beginning normal polling. Each group still requires its
own policy, committed configuration and application ownership checks.

## Failure and shutdown

Identity, group inventory, application and bootstrap limits are checked before
opening files or sockets. Later errors return `NativeMultiStartupRejected<A>`:
its `application` field contains the entire map, possibly partially restored,
and `try_cleanup` closes and joins started resources. Keep polling cleanup until
it returns `true` before reopening the store or endpoint. Creation failures may
leave files behind; cleanup does not roll back durable state. Do not reuse a
partially restored application as a fresh recovery input.

Drive successful nodes through the existing Node shutdown and worker-join
contracts. This startup owns no hidden executor or background poll loop.

Downstream tests in `tests/startup/multi.rs` exercise TCP and QUIC three-group
checkpoint/reopen/retry histories, omitted and changed configuration rejection,
preflight refusal and late construction cleanup. These are finite Linux local
histories; macOS execution remains separate work.

## Counter executable

Pass `--groups FILE --service-access ACCESS` to `voteboat-counter serve`. The
group file declares the complete original local assignment set, with at most256
groups and64 KiB of input:

```text
voteboat-counter-groups-v1
group 1 1 1 m:3 v:1 v:2 v:3
group 7 3 9 m:3 v:1 v:2 v:3
group 8 2 11 w:3 1 v:1 1 v:2 1 v:3
```

Each row is `group ID INCARNATION CONFIGURATION POLICY`. Policies use the
existing recursive `v:N`, `m:COUNT` and `w:COUNT WEIGHT CHILD ...` grammar.
Store identities come from the existing peer/deployment configuration. All
groups use the existing bounded counter application, with independent data and
retry histories. The profile selects wire8; use it consistently across peers.
Recover with the same original file, even after later configuration changes.
Recovery also refuses an omitted `--groups` flag when the WAL contains multiple
groups; it cannot silently open only group1.

Commands select the exact group and incarnation:

```sh
voteboat-counter client 40000 auto group 7 3 add 42 5 --service-tls ./tls --principal 3
voteboat-counter client 40000 auto group 7 3 read --service-tls ./tls --principal 3
voteboat-counter client 40000 1 group 7 3 status --service-tls ./tls --principal 3
voteboat-counter client 40000 1 group 7 3 checkpoint --service-tls ./tls --principal 3
```

The access file must grant that principal the requested permission for group7,
incarnation3; a grant for group1 does not authorize group7. Automatic routing
supports reads and adds, preserving the exact group, operation and payload
across explicit leader rejections. An interrupted write remains unknown: retry
the same complete command. Different groups may reuse an operation ID.

Unprefixed existing commands retain their original group1 scope. Node controls
such as `quit` do not accept a group prefix. The [multi-group drain controls](MAINTENANCE.md#multi-group-source-controls)
operate on the complete local assignment set. The endpoint-discovery profile
is not yet connected to this executable mode; incompatible options are rejected
before startup.
WAL maintenance and automatic checkpoints operate through the existing shared
Node. Single-group invocations remain available without `--groups`.

## List local assignments

```sh
voteboat-counter client 40000 1 list-assigned-groups - 8 --service-tls ./tls --principal 3
```

Use the returned `next` cursor for the next page; `more=false` ends the list.
Limits are1..8 rows per page and256 total assignments. Each comma-separated
`assignments` row is `GROUP:INCARNATION:ACCEPTED:STABLE:NEXT`, where the last
field is the joint configuration's target ID, or `-` outside a joint change.
The header includes the exact local node, store, store incarnation and session.

This is locally accepted configuration state, which can precede durability or
commitment. It is not global placement, live ownership or quorum evidence.
The command works on followers and without a quorum. Inspection permission
is required for group1 and every local group, even for a single-row page; a
group prefix is not supported. Existing trusted loopback mode stays available.

Cursors bind the complete assignment/configuration view and store session.
Membership changes or a reopened store invalidate them; restart the listing
with `-`. Ordinary data writes do not invalidate the view. The service retains
no cursor resources between requests.

## Group membership administration

Add `--group-admin-plans FILE` to select trusted plans for individual groups:

```text
voteboat-counter-group-admin-v1
group 7 3 seven.plan
group 8 2 eight.plan
```

Each file uses the existing `voteboat-counter-admin-v1` grammar described in
[counter administration](COUNTER_SERVICE.md). Paths are relative to the manifest
directory. Rows must be sorted, unique, and match exact groups/incarnations in
the original startup manifest. The manifest and each plan are limited to64 KiB;
there are at most256 plans,1 MiB of combined input, and4 MiB of retained
configuration records. Invalid input is rejected before opening node resources.

Plans authorize specific original operations; loading a plan does not execute
it. An authenticated administrator must address the selected group's leader:

```sh
voteboat-counter client 40000 1 group 7 3 configure 7001 --service-tls ./tls --principal 3
voteboat-counter client 40000 1 group 7 3 configuration-status 7001 --service-tls ./tls --principal 3
```

An invocation advances one configuration record. After a committed joint change,
repeat the same operation to finalize; after interruption inspect status and
retry the same original plan and operation. Recovery requires the original group
file and the same trusted plans. Separate groups may reuse operation IDs without
sharing pending requests, completion replies or durable configuration history.
Groups without a selected plan remain readable but cannot be configured through
this interface. Permissions are checked against the exact group/incarnation both
when admitting a command and when executing its configuration proposal.

## Group leadership maintenance

Add `--leadership-maintenance enabled` consistently on every peer to use
the existing schema2 maintenance application for each declared group. Create
fresh stores with that profile and retain it on recovery. Existing plain-counter
stores require migration; changing the flag does not convert their records or
checkpoints. The profile uses the existing wire8 transport.

The [leadership commands](MAINTENANCE.md) accept an exact group prefix:

```sh
voteboat-counter client 40000 1 group 7 3 move-leader 40001 9 2 2 1 --service-tls ./tls --principal 3
voteboat-counter client 40000 2 group 7 3 leadership-status 40001 --service-tls ./tls --principal 3
```

The move arguments retain their existing meaning: operation ID, expected stable
configuration, target node, exact store and store incarnation. `resume-leadership`
and `cancel-leadership` take the same group prefix and original operation ID.
Address the current group leader explicitly. Status is a quorum-backed read of
the durable historical record, not proof that the target is still leader now.

Each group has its own attempt, deadline, cancellation and completed record.
Different groups may reuse an operation ID. A cancellation in one group cannot
cancel another group's same-ID handoff. Restart resumes durable pending work;
completed records remain idempotent. The existing group administration plans
also use schema2 readiness requirements in this profile. This enables individual
group handoffs. Multi-group source drain controls and the bounded foreground
runner are available as described below.

## Multi-group source drain

Use `--node-drain enabled` on all peers with the maintenance profile. On the
source, add `--group-drain-plan FILE`; voter entries require the corresponding
`--group-admin-plans` records. The original full plan binds every local
assignment, including groups where the source is already a learner. Start,
status, resume, cancellation and checked stop operate on that complete plan.
Per-group leadership and membership commands perform the individual moves.
Use `group-drain-run BASE SOURCE SEQUENCE OP --service-tls DIR --principal ID`
to drive those moves with the original plan; `--command-peers FILE` supplies
explicit remote endpoints. It requires permission for every source assignment
and endpoints for each planned target. It reports success only after the source
accepts its readiness-checked shutdown.
See [the plan grammar and operator sequence](MAINTENANCE.md#multi-group-source-controls).
