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
histories; executable multi-group controls and macOS execution remain separate
work.
