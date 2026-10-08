# Responsibility manifests and routing

This is the P5 routing foundation, not a replicated directory or a source-transfer
protocol. `routing::resolve` uses the public `ManifestCache` and `PartitionPolicy`
contracts. Native implementations are `NativeManifestCache` and
`NativeBytePartition`; hosts may supply either provider without private APIs.

## Manifest and ordering

`ResponsibilityManifest::new` validates owned `ManifestInput` and returns the
original input on rejection. The immutable checked shape includes:

- Responsibility ID/incarnation, parent responsibility/authority and its own
  metadata authority group ID/incarnation.
- Application adapter ID/version; the responsibility identity is its namespace.
- Partition scheme ID/version and a half-open bucket scope in `[0,256)`.
- Ownership epoch, route generation, explicit effective placement requirements
  and Active/Fenced lifecycle state.
- Single, Partitioned or Delegated execution. Partitioned maps contain concrete
  groups; delegated maps may mix concrete groups and child authorities.

Ranges must be sorted and cover the entire declared scope exactly once. Empty
maps, gaps, overlaps, duplicate children, direct self-delegation and invalid
versions/placement bounds reject. Indirect cycles fail during bounded traversal.
A child binding specifies exact responsibility incarnation, authority group and
ownership epoch. Resolution checks its parent binding and exact delegated scope.
Manifests are not authenticated or certified as committed by their constructor.
Full committed ancestry validation belongs to the forthcoming directory state
machine; cross-parent movement has no implementation here.

Single has one group ordering boundary. Partitioned/delegated handles have
ordering only within the concrete selected group. Directory ancestry, execution
lanes, WAL lanes and quorum topology remain separate. Effective placement fields
are declarations; cache admission neither provisions replicas nor changes quorum
membership. Ownership epoch currently covers a whole responsibility manifest;
there is no per-bucket transfer transition in this slice.

Native scheme ID 1/version 1 accepts exactly one unsigned byte as the routing
key, giving the stable bucket 0..255. Bucket selection does not depend on group
count or a language runtime hash. Other schemes are explicit host policies;
version equality and returned bucket bounds are checked in core. Hosts must
supply deterministic policies implementing the committed scheme's exact semantics.

## Hints, execution and progress

Only admit manifests obtained from an authenticated, authorized committed/applied
directory or explicitly trusted bootstrap source. The cache is volatile location
metadata, never execution authority. `resolve` returns a `RouteHint` naming the
terminal responsibility, concrete group, adapter/scheme, bucket/scope, epoch and
route generation. It does not create groups, send requests, check credentials,
read a clock, spawn a worker or acknowledge durable work.

Start cold at a known authorized root or warm at an established child locator.
Only that path is visited; the cache need not contain other subtrees. Parent
invalidation leaves the child hint available. A missing path returns the exact
missing responsibility. The caller controls authenticated fetch, redirect and
retry budgets; there is no automatic network loop. Cache hits and parent
unavailability do not establish quorum availability or leadership.

`check_owner` compares a command-carried hint and independently partitioned key
with **local committed/applied ownership**, including exact namespace, group,
scope and epoch. Invoke it at admission and again at ordered application. A
fenced local manifest refuses the earlier command. A route generation change
alone does not revoke unchanged ownership. Group leadership, local assignment,
authorization, operation identity/dedup and read barriers remain separate checks.
Do not use a cache entry as the `committed` argument. This slice does not yet wire
these calls into Node/application command execution or supply a command codec.

A larger epoch in a parent cache cannot fence an isolated old owner. Only a
source-local durable fence, valid import and durable target activation may move
ownership; P6 supplies that protocol. There is no API here that performs it.

## Resource and lifecycle contract

Both seams are synchronous, caller-driven and nonblocking; providers own their
private resources explicitly. Contract version is `ROUTING_CONTRACT_VERSION = 1`.
No persistent/wire format, durability token, asynchronous effect, watermark or
completion domain is introduced. Generations and epochs are explicit input from
the committed source, never invented on cache restart. Drop releases cache hints;
there is no accepted external work or shutdown drain. Inclusion creates nothing.

Core ceilings are 256 entries **including retained Vec capacity** per manifest,
4096 key bytes and 32 manifest visits per resolve. Cache construction permits
1..4096 manifests and 1..64 MiB of charged value/route-capacity bytes. Native
accounting includes manifest values and spare route capacity, with BTreeMap and
allocator bookkeeping additionally bounded by the entry count. It is not an
exact process-RSS bound. Host policy allocations have their own embedding budget.

Admission is atomic. Rejection returns the original manifest and allocations;
no existing entry or credit changes. A newer generation replaces a retained entry
only within its immutable identity/parent/authority/scope/adapter/scheme. Epochs
cannot regress; changed group mappings need an increased epoch. A fenced hint
cannot reactivate in the same epoch. Exact same-generation content is idempotent;
conflicting same-generation content and older generations reject. Replacement
checks the complete new retained footprint before dropping the old value.

Invalidation requires the exact observed generation so an old invalidation cannot
delete a newer retained entry. There is no automatic eviction. Invalidation or
restart forgets that entry's generation; a subsequently fetched stale hint may be
cached again. This is safe only because hints never authorize execution and the
local committed owner rejects stale contexts. Durable directory generations and
retirement tombstones must not be replaced by volatile cache history.

Errors distinguish malformed shape/schema, bounds/capacity, stale/conflicting
updates, absent paths, identity/child/parent mismatch, cycles/hop exhaustion,
out-of-scope key, fenced state, stale epoch and wrong owner. No failure partially
updates the native cache, and none implies rollback of a committed directory edit.

## Evidence and next step

`tests/routing.rs` exercises native and downstream host providers, all 256 bucket
boundaries, warm child lookup after parent removal, disjoint coverage, bounded
paths/cycles, forged provider results, schema/incarnation/epoch/context refusal,
admission-before/apply-after fence checks, atomic update/invalidation and retained
allocation/byte rejection. These are deterministic routing checks, not distributed
ownership-transfer or parent-quorum-outage tests.

Next P5 work is the replicated directory application with bounded command and
checkpoint encoding, native replay/recovery, then routed application admission
and apply through existing durable groups. Its acceptance includes a real durable
child write while the parent cannot commit, unchanged parent logs, and stale owner
refusal. Remaining P4 release faults and P6 split/merge remain on the macro plan.
