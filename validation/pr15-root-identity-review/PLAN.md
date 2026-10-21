# PR15 current-review repair plan (before source edits)

Source dec5c5d79fa66340b86a6936fa8e420ce9320217; existing branch
lb/dr1-session-ledger. Reviews A5594734109 and B5594735675 both request
changes; B's corrected verdict replaces its initial approval. Parent alone
integrates main and owns fresh independent reviews and the whole final gate.

## Schema, identity and lifecycle

P2 changes only the transfer-service fixture. Reuse the exclusive create_dir /
PID + checked sequence / AlreadyExists-skip pattern in routed/delegation.rs and
fixture_isolation.rs, without a shared framework or dependency. A private fixture
Directory owns a PathBuf and dereferences to it, preserving existing callers.
A directory identity has no port-base component; the sequence is a name source,
not an ownership counter. Exclusive creation establishes ownership. Existing
files/directories/symlinks are skipped, never opened, removed or adopted.

States: unallocated -> exclusively created -> preparing -> Cluster-owned ->
children killed/waited -> normal caller deletes its exact path. The existing
normal external removal assertions stay unchanged. Directory Drop cleans its
own root during unwind, covering setup failure before Cluster construction and
failure after partial child startup. Cleanup failure during unwind is diagnosed
without a second panic. Cluster Drop still kills/waits all children before its
fields (including directory and port lease) are destroyed. Port/fork guards,
reader joins, restart behavior, handoff and shared namespace stay unchanged.
Hard-crash leftovers remain unowned stale entries and must be skipped. Tests
own and remove their simulated stale/collision entries. No executable library,
example/Cargo/lock/hook/matched change; src/raft.rs changes only the obsolete
numeric comment, with charging code preserved.

## Acceptance and negative control

Implementation detail refinement after the initial helper-only controls: the
final regression also exercises real Cluster/PortLease Drop and places forced
stale collisions in a private parent. The original before-edit plan is retained
in the raw report directory; initial helper-only passes are not final evidence.

Socket-free deterministic filesystem controls isolate the identity question
from host-wide port contention. A private unstarted-Cluster constructor uses
the real Cluster and PortLease Drop paths with a private lock-only reservation
(the existing fixture_isolation pattern, address port0, no sockets/shared pool).
It is local to this fixture regression, not a library seam. Require reacquisition
of the same base lock after A Drop, independently of other host tests. Allocate
A and retain its root after normal owner Drop, allocate B using the same helper
as Cluster, write B's marker and store, then execute A's delayed external cleanup
and require B's identity and bytes to survive. No sleeps or probabilistic loops.
A focused identity-only mutation restores the old PID/fixed-base create_dir_all
behavior in that helper; the identical control must fail its fresh-identity
assertion. Restore the patch and pass the identical command.

Use the private create(parent) implementation (new() supplies temp_dir) inside
an owned isolated parent to force collisions without adopting unknown scratch.
Force AlreadyExists with predicted next sequence entries under the fixture gate
(all fixture allocation holds that gate); test both stale directory/store and
non-directory collision, preserving their bytes. Catch an intentional panic
while the new directory is partially prepared and require owner-only removal;
then allocate a clean root with no stale stores. Retain overflow controls.
Run controls, full fixture_isolation and transfer_service sequential default/all,
and the minimal existing reconnect lease caller. Run seven strict checks:
root fmt/four Clippy configurations and matched fmt/Clippy, locked/offline,
keep-going/all-targets/-D warnings. Actual enabled pre-push repeats all seven,
guarded before each Cargo step. Fail any aggregate on any unexpected step failure.

## Linked mini / macro plan

| Item | Purpose | Dependency | Completion checks | Macro |
| --- | --- | --- | --- | --- |
| Current PR15 | Repair these P2/P3 findings and judge independent DR1 | Exact dec5, no ST2 production | Deterministic positive/negative controls, focused suites, strict7, enabled nonforce push; then parent fresh reviews + whole five-config gate | Incomplete LB, bounded application sessions |
| Next PR23 N1c | Judge existing source without rejected ST2 | Independently qualified DR1/main | Parent reviews/full suites/existing performance gate | Existing network milestone |
| Then PR22 N1b | Finish existing candidate | Reconciled main after PR23 | Existing docs caveats, reviews/validation | Existing network milestone |

Macro reviewed: no milestone completes, no new prerequisite/roadmap work.
DR1 is not automatic end-to-end lifetime maintenance. Static-bootstrap,
checkpoint-less admission, DR2/TX1 remain deferred. Old full-gate failures remain
failed history. No full matrix/exhaustive relay/performance/macOS/CI watch here.

Resources: own target only (matched shares it), cores0-3, absolute nice19 all
threads, jobs<=4/incremental0; stock diskguard before every launch/hook step,
own<=60GiB/root150/worktrees120/live<=5/free>=500. Save reports only to main-safe
pr15-root-identity-review, then mandatory own target/worktree/scratch sweep and
zero-owned-job audit/final stock guard. Never touch shared ports or others' files.
