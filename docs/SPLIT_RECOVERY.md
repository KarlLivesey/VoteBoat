# Split recovery and trusted host resumption

The first top-level whole-responsibility split uses the existing public
[transfer intent](TRANSFER_INTENTS.md), [source fence](SOURCE_FENCING.md),
[target import](TARGET_IMPORTS.md), [metadata publication](TRANSFER_PUBLICATION.md)
and [target activation](TARGET_ACTIVATION.md) contracts. Each group keeps its own
ordered durable log. Source F, target import I, metadata publication and target
activation are separate indices; none is a global commit counter.

An authenticated trusted embedding host can resume from quorum-readable state:

| Observed durable state | Next permitted action |
| --- | --- |
| No retained successful intent | Commit the exact checked intent in its directory. |
| Intent retained, a target not staged | Commit that target's exact bootstrap under the lifecycle ID. |
| Both targets staged, source not frozen | Commit the matching source freeze. |
| Source frozen, a target not imported | Export that target's immutable scope through F and commit its complete inline import. |
| All imports retained, publication absent | Verify matching complete source/target observations and commit metadata publication. |
| Publication retained, a target inactive | Verify the original directory decision against that target's local import and commit activation. |
| All required targets activated | Serve those disjoint scopes under their new epoch; retain original lineage and old-source fence. |

This is a composition recipe, not a newly implemented autonomous coordinator,
operator endpoint or cryptographic certificate scheme. The host authenticates the
actual observing group/configuration and obtains fresh quorum barriers. A timeout
is an unknown result and never evidence that an intent/fence/import/publication/
activation is absent. A local diagnostic or serializable record is insufficient.
Changed/contradictory observations must be resolved through the existing checked
contracts, not by guessing an owner or creating an empty replacement.

Use the same lifecycle/semantic operation IDs across retries. Successful committed
phase queries retain the first indices/digests, even after subsequent noops,
leadership changes, retries and checkpoint recovery. Application data/results/
outbox move through scope images and actual target import, not through a majority
of image hashes. After fencing, complete the transfer forward; no unfreeze path is
provided. A partially activated split may serve one target while the other pauses.
Ordinary active target writes and reads require no parent/source access.

## Selected native recovery ledger

`tests/routed/split.rs` composes real three-replica metadata, source and two target
groups. Its test host reconstructs the next action from fresh quorum observations
on every invocation and discards action receipts. Expected retained statuses are
comparison oracles only; they do not drive resumption.

Each history closes/joins every native owner and reopens from its selected files
after each of nine committed boundaries: intent, left stage, right stage, source
fence, left import, right import, publication, left activation and right activation.
One variant retains WAL; another checkpoints before each reopen. TCP/TLS and QUIC
use the same application contracts. At every boundary and after reopen, queries
check which owners can serve and inactive/fenced admission refuses data.

After both targets activate, metadata/source workers stop before original-operation
retries and new writes to both child groups. Another reopen must retain original
phase identities, updated values, retry behavior and outbox. The old source remains
fenced, and old route contexts refuse at targets. The phase trace must finish once
without regression or a duplicate phase action.

These selected interruptions occur after commitment and graceful worker drain;
they do not cut power mid-write, drop a partially replicated phase entry, partition
a quorum or establish arbitrary-fault liveness. Earlier storage/core fault tests
remain separate evidence. This ledger does not implement cancellation, retirement,
subsequent transfer of an activated target, delegated-parent coordination,
distributed merge, an automatic resumer or macOS/separate-host validation.

Run the selected ledger with:

```sh
cargo +stable test --locked --offline --all-features --test routed split_resumes -- --nocapture
```

Independent native histories serialize their worker/socket topologies inside the
test binary; each history retains concurrent replicas/groups. CI is background
feedback and does not gate further work. Compatible [merge](MERGE_RECOVERY.md) and
selected [repeated transfers](REPEATED_TRANSFERS.md) are covered. Recursive lifecycle
and retirement remain, followed by measured P7 tuning. The full
P0–P7 objective stays active; P8 remains deferred.

## Source membership changes during a split

Source membership and transferred ownership are separate state. The split's
source remains one Raft log while it changes voters through joint consensus.
A source fence cannot be undone by that configuration change, restart or by
losing a configuration reply. Imported provenance names the configuration under
which the source was observed, rather than the original bootstrap configuration.
Later source finalization does not rewrite an already imported lineage record.

The slice185 native history stages both targets, commits source joint
configuration2 (voters1/2/3 changing to2/3, with1 retained as learner), then
commits the source fence. It leaves the configuration completion unread and
aborts the source owners. Recovery preserves the joint operation, exact fence
and export bytes. Source, target and metadata observations drive import,
publication and activation through the existing public APIs. The operation's
local durable status generates its authorized finalization through
`Node::resume_configuration`; an obsolete expected-configuration record is not
submitted as a fresh change.

After final configuration3 is recovered, all source replicas remain fenced and
replica1 is a learner. Targets retain configuration2 in their import provenance.
Child writes and retries continue with source/metadata off, then survive target
reopen without repeating outbox effects. The test fixture selects explicit
member startup and drains aborted transport and storage ownership before reopen.
It does not use a bootstrap ID as current quorum evidence.

This selected composition does not establish arbitrary concurrent reconfiguration,
new-voter interruption, revocation, rollback, device power-loss or all recursive
lifecycle schedules. Those remain separate baseline obligations.
