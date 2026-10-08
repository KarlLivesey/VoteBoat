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
feedback and does not gate further work. Compatible multi-source merge is next,
then recursive lifecycle and retirement, followed by measured P7 tuning. The full
P0–P7 objective stays active; P8 remains deferred.
