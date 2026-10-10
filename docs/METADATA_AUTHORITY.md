# Moving metadata authority

The source half is implemented. Target import/activation and live-owner refresh
are the next slices; a frozen image alone does not authorize a new metadata group.

`MetadataAuthoritySource` wraps a `LifecycleDirectory` before its first bootstrap
and shares its existing Raft log, storage and runtime contracts. Construction
selects a fixed export budget, large enough for the directory's declared snapshot
bound, up to64 MiB. No second store or worker is created.

`MetadataMovePlan` binds the source/destination groups and every current local
manifest. It checks local parent/child closure, generations and destination
identity conflicts. Its derived manifests change metadata authority, local
parent/child references and route generations. Responsibility identities,
concrete data groups and ownership epochs stay unchanged. This structural plan
does not provision or activate the destination.

| Source state | Directory service | Export |
| --- | --- | --- |
| Before selected bootstrap | Uninitialized | Unavailable |
| Live, settled directory | Normal proposals and quorum reads | Plan can be prepared |
| Open lifecycle reservation | Existing lifecycle can progress | Freeze refused |
| Committed freeze at F | Mutations and authoritative reads fenced | Exact original directory checkpoint at F |

Use the wrapper's `bootstrap_command` to bind its profile, then normal directory
commands. The live-only `directory()` view supplies existing command builders
and local diagnostics, which still require matching quorum observations. Prepare `plan(destination)` and submit `freeze_command` with a fresh
operation ID. Pending admission and apply recheck the complete source view.
There is reserved control capacity for this fence even when ordinary operation
history is full. A stale plan cannot freeze a changed directory. Rejected
control IDs remain reserved in a separate bounded ledger, carried with their
original source indices and digest in the export. Filling that failure ledger
does not consume the successful-fence reserve.

After F, query `MetadataSourceQuery::Status` through the normal quorum-read path.
`export` returns immutable checkpoint bytes, plan and source status. The checkpoint and rejection-ledger digests,
schema, source group and F identify the original authority/index domain. Local
method results alone are not authenticated foreign quorum evidence. The host
must carry verified source observations into the later target protocol.

The wrapper's applied prefix can advance while its inner directory remains at F.
Exact fence retries return the original status. Later directory commands cannot
revive service. Recovery binds the initial profile, export budget, original
bootstrap, plan, F and image digest; it preserves all original directory command
history. There is no timeout-based unfreeze.

The selected downstream tests cover source/plan bounds, closed subtrees, stale
and pending views, exhausted history, exact retries, malformed checkpoints and
native journal interruption at every fence-frame byte. They establish the source
half, not end-to-end authority movement or native socket deployment.
