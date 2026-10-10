# Moving metadata authority

Source fencing, import, publication and destination activation are implemented
for Rust embedding. Live-owner/locator refresh, repeated moves and native socket
composition remain; an imported image alone does not authorize a new metadata group.

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

`MetadataAuthorityTarget` binds a pristine source profile, complete plan, transfer
operation and source configuration before bootstrap. Commit its selected
`bootstrap_command`, then `import_command` with the source image and verified
configuration. Quorum status reports the destination's own stage/import indices
separately from source F. Exact retries and checkpoints preserve both domains.

`historical_directory()` exposes the original source history for inspection,
including original operation results. It is not a serving destination directory.
The target validates image/plan/rejection digests, bootstrap, complete manifests
and original indices; no directory command may occupy the source fence or a
rejected-control index. Changed imports and wrong construction bindings refuse.

This basic target profile accepts no activation or directory service. Select
`MetadataPublishingSource` and `MetadataServingTarget` before bootstrap for the
complete publication/activation path. After import, propose the source wrapper's
`publication_command` with the verified import and destination configuration.
Obtain its committed publication, then propose the destination wrapper's
`activation_command`. Both commands retain exact original results on retry.

Activation creates a private writable base with updated authority, local
references and generations. It preserves allocation reservations and original
operation outcomes. Checkpoints keep the original image and a bounded tail of
new destination commands; recovery reconstructs both domains. `Directory` queries
serve current manifests after activation. Inherited operation queries/receipts
are explicitly marked historical with the source identity; `Creation` queries
likewise name the original authority and return bounded encoded intents.

The source remains fenced after publication. New metadata writes and namespace
creation after activation are covered, as are original retries, checkpoint
truncation and native journal interruption of publication, activation and a later
write. These are selected embedding/modelled-journal tests; data-owner grants,
parent/child locators, a second metadata move and TCP/QUIC service composition
remain work for the next slice.
