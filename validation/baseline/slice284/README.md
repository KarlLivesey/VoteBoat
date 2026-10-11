# Native member failed-send recovery284

The member fixture observes an actual failed native send to the absent witness,
drained outbound credits and executed cancellation. After reopening the original
witness store, it waits for both authenticated roster bindings before issuing a
fresh-context query. A connection remains insufficient authority: the retained
witness must grant, while the compacted witness must refuse. Original old-view
prefix/refusal, acknowledged data, cold recovery and owned cleanup checks remain.
No production API, persistent format, transport budget or deadline changes.

Concurrent member fixture paths include a process-local atomic sequence, fixing
the actual282 AlreadyExists failures. Diagnostic failures retain their precise
caller and actual core/resource state.

Linux and Apple Silicon macOS each pass the complete member target:50/all and
27/default,77 executions per host. Formatting and four strict all-target Clippy
profiles have zero diagnostics; all668 inputs verify before/terminal. Core and
native-only profiles provide compile/lint coverage for these TLS-gated fixtures.
Failed282/283 and original276 are preserved separately.

The original276 compacted-Joint InvalidMessage is unreproduced; passing current
complete suites does not explain that older event. Broader membership/lifecycle,
provider and full baseline obligations remain. This qualifies explicit native
failed-send host recovery, not arbitrary network availability or all P0–P7.
