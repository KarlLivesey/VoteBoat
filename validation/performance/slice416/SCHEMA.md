# Slice416: executable application batching capability

Purpose: make the executable's plain Counter wrapper report the existing
Counter ready-batch capability. No Core, native startup limits, scheduler policy,
storage, provider or two-update pipeline redesign belongs to this slice.

## API, data and admission

Implement `Application::supports_ready_batch` by forwarding to the active inner
application, as the wrapper already forwards context validation. Plain Counter
reports true; Maintenance retains its actual capability (currently default false)
because its protected commands can depend on the serial Core context.

Receipt shape remains `MaintenanceReceipt<CounterReceipt>` even for plain data.
Admission must retain `size_of::<Receipt>()` per original; the resulting batch
bound remains command count times this wrapper size, not CounterReceipt size.
Full pending original commands participate in Counter's dedup capacity check.
Repeated/conflicting IDs remain individual invocations and ordered committed
business outcomes, with distinct admission and log positions.

## State, ownership and failure

The existing authorized ClientRouter selects the proven batching path only for
the opted-in application. Existing visit/item/byte/output caps, protected queue
boundaries and later-candidate refusal remain authoritative. One Core update
owns the selected originals under one exact persistence dependency. Written
cannot release any original result; exact durability and ordered application
retain the existing result-routing contract. Cancellation or uncertain failure
does not turn an accepted original into a successful receipt or roll back its
identity. Retry semantics remain Counter's retained immutable original outcome.

Maintenance continues singleton execution and original envelope decoding,
context validation, administration receipt bounds, checkpoint schema and data
order. No hard-coded exemption claims Maintenance is batch-safe.

## Acceptance

Use the existing client-admission owner/worker fixtures with the actual executable
wrapper source. Generalize only the fixture's receipt-type plumbing if required.
Prove queued plain originals share one bounded update and every original retains
its position, wrapper receipt and duplicate/conflict outcome. Check queued and
retained capacity refusal, post-commit retry, no result after Written, and unknown
results after injected uncertain failure. Prove Maintenance reports its inner
capability and queued data stays singleton with correct wrapper outcomes.

Run affected integration and executable tests, formatting and scoped strict
Clippy in `target/p9/slice416-checks`. Root owns full four-profile and actual
TCP/QUIC acceptance. Record actual commands/results and retain initial failures.
No throughput claim is made without a separate frozen-source measurement.
