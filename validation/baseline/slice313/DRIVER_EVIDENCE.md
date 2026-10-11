# Slice313 bounded transaction driver evidence

Schema source: root `docs/IMPLEMENTATION.md` Schema313 and `validation/baseline/slice312/PLANNED_DRIVER_SCHEMA.md`. Production ownership is only `src/transaction/driver.rs`, its child modules and the minimal entry/re-export in `src/transaction.rs`.

## Implemented

- Generic borrowed cohort of 1..64 existing Nodes, with heterogeneous local group inventories and homogeneous selected public provider types. Each exact coordinator/participant group must be represented. Constructor validates immutable profile/role/incarnation/limits and account IDs **and opening balances**, participant lifetime capacity >= coordinator lifetime capacity, required receipt/read byte budgets, Running state and empty/exclusive client/read streams. Current balances and local transaction phases supply no authority.
- `submit` accepts the complete immutable spec and immediately owns an actual original Node Start proposal ticket; `resume` owns an actual original coordinator quorum Status read ticket. Retained/pending original Start specs reserve all ten phase IDs at the coordinator. Intake failures return the original business request.
- Single command/read attempt per accepted invocation. Exact full opaque Node completion tickets, actual node slot, original operation/command bytes or query, receipt current applied position and canonical first retained index are correlated before deriving private protocol facts.
- Canonical Prepare0/Prepare1, durable Commit-or-Abort, Apply0/Apply1, durable AppliedAll, Release0/Release1 and durable Finish. Abort requires an actual retained PrepareRefusal; no timeout/pre-admission refusal/Unknown is a business Abort fact. Account mutation/release remain owned by the qualified application312.
- Lost/uncertain observations fresh-read coordinator status, validate retained immutable content/phases/proof shapes and reconstruct the original first-index dependent commands. Fresh status is not rejected merely because retained phase positions are newer than its read barrier index.
- Driver generations are explicit and bound to an actual RuntimeOwner. Host MUST use a fresh generation on each attach under the same owner. Reuse is outside this contract; there is no hidden global generation allocator.
- Bounded active business slots retain credits until the original opaque exported DriverCompletion is consumed. Wrong/stale completions return their original ownership. DriverUsage counts actual retained command Vec capacity, pending/output/exported credits and retained fault outputs.
- `close` closes intake and continues accepted work. `stop_waiting` cancels original waits, tolerates StaleTicket when the original Node output is already ready, consumes cleanup outputs without producing phases, and publishes Unknown. It never compensates/unlocks. Drain checks actual Node client/read reservations, including reservations surviving Unknown observation consumption.
- `advance` prevalidates its own monotonic clock and all nested numerical budget scopes. Node polls, completion consumption (including retained error-output cleanup), and phase visits have separate bounded scopes. Deferred retained fault outputs rotate fairly. Provider/contract failure stops phase production, keeps cleanup polling and retains rejected original opaque outputs rather than claiming drain. Host-owned Nodes/providers are never implicitly shut down.

Public APIs are in `driver/types.rs`, construction in `driver/construction.rs`, protocol reconstruction in `driver/protocol.rs`, real completion correlation in `driver/polling/consume.rs`, and lifecycle/credits in `driver/outputs.rs`.

## Checks

All commands use `CARGO_TARGET_DIR=target/divide-membership/build`.

- Initial `cargo +stable test --locked --offline --lib transaction`: 12 passed, 0 failed (11 existing application tests plus initial budget control), `driver313-preflight.log`.
- Final `cargo +stable test --locked --offline --lib transaction::driver`: **8 passed, 0 failed**, `driver313-private-final.log`. Seven private controls mutate the driver's retained expected node/ticket/operation/original command/query while consuming **real original opaque Node Applied/quorum read outputs**, then assert no participant phase production and actual ownership drain. This is not scalar receipt fabrication. The eighth control checks invalid nested budget and valid zero top-level progress scopes.
- `cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings`: **zero diagnostics**, `driver313-clippy-final.log`.
- `cargo +stable fmt --all -- --check`: passed, `driver313-fmt-final.log`.
- `cargo +stable test --locked --offline --all-features --lib transaction`: **19 passed, 0 failed** (11 application + 8 driver controls), `driver313-transaction-final.log`.

Focused causes corrected: first library preflight found a sibling method visibility error and unused import; initial strict found complex cohort spelling, large inline Attempt enum and modulo spelling, resolved by a named cohort alias, one boxed immutable command and is_multiple_of. New native private fixture imports/transport variant were corrected from compiler diagnostics. Initial native private run was denied by the filesystem/network sandbox at localhost listener binding; authorized local-native rerun passed. No lint threshold/deadline/safety weakening and no additional dependency.

## Limits / integration

This is a fixed two-participant trusted Rust host-driver protocol, not an arbitrary transaction program or remote proof/certificate intake. Lifetime records are never evicted. Broader Node/network/native acceptance, cold reopen/checkpoint, TCP/QUIC histories and public host-provider substitution are separately owned and qualified by root/native agents; these private controls do not certify a new engine or all possible external adapters. An irrecoverable provider rejection retains its original opaque output, may prevent drain, and is reported through failure; there is no destructive implicit abort path. Host clocks must use the Nodes' established epoch on first attach; subsequent driver backward-time rejection occurs before any mutation. Drop ends the borrow and does no I/O or compensation.

Source frozen after the checks above pending concrete independent review findings. No commits or pushes by this agent. Root owns final Linux/macOS/four-feature-profile qualification and milestone accounting; no P8-complete claim here.
