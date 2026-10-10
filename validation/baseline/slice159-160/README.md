# Baseline review159 and automatic metadata discovery160

Base revision `1231153308e955f363a81174a5e4557e4a516a7f`, plus this commit's
changes. Linux local evidence; RPL-1.5. `source-sha256.txt` identifies the checked
adapter and fixture source. These are selected tests, not full P0–P7 acceptance.

## Completed checks

| Command (prefix `cargo +stable`) | Result | Log |
| --- | --- | --- |
| `test --locked --offline --all-targets --no-default-features` | 68 targets, 591 passed, before160 | core-before-adapter.log |
| `test --locked --offline --all-targets --no-default-features --features native` | 71 targets, 882 passed, before160 | native-before-adapter.log |
| `test --locked --offline --all-features --lib --test counter_service` | 64 library +35 service tests passed, after initial159 fixture corrections | fixtures-lib-counter.log |
| `test --locked --offline --all-features --test counter_service client_supplied_configuration_targets` | 2 passed, after latest CI fixture correction | configuration-targets-final.log |
| `test --locked --offline --all-features --test routed automatic_metadata` | 2 passed: TCP/QUIC × WAL/checkpoint, 4 histories | metadata-lookup-final.log |
| `test --locked --offline --all-features --test routed native::automatic_lookup` | 4 original lookup/service histories passed | original-lookup.log |
| `test --locked --offline --all-features --test manifest_mapping --test lookup_discovery` | 5 passed | mapping-and-source-all.log |
| `test --locked --offline --no-default-features --test manifest_mapping --test lookup_discovery` | 3 passed | mapping-and-source-core.log |
| `clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings` | zero diagnostics | lint-all.log |
| `clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings` | zero diagnostics | lint-core.log |
| `doc --locked --offline --no-deps --all-features`, `RUSTDOCFLAGS='-D warnings'` | passed | rustdoc.log |

Formatting (`cargo +stable fmt --all -- --check`) and `git diff --check` pass.
`node validation/check-inventory.mjs` validates 89 records' metadata and paths;
it does not establish conformance. Existing independent checker suites pass:
maintenance5, offered5,
journal-timings3 and serial-gate6, totaling19. They verify the existing
arithmetic/validation fixtures, not new performance or protocol evidence. No
benchmark is rerun here.

## What the new histories exercise

The lookup owns an existing Node through `MappedManifestReadSource`. Public
stateless mappings adapt lifecycle and metadata queries/results without changing
the original accepted read, quorum barrier or rejected completion payload.
`ManifestReadSource` version2 exposes that original result type. Persistent
formats and metadata activation rules are unchanged.

Actual native metadata is read at the initial authority, refused while fenced
or inactive, and read after two transfers through publishing and serving
profiles. Phase helpers reopen the original WAL/checkpoint files. The same
discovery/resolver contract checks current authority/generation, invalidates an
observation, cancels an accepted refresh and drains retained Node read credits.
A deliberately wrong ticket returns the original typed completion, which the
Node can still consume. Pure mapping tests reject historical/control results
and compile a host-defined mapping without native features. Existing lookup
tests retain their stale-binding, deadline and recovery coverage.

## Failures and corrections retained

`metadata-lookup-initial.log`: both new tests initially checked cancellation
credits before the Node processed its queued cancellation. The fixture now
polls through owner cleanup as well as the observable terminal result; no
production cancellation semantics or budgets change.

CI runs [38015395454](https://github.com/KarlLivesey/VoteBoat/actions/runs/38015395454)
and [38015646068](https://github.com/KarlLivesey/VoteBoat/actions/runs/38015646068)
failed service fixtures on legal leadership changes. Post-configuration data
checks now use the existing exact-operation retry helper and automatic data
routing. Configuration writes select a current leader and retry only
`NOT_LEADER` or `UNKNOWN LeadershipChanged` with identical arguments.
Authorization, conflicts, malformed records and deliberate unknown-outcome
checks remain explicit. A first attempted fixture change incorrectly used data
`auto` mode for admin commands and failed parsing; that log is retained, as is
the subsequent compile-time mutable-borrow correction. Final target tests pass.

Both runs also failed the macOS QUIC mailbox fixture because it assumed send
completion implied immediate receive availability. Only positive receives now
retry `WouldBlock` within two seconds. Packet, queue and stale-lease assertions
remain exact. The corrected library suite passes locally; macOS remains unverified
until the new revision actually runs there. Both prior CI lint jobs passed.

## Pending broad sweep

The original `--all-targets --all-features` run remains live, using the binary
compiled before160. `all-features.partial.log` and `pending.json` are a snapshot,
not a successful terminal result. Two earlier full/routed runs were also found
still live and advancing through native histories; they were not restarted or
terminated. Native topology tests serialize on the existing history mutex.
Do not launch another full sweep merely because these observations expire.

Focused results above cover the changed contracts. Full-suite completion,
macOS execution, broader failure coverage and all other gaps in
`docs/BASELINE_ACCEPTANCE.md` remain open. No performance target or full baseline
completion is claimed.
