# Operator authority and phase-cut corrections

Base: `1beab3f`. RPL-1.5; Linux local execution. Final source content is captured
in `source.sha256`; it excludes documentation and validation outputs.

The exact configuration LeadershipChanged response incorrectly terminated both
foreground drain variants. It now requests source observation, retaining the
original operation, source plan and request/time budgets. Other unknown responses
and interrupted connections remain terminal with their original identities.

The group membership fixture could retry a phase-advancing command into Final
while expecting to capture Joint. Its original unread command is now submitted
once and all replicas must prove Joint before the wait is disconnected. The
offline Joint checkpoint/WAL checks remain. Grouped leadership test callers
reselect the leader after exact known authority changes; data callers explicitly
retry the same ID and payload. The ordinary CLI still stops uncertain writes.
The previously empty assignment assertion now includes process and service
diagnostics without accepting additional errors.

Actual validation:

| Command / log | Outcome |
| --- | --- |
| `cargo +stable test --locked --offline --all-features --bin voteboat-counter configuration_busy_reobserves_but_does_not_mask_other_rejections -- --nocapture` / `classifier-before.log` | Expected failure before production fix: exact LeadershipChanged returned an error. |
| `cargo +stable test --locked --offline --all-features --bin voteboat-counter drain_runner:: -- --nocapture` / `runner-unit.log` | 7 pass after fix. |
| `cargo +stable test --locked --offline --all-features --test counter_service -- --nocapture` / `counter-service.log` | 122 pass,54.35s, before final grouped caller/diagnostic edits. |
| `cargo +stable test --locked --offline --all-features --test counter_service group -- --nocapture` / `groups-final.log` | 24 pass,10.36s, final grouped caller changes. |
| `cargo +stable test --locked --offline --all-features --test counter_service assignments:: -- --nocapture` / `assignments-final.log` | 3 pass,9.23s, final assignment diagnostics. |
| `.githooks/pre-push` / `pre-push-checks.log` | Formatting and default/all/core/native-only strict Clippy pass with zero diagnostics. |
| `git diff --check` | Pass. |

An initial filter used `--exact` without the module-qualified name and selected
zero tests. It was immediately corrected; it is not counted as verification.

`ubuntu-job.log` is the completed job114159916794 from run38033786328 at01d5190,
with ANSI escapes and trailing whitespace removed. It predates these fixes.
It records122 counter passes,12 directory passes and one directory failure:
`recursive_route_enforces_root_observation_floors` gets an authentication deadline
error instead of the expected recursive lookup exhaustion context. No change
to this failure is claimed. The same run's macOS job114159916607 remains in
progress at the last observation. Earlier eight macOS failures remain recorded
under slice199c. No complete platform or full P0–P7 acceptance is inferred.
