# Isolated426 qualification

The exact ccee8eca1fbc8e2584f148e4e69ef34a614aa2bc selected export plus the
21-file overlay is frozen. Main source, hooks and remote hosts were untouched.
`SOURCE.json`, `base-source.sha256`, `source.sha256`, `overlay.sha256` and
`PATCH.diff` identify the complete selected inputs and reviewable change.
The selected1415-file inventory includes focused documentation; it is not a
claim that prior benchmark exports used an identical inventory scope.

Original final runner36592 is terminal0 with `CHECK_STAGE=DONE` and
`CHECK_STATUS=0`. `check.sh` records the exact sequential commands and manifest
working directory. All-target Clippy covers all/default/core/native configurations;
it was run directly against the isolated package, not through an outer Git hook.

| Final functional scope | Passed | Failed |
|---|---:|---:|
| All-feature log_store | 19 | 0 |
| All-feature provider_conformance | 31 | 0 |
| All-feature snapshot | 26 | 0 |
| Core provider_conformance | 16 | 0 |
| Core snapshot | 11 | 0 |
| Core native-only log_store target | 0 | 0 |
| Standalone matched harness | 34 | 0 |

Root fmt and all four strict profiles plus standalone fmt/strict are zero.
Snapshot scope includes actual native files, acknowledged-state/retry power-loss,
log/snapshot failure and lost completion controls. Standalone scope retains real
12000-entry checkpoint and lagging-follower snapshot histories.

The first formatter failure was an incorrect child-module path, then the first
compile found a missing test Effect import and String ownership in a fault fixture.
Those are retained in fmt-attempt1-output.txt and affected-all.log. Subsequent
fixture-only failures are retained in affected-all-attempt2.log and
matched-tests-attempt1.log: snapshot session was not an identity refusal, the
memory entry used invalid term0, and a legacy count expected27 phases. Focused
corrections retain production ordering and the exact new28-phase shape.

The new `store_matches_state_exact_nested` is a provider full-Eq elapsed interval.
The old clone phase remains separate. Equality item counts are logical retained
entries of successfully equal states; false/error gives zero, not partial or
physical traversal. `state_to_pin` now begins after comparison. These changed
scopes require fresh measurement. No performance benchmark or speed claim was
made in this qualification.
