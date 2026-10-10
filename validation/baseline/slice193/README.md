# Slice193 — transfer preview

Base revision:75c67dcec441aa44a9c01297f6f81bce48387f21. The source hashes record
the completed slice before commit. RPL-1.5; no dependency or persistent/wire
format change. See [usage and contract](../../../docs/TRANSFER_PREVIEW.md).

The public preview reuses checked transfer intents and scope providers. The
native CLI consumes explicitly configured templates and exact target placement,
with output distinguishing payload bounds from live state or durability facts.
The original native placement predicate and counter policy parser are shared,
not independently reimplemented for the preview.

## Checks actually run

| Log | Result and scope |
| --- | --- |
| focused-all.log |8 placement +13 placement-planning +12 scope +3 preview CLI tests pass. Includes native/host providers, split/merge, schema/budget refusal and forbidden export/import calls. |
| scopes-core.log |11 scope tests pass without default features; one native-storage test is correctly excluded. |
| retained-final.log |1 selected retained-insertion preview test passes;38 other tests filtered. |
| counter-placement.log |5 tests pass, including real TCP/TLS and QUIC generated placement execution/recovery.65 other tests filtered. |
| cli-final.log |3 CLI tests pass again after extracting rendering; no further behavioral change. |
| format.log / clippy-*.log |Format and strict all-target all/default/no-default-feature Clippy pass with zero diagnostics. |
| inventory.log |96 records pass inventory shape/path checks, not behavioral conformance. |

Eight tests are new. Counts above include overlap between feature configurations
and a repeated CLI run; they are not a count of distinct new tests or a full
suite claim. `commands.txt` contains the corresponding commands.

## Failed check and focused correction

`retained-initial-failure.log` retains the first retained-scope failure. The moved
scope supplied by the intent is narrower than the original owned scope, so the
initial report omitted the range the source keeps. The corrected implementation
computes retained ranges from the original manifest and checks that the source
application covers them. The final regression passes without changing execution
or relaxing the test. A test-module path and a too-long render function were
also corrected during compilation/linting; no lint threshold was weakened.

## Limits

The report does not execute a transfer, import live data, reserve capacity,
guarantee pause duration, count retained WAL bytes or establish actual quorum
availability. Typed bounds require honest scope providers. Existing durable
fence/import/publication/activation and retention proofs remain mandatory.
Operator start/status/resumption is the next slice. This is not full P0–P7,
performance, macOS or separate-machine validation.

`background-status.txt` records the original175 cargo/routed processes as live
and CI38025859182 at the base revision as pending. Older cancelled CI does not
constitute a pass. These background jobs were not restarted for this slice.
