# Slice 421: native ready visit selection

Linux implementation and root acceptance are qualified; publication is the next
step. Mac qualification and paired throughput remain pending. No throughput or
barrier-count improvement is claimed.

`NativeStartup.ready_visit_items` selects the whole original-item visit quantum
in 1..=64. Existing callers explicitly retain16. Validation precedes resource
creation in static, member, multi, discovery, rotation and enrollment paths.
The common runtime assembly applies the selected scalar on create/recover;
all existing derived log/byte/owner floors, Core 64, time limits, application
capability gates, protected queue ordering and class cursor remain unchanged.
This is volatile startup configuration; no disk, wire or receipt format changes.

The actual native benchmark accepts `--ready-items 1|16|32|64`, default 16, on
startup/shared/offered/maintenance/recovery paths and prints requested/effective
visit items, Core proposal cap and visit byte/ms caps. Lane mode rejects the flag
before opening resources. Window 32 still limits queued originals to 32; a selected
quantum 64 is not a promise of 64-command batches.

## Focused evidence

All commands use isolated `CARGO_TARGET_DIR=target/p9/slice421-checks`; executable
feature configurations are sequential. Native socket controls are qualified
outside the restricted localhost sandbox.

- Pure startup-limit control 1/0 checks 1/16/32/64 over groups 1/2/256 and ordinary/
  larger WAL batch limits. Every other derived resource limit equals default 16.
- Final2 affected ready controls: Owner16/0, runtime3/0, startup3/0, log-store1/0.
  New histories cover selected widths across64 queued originals, exact original
  positions/receipts, reopen with a different valid quantum, duplicate/conflicting
  retries, all selected Unknown outcomes after Written failure, ongoing control
  arrivals with cross-group Data/Background service, and early invalid selection
  rejection before occupied listeners, files or worker launch.
- The log-store control executes32 actual ModelIO power-loss cuts: each selected
  count, proposal/commit phase, and append/sync/publication-before/after. Recovered
  whole state equals the phase-appropriate original-validator oracle and the failed
  store/Core fence; this is a durability control, not a runtime-width measurement.
- Final CLI parsing/settings/shared-assembly controls3/0, all-feature library
  ready controls17/0, no-default Owner16/0/runtime3/0, final formatter and strict
  all-target/all-feature zero. The shared-assembly getter checks actual selected
  TimedShard.visit_items and unchanged byte/ms caps. The final ready library
  controls include protected queue boundaries, byte/time/output/count caps,
  later-refused originals retained, duplicate admissions and startup resource floors.

Preserved initial failures are fixture/compile errors, not ignored product failures:
explicit child module paths with #[path] imports; missing new private assembly fields;
wrong test names for Core batch and ProposalPosition; scheduler test provider capacity
smaller than declared max_groups; route helper generic type; two benchmark functions
above the existing100-line lint limit. Focused fixes use existing APIs, correct declared
capacity and coherent dispatch/routes extraction. No lint allowance or threshold change.

Root original51026 completes once with exit0 on the final37-input source map:
398 affected startup/member/runtime/owner/storage/maintenance controls, actual
TCP/QUIC retry/leader replacement/native recovery2 plus maintenance6, and eight
debug functional startup/shared TCP/QUIC runs at quantum32/64. Each CLI verifies
64 measured originals,128 recovered including warmup, retries/worker joins and
the exact selected settings. Formatter, all four strict profiles and standalone
matched formatter/strict checks are zero. [Commands and raw records](root/COMMANDS.md).
These functional runs establish no rate comparison. Frozen Mac qualification and
paired both-host throughput remain required.

`SOURCE.sha256` lists all 37 changed compiled inputs, including every new child
module. `QUALIFICATION.json` maps exact commands and original runner terminals to
raw logs. The later shared getter was added after the native final2 controls;
final CLI/core/library/fmt/strict checks qualify the last source form.
