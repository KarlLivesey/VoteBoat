# Slice212 — transfer observation phase consistency

The unchanged VBTOBS01 decoder now requires local staging before import and
import before activation, with each recorded phase inside the completed target
read prefix. Activation also requires a positive metadata publication index.
That index belongs to another group: values1,10000 and u64::MAX still round-trip
under target prefix101. Parsing does not establish freshness or quorum authority.

The regression uses real target application stage/import/activation records and
an actual deterministic core read barrier. Ten scalar cases plus a missing-import
case require InvalidCommand. Existing truncation/trailing/group-matching checks
remain. Native split observations now encode/decode actual Node-completed reads
before the unchanged operator consumes them across every selected phase.

Executed evidence:

- `before.log`: the initial fixture asked for an activation command before
  applying its import and failed with NotApplied. This did not prove a decoder
  defect; applying the import first corrects that fixture prerequisite.
- `before-order.log`: corrected fixture fails against the original decoder,
  which accepts the missing-stage/imported/activated sequence.
- `core.log`: initial core-only operation target passes12 cases.
- `operation-all.log`: all-feature operation target passes12, including exact
  typed refusals.
- `native-compile.log` and `native-private-module.log`: guessed module paths
  failed to compile. Inspection of the public transfer reexports identifies
  the correct constant path; no production API change was needed.
- `native-phases.log`: all4 TCP/QUIC WAL/checkpoint split phase recovery histories
  pass, including canonical native observation bytes and unchanged read identity,
  configuration, prefix, serving/fencing, retry and recovered outbox checks.
- `fmt.log` and `clippy-all.log`: formatting and all-feature strict Clippy pass.
- `transfer-service.log`: all18 native executable split/merge histories pass
  locally. This does not close the previous-source platform retirement failure.
- `operation-default.log` and `core-final.log`: all12 final operation checks
  pass in default and core-only builds, including exact InvalidCommand assertions.
- `checks.log`: formatting and all four strict Clippy configurations pass with
  zero diagnostics through the enabled pre-push script.

`ci-observed.json` and raw Linux/macOS logs are the terminal operator run
38046741849 on previous source542428d, not validation of this decoder change.
Ubuntu counter158 and directory19 pass; transfer17 pass/1 fail at merge
retirement on explicit Unknown(LeadershipChanged). macOS counter153 pass/5 fail;
its later targets were not run. Platform and functional recovery remain open.

The user will run Daybreak for broad security work. The originally planned
expanded malformed-input/fuzz corpus is deferred; this slice finishes a
reproduced lifecycle consistency bug and makes no security audit or measured
allocation claim. No persistence format, timer, provider seam or consensus rule
changes. The full P0–P7 goal remains active; P8/Windows stay deferred.

Reproduce sequentially in a socket-enabled environment:

```sh
cargo +stable test --locked --offline --no-default-features --test transfer_operation
cargo +stable test --locked --offline --all-features --test transfer_operation
cargo +stable test --locked --offline --all-features --test routed split_resumes
cargo +stable test --locked --offline --all-features --test transfer_service
cargo +stable test --locked --offline --test transfer_operation
sh .githooks/pre-push
```
