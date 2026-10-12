# Partial frozen qualification 324: Linux passed, macOS failed

This is explicitly partial evidence. Linux passed all 16 selected stages with
`run=0`, `source_guard=0` and independent audit zero. Mac's original aggregate
failed with `run=101`, `source_guard=0` at the all-feature controlled network
stage. A separate continuation executed only the seven unrun stages and passed
with its own terminal/source guard zero. It did not replay the failed stage or
change any source, timing or assertion. Mac's whole-scope audit remains failed.
**Slice324 is not qualified on both supported platforms.**

| Profile | Activation private/public, Linux and Mac | Logical native, Linux and Mac | Controlled network, Linux / Mac | Nine-owner network, Linux / Mac | Strict Clippy, both |
| --- | ---: | ---: | --- | --- | --- |
| All features | 7 / 4 | 30 | 4 passed / 2 passed, 2 failed | 2 passed / not run | zero |
| Default/TLS | not repeated | 19 | 4 / 4 passed | 1 / 1 passed | zero |
| Core only | 7 / 4 | not applicable | not applicable | not applicable | zero |
| Native only | not repeated | TLS target excluded | TLS target excluded | TLS target excluded | zero |

Linux: 82 selected Rust tests passed. Mac: 78 passed, two failed; two all-feature
nine-owner tests were not executed after Cargo stopped that failed stage.
Both platforms passed formatting, all four full-target strict Clippy profiles,
124-entry inventory validation, 25 partial provider reviews covering 273 named
operations, and 33 metadata negative controls. Partial reviews do not certify
every inventory entry as an interchangeable provider. Unchanged pure send-window
and stripe matrices were deliberately not repeated.

The primary Mac failure is
`controls::real_dispatched_rejected_batch_cancels_exact_original_after_close`:
its fixture's initial child leader/readiness wait timed out at
`tests/logical_voter_network/cluster.rs:170`, with root
`voteboat-logical-nine-child-85674-7`. The next test,
`refusals::real_frame_refuses_truncation_oversize_and_original_receipt_mismatches`,
failed on the poisoned shared history mutex. The original failed log is
[mac/mac-all-logical-network.log](mac/mac-all-logical-network.log).
[PHASE_LOCALIZATION.md](PHASE_LOCALIZATION.md) records the bounded read-only
finding: `Cluster::open` submits the selected child Campaign and waits for
current-term committed/applied leader readiness before claim/session assertions.
This identifies the failed phase, not a particular stalled job or established
load/protocol cause. No unchanged retry, deadline increase or production fix
was used in this evidence.

Activation controls qualify the opt-in replicated application and its public
contracts, not an actual membership/representative activation driver. Full native
30/19 targets include original held-checkpoint close histories: retained original
work progresses through actual completion, binding/compaction and owned cleanup.
Actual native activation, deeper nesting and full P8 completion remain pending.

Exact frozen input closure: 1,047 paths. Manifest SHA256:
`176db6b88f3428d9398cb0e5bfe227d97bd745e056efdbde7891ff0800387932`;
path-list SHA256:
`e1bfc20875036bcf68090a7ea41f4c7661fc92e91e67957990bf4f67feee5c4a`.
[source.sha256](source.sha256) and [source-files.txt](source-files.txt) match
both platforms, along with runner/selector/audit bytes. Root before/after,
local/remote copied, before-run and terminal guards passed; original failed Mac
and continuation guards are separate. [guard-summary.json](guard-summary.json)
records exact checks. Saved scripts and [evidence.sha256](evidence.sha256) pin
all artifacts. [linux-audit.json](linux-audit.json) passes; the saved
[mac-audit.json](mac-audit.json) intentionally fails for the retained original
stage. Raw stages and terminal/environment evidence reside in [linux](linux)
and [mac](mac). Ignored complete evidence is under
`target/divide-membership/qualification324`.

Linux initially used a detached driver that disappeared during its first strict
compile without a terminal record; no Rust tests had run. That incomplete launch
is retained under [interrupted-linux-launch](interrupted-linux-launch). Retained
foreground driver 4171611 / session 72175 then skipped the four completed metadata
stages and finished all uncompleted stages. No Rust test history was duplicated.
Mac original driver 83347 and continuation 89913 exited; original 302 shell 20050,
Cargo 21495 and routed 83714 were inspected live and left untouched. Independent
idle completed 321 Linux and completed 323 Mac caches were copied without live
links; profiles ran sequentially, with process-only FD 8192. Linux Rust 1.98.1
x86_64 and Mac Rust 1.98.0 arm64/Darwin 27 environments are saved. No archives were
downloaded or unzipped.

The existing failing `logical_nested_native` and native-only Mac315b `effect_owner`
catch-up are outside this scope. Later nested fixture changes, current-root work,
new activation drivers, performance tuning, security and full P8 are excluded.
