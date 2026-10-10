# Slice199a — focused platform execution preparation

Base revision: `1af1478`. This slice adds no Rust/runtime change.

The completed [older macOS job](https://github.com/KarlLivesey/VoteBoat/actions/runs/38029044043/job/114146478974)
ran at `9e1e612` and reports82 passing counter tests plus one incompatible-profile
refusal failure. `macos-9e1e612-before.log` retains that job's output with ANSI
color codes and trailing whitespace removed. The old test reopened fixed node1; current code instead
reopens the actual acknowledged writer, as shown by88b4719 in
`existing-fixture-fix.txt`. A follower without the learned commit could explain
the old result, but its exact prefix was not recorded. Fresh platform execution
is required; this review is not a macOS pass.

The new Operator recovery workflow independently runs the counter, directory and
transfer executable targets on Ubuntu and macOS. It keeps20-minute bounds,
separate platform outcomes, a concurrency group separate from Platform feedback,
and14-day stdout/stderr artifacts. It neither cancels the older full-suite run
nor makes either workflow a merge gate. The shell invocation was syntax checked;
GitHub execution remains pending at this record's creation.

`local-operators.log` passes13 directory tests in20.73s and5 transfer tests in
69.33s. These include authenticated recursive lookup, offline-child operation,
TCP/QUIC split recovery at all ten phase boundaries and lost fence/publication
replies. The unchanged counter/command source already passed122/16 tests at
1af1478, recorded under slice198; those were not rerun for this workflow change.
Formatting, all four strict Clippy profiles and the105-contract inventory
check pass. Commands and hashes identify the checked scope.

199 remains open until actual Linux/macOS operator jobs complete successfully.
Full-crate platform evidence, separate-machine deployment and the remaining
P0–P7 acceptance/performance obligations are unchanged.
