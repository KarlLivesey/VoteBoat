# Ready410 macOS functional qualification

Root confirmed current production source/tests/examples/Cargo/clippy frozen
before this exact byte copy. Copy includes new runtime readiness/batching modules
and all new client_admission ready_batch tests. Parent commit and working-tree
status are recorded separately. Every archived source byte matches the frozen
root after copy, and every input is guarded before/after Mac qualification.

Use entirely distinct /tmp/voteboat-p9-ready410/build. No old409/408/407 build
directory or source tree is selected; this prevents stale shared-cache artifacts.
Feature configurations and executable-bearing test suites run sequentially.
The first all-feature compile emits Cargo JSON; require non-fresh voteboat
library compilation from this snapshot before the test suites.

Acceptance: formatting plus strict all-target Clippy default/all/core/native;
all-feature and core library/runtime/effect-owner/admission/worker/application/
provider/Raft controls. Preserve actual test names/counts, raw logs, commands,
exits, complete source guards and terminal status. No timed benchmark or
performance claim comes from functional test elapsed times. Root owns tracked
documentation/publication and all source changes.
