# Slice223 — operator controls after shutdown admission closes

The counter executable used to keep advancing its operator drivers after the
Node entered Quiescing. A quit queues leadership cancellation while Running,
but the transfer can still be visible until the owner executes that event.
The next driver tick attempted another control against closed admission and
terminated the executable with `Closed`.

The existing `advance_leadership` now consumes accepted client/read outputs
first, then advances operator/drain drivers only while the Node is Running.
Node polling continues to drain admitted work. `finish_service` retains drain,
credential and peer publication workers and checks their actual joined results
before releasing the native Node/directory owner. Node's public control admission
still refuses new work with `Closed`; errors are not globally ignored. No public
seam, durable record, synchronization, Raft timer or caller deadline changes.

Native binary-unit tests create three actual service nodes using reserved local
TCP/UDP endpoints, real files and the existing TLS fixture. They hold the target
and source owner step across cancellation admission and begin_shutdown, exercise
the actual executable helper, and require closed admission, retained transfer,
queued cancellation, complete drain, worker joins and released endpoints. The
final tests additionally require the source term to remain unchanged so that an
election cannot satisfy cancellation. Explicit fixture timers isolate that held
step; production/default timers are unchanged.

Local validation:

- `operator-before.log`: both TCP/QUIC histories fail with exact `Err("Closed")`
  against production sourcebase `a12d4124cb5671fed38fa614b10e384fc31c5e22`, after
  actual resource drain/join/release. Its initial unused fixture import warning
  is retained; the import was removed from the final source.
- `operator-after.log`: both histories pass after the admission guard.
- `counter-unit-all.log`:40 pass before the additional same-term assertion.
  `operator-final.log` and final-source `operator-final-source.log`:2 pass each
  with that assertion. `counter-unit-default.log`:39 pass on the final source.
- `counter-all.log`:all171 actual TCP/QUIC counter integration histories pass,
  including original membership-drain cancellation/reopen, leader loss, pending
  intents, checkpoint recovery, exact retries and failed drain publication.
- `membership-drain-default.log`:4 pass, including changed/omitted original-plan
  recovery and clean stop. Feature binaries were rebuilt only after the preceding
  service tests had terminated.
- `clippy-all-final-fixture.log`:the stronger assertion initially made the test
  function105 lines. Naming the already-existing source owner removes repeated
  field chains; `clippy-all-corrected.log` passes with zero diagnostics. No lint
  suppression or threshold change. The failed log remains separate.
- `strict-final.log`:formatting and strict all-target default/all-feature/core-only/
  native-only Clippy all pass with zero diagnostics through the unchanged hook.
  `source.sha256` identifies final source and retained artifacts;
  `source-check.log` records their verification.

Feature builds and live service targets ran sequentially. This change affects
only counter executable scheduling; unrelated directory/transfer targets are
not repeated locally. No full local repository test or macOS pass is claimed.

Preceding-source platform evidence:
[run38055650899](https://github.com/KarlLivesey/VoteBoat/actions/runs/38055650899)
is terminal at sourcebase `a12d4124cb5671fed38fa614b10e384fc31c5e22`.
`ci222-status.json`, `ubuntu222-job.log` and `macos222-job.log` retain actual results.
Ubuntu passes171 counter/22 directory/21 transfer. macOS passes169 counter and
fails2: original19701 QUIC runner budget expiry and original-source96100 resume
returning NOT_LEADER after that node becomes a follower. Later macOS targets are
unrun. The prior221 shutdown failure passes on this unchanged production source
in this run; that alone proves neither a cause nor acceptance of this correction.

The native held-step regressions establish the reproduced shutdown cause and
local correction, not every earlier Closed failure. Matching macOS confirmation,
remaining runner/source boundaries, separate-host deployment, broader provider/
fault coverage and the original P7 gate remain open. Full P0–P7 stays active;
broad security stays with the user's Daybreak run.
