# Slice226 — finish preparation before the next maintenance intent

The membership-drain fixture prepares its source using19750 when restart elects
another leader. Previously it returned when the target became Leader, although
the replicated19750 record could still be Pending. Maintenance correctly returns
Busy for a different intent during that interval. The fixture now retains the
original source and reads19750 through the current leader until quorum-confirmed
Completed with the expected configuration/target/store/incarnation. It does not
submit another move after an accepted or uncertain original admission. An
inconclusive result remains failed setup, not evidence of a completed handoff.

The existing15s preparation deadline and production behavior are unchanged.
Leadership-status shares only the three exact existing read-transient replies
with resume; modified/extra/unrelated replies remain terminal. No blanket retry,
new owner/provider, durability change or security work is introduced.

Validation:

- `preparation-all.log`: two real forced-source-change TCP/QUIC checks pass.
  Original19750 Completed quorum evidence precedes an accepted19701 Active drain;
  cancellation, original19700 data retry and clean stop pass.
- `counter-all.log`: full counter177 passes in58.56s, including original killed
  runner/source restart, immutable plan/journal, changed-plan refusal, checkpoint
  and native configuration-cancellation histories. This ran before the final
  explicit leadership-status policy name was added. `counter-all-source.sha256`
  records that intermediate source; do not advertise it as a full final-source run.
- `membership-final-all.log`: final affected membership7 passes, including both
  forced-source-change checks and original TCP/QUIC joint/final/reopen histories.
  `read-policy-final-all.log`: final follower/read-policy4 passes, including real
  original-source refusal/reselection and exact permitted/terminal replies.
  `membership-final-default.log`: default TCP membership5 passes. Feature builds
  that spawn services ran sequentially. Unaffected directory/transfer suites were
  not repeated locally for a counter-fixture-only change.
- `strict-final.log`: formatting and all four strict all-target Clippy profiles
  finish with zero diagnostics before commit/push. The enabled push hook
  repeats them; it is not bypassed. Final source hashes are retained separately.
- `ci225-state.json`, `ci225-{macos,ubuntu}.log`: terminal source
  `41b22371c5c6e8c6469f2056b9688e925e916516`, run38057634479. Ubuntu175 counter/
  22 directory/21 transfer passes. macOS172 counter passes/3 fails; later suites
  are unrun. The changed-plan setup explicitly records19701 Applied/Busy. The
  killed QUIC runner never observes an Active journal; its runner-first log is
  not retrieved, so the identical cause is not proved.17015 misses preparing
  after an unobserved send. No225 budget diagnostic appears in the failure log;
  earlier19701 budget expiry remains unclassified. These are preceding-source
  observations, not226 acceptance.

Matching macOS, broader provider/fault/deployment, the original250ms P7 gate and
full P0–P7 remain open. The next bounded functional check is17015 admission;
independent durability attribution continues while CI runs. Broad security stays
with Daybreak. No completion percentage is inferred from passing selected checks.
