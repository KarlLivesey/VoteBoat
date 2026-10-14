# Slice444 final schema — pause the test follower's tick too

Written before final fixture editing. Pi causal trace20173 failed in12.11s:
paused slot2 campaigns from Follower/term1/applied1 to Candidate/term2 at frontiers
[11293,11293,1]; slot0 steps down Leader1→Follower2 with four pending receipts.
Terms advance through6 with no frontier progress; originals11293–11296 hit the
10-second diagnostic failure bound, then cleanup succeeds. This establishes the
paused follower's live campaigning disrupted the test. The original RecvError
branch was not reproduced and remains separately retained.

Fixture correction: under cfg(test), Actor.tick returns Ok immediately when the
same receive-pause flag is set, before heartbeat/campaign selection. No production
branch, protocol/election timer/default/queue/capacity/worker/provider/outcome
change. Remove temporary error/campaign/transition logging and observer variables
from final source; all diagnostic source/logs remain in prior sealed archives.
Keep the bounded snapshot-write failure/owned cleanup path, with all original
receipt/frontier/actual snapshot/attribution assertions unchanged.

Ownership/partial failure: the same actor owns Core/state/pending senders and the
same cluster owns joins. Only the explicit lagging-follower test pause suspends its
tick path; resuming the flag re-enables the unchanged campaign logic. No stored
operation is dropped, rewritten or accepted from timeout. The10-second write
budget remains test failure, retaining IDs/result/frontiers/cleanup; production
campaign deadlines and restart behavior stay. No persistent state or migration.

Add one deterministic no-sleep actor control: recover a legitimate single-voter
actor as Follower, force last-contact three seconds overdue, pause and tick,
require unchanged role/term/full log/provider state/application/persistence counts,
then unpause and require the existing campaign advances term and log/application.
The real12000-original/OFF+ON snapshot case remains the three-voter catch-up check.
No broad suite or timing workaround. Only Actor cfg(test) guard, existing snapshot
test failure handling and one existing actor test file change; no new module.

Capture separate final selected hashes/modes/patch/production projection and run
only the new actor control plus exact snapshot test, formatting/strict/source
guards on Linux. Parent reviews then runs scoped Pi and Mac checks, and publishes
with the enabled hook. No performance artifact, benchmark/profile, remote/Git
operation by this agent. Final non-test Actor bytes project exactly to retained435;
existing public/Core/provider/library inputs remain unchanged.
