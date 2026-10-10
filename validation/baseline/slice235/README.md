# Slice235 — original replacement-setup handoff recovery

Base f55d8fa41cf6ca6121cf51cde3dfd26c981baadf. Exact original move-leader words
use the existing explicit administration caller and documented retryable
responses. The data-only auto CLI, production APIs and file formats stay unchanged.
Existing10s retry scan plus leader/command bounds are not a new end-to-end15s
wall-clock guarantee. Conflict/Busy/unrelated errors and invalid shapes are terminal.

Actual authenticated unread19750 histories wait for Completed, prove the sampled
source is Follower, then replay without changing operation/configuration/target.
The old one-shot call fails NOT_LEADER for both TCP/QUIC; final replacement5 passes.
Exact terminal receipt is retained through learner replacement, drain, cold
recovery, data retries and worker joins. All-feature drain42/unit24 and default
30/unit23 pass sequentially. Formatting/four strict profiles finish zero;
inventory108 and conformance metadata9 partial reviews/68 operations pass.

The first selected helper was incorrectly data-only: four intended histories
fail its explicit move-leader refusal. That log and the focused administration
caller correction are retained. No production behavior changes or general P4,
platform, performance or security completion is claimed. Source233 Linux's exact
UNKNOWN LeadershipChanged selects this fixture correction; the forced old failure
is NOT_LEADER and does not establish an identical timing/transport cause.
Matching source234 operator run38067409575 is in progress at observation, with
Ubuntu114257755705 and macOS114257755869. CI remains background feedback.
