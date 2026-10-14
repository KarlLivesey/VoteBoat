### Slice431 schema — reuse owned ordinary append input (planned)

430 Linux saved stack data records entry cloning910/8,085samples (11.255%):
core durable completion287 (3.550%), independent pending plan340 (4.205%),
ordinary receive append188 (2.325%), materialization46 (0.569%). These overlap
with broader helper scopes and are not copy-byte/call measurements. Completion
leases and provider submission require the first two independent owners under
current contracts; a local borrow cannot remove them. The next contained change
targets the owned received append, with at most the measured receive context as
a diagnostic ceiling, not an11.255% performance promise. Mac430 paired/profile
work continues separately and neither host's sustained acceptance closes.

Data/API: private receive_append takes the already-owned Message. Public Message,
LogEntry/Vec payloads, effects, providers and stored/wire formats remain unchanged.
Transitions: retain retirement_commit before ordinary authentication and all
existing term, entry, prefix, configuration and matching checks. Find the first
missing/replaced offset, compute matching/commit and build its reply before taking
anything. Only offset0, exact entry capacity==len and Noop/Command payloads with
bytes capacity==len permit moving the whole Vec into Suffix. Payload eligibility
is computed during existing entry validation, with no extra scan. Configuration,
overlapping offset>0 and spare allocation cases keep exact slice.to_vec behavior.
This guard preserves the current cloned update's advertised raw allocation shape
and effect reservation; it does not normalize a formerly inadmissible request.

Ownership/failure: the message exclusively owns its consumed vector; core pending
still makes its independent normalized plan, and the original update still crosses
the same owned LogStore seam. Tickets/barriers/durable effects, storage-failure
fencing and unknown outcomes stay unchanged. No new proof/cache/generation is
introduced. Rewrites retain the same suffix-generation/checkpoint invalidation;
recovery reconstructs exactly the existing serialized records. No reply escapes
before its existing durability dependency. Early errors and fully matching/empty
appends must not take input or fabricate a suffix. Drop releases remaining input.

Acceptance: differential moved-versus-forced-clone flows for arbitrary128-bit IDs,
empty/nonempty commands, tight/spare payload and entry allocations, matching
prefixes, uncommitted replacement and configured payload fallback; pointer controls
prove only the intended transfer and the independent pending owner. Compare exact
updates, errors, committed effects, replies and recovered state across delayed/
wrong completions and storage failure. Existing retirement/authentication,
configuration rollback, core/provider/snapshot and actual TCP/QUIC retry/recovery
histories stay enabled. Root format/four strict plus standalone checks must remain
zero; publish the checked slice, then fresh both-host paired rates/p99 and new
profiles decide whether the guard's work yields an actual gain.


Qualification fixture scope: public unadmitted configuration ingress must return InvalidMessage. The configuration payload fallback control then uses the existing internal membership receive fixture; it does not claim public configuration admission. Modeled unit completion tickets are not disk durability evidence.
