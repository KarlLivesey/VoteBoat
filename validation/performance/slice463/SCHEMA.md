# Slice463 — one planned ordered ready transition

Before-edit schema. Native46112cases did not establish a consistent reduction
in barriers/original and had two p99 losses per protocol;462 restores455. This
new producer plans the complete bounded ordered intake before any Core mutation
or dequeue, rather than replacing an already-created pending request or tuning
461 eligibility. Its benefit and actual opportunity frequency remain unproven.

Data/API: add startup-only BatchLimits.max_ready_reply_inputs, default2,0 disables,
validated bounded<=64. Existing proposal/replication/repair envelopes and runtime
visit16 defaults stay unchanged. Add default-false ProposalAdmission capability
supports_ready_commit_batch with exact context/error/receipt-bound equivalence
before versus after applying the old ordinary committed prefix and removing its
pending reservations; Counter opts in. Wrappers/logical/context-sensitive hosts
keep serialization unless independently opted in. Private Core
propose_after_ready_replies(&[Message], Vec<Proposal>) validates the whole intake,
plans one ordinary LogUpdate and creates Pending only after every fallible check.
Err preserves all Core state and the original queued inputs. No public step,
leased-effect mutation, temporary pending removal or persistent deferral mode.

Core intent: no pending dependency, leader/local voter/plain stable membership,
no transfer/read/snapshot/checkpoint/readiness dependency; all selected controls
are unique-peer exact primary successful current-term Appended replies matching
the full old durable tip. Reuse authenticated_message_source and original request
scope, term/configuration/context/range validation. First reply alone must establish
the current-term full-tip quorum under the original validated policy. Additional
replies may retire only their exact old-tip requests. Commit notifications they
would owe after the old commit are explicitly folded into the same Durable-gated
LeaderAppend continuation; no Send or result escapes before that completion.
Any other control or an invalid/stale/alias input is a boundary: leave it queued
and run the ordinary first input. Preflight two legacy revision increments and
conservative future request-context room; exhausted counters fall back rather
than make a previously unproposable original durable. Plan-copy/conversion errors
precede the sole pending/progress/request mutation. Do not clone whole Core/log.

Runtime/ownership: a non-consuming scalar selector simulates the actual
[Control,Control,Data,Background] cursor and visit usage, selecting only already-
ready reply controls until ordinary Data is actually next. Count every consumed
input in advance/output/visit bytes/items; retain existing protected-tail and
background vetoes. No fishing past boundaries or waiting for new data. Reserve
all temporary control/command copies, quorum planning, observations, output and
receive-source retention wholly against bulk credits before application checks
or Core planning. Keep an independently available original-control reservation;
on any refusal refund/downgrade and execute the unchanged first input immediately.
Partial valid proposal prefixes may join; rejected data remains queued.

Private joined output contains ordered control observations (actual source and
optional tracked admission) plus the existing proposal Stepped with its first
original and batch members. Public Stepped/single-step APIs remain unchanged.
Owner emits one OwnerStep for every original/control in queue order under one
visit and retains ALL consumed source peers through leasing, worker completion
and effect retirement. Queued/leased/active obsolete-session inspection sees their
union; it grants no authentication or quorum authority. Preserve separate full128
operation invocations and admission/position mapping. Preallocate expanded owner
output and active source buffers before Core commit. TimedShard allows this path
only when ordinary managed leader timer refresh is a proven no-op (same heartbeat
kind/reset, not closed/failed); otherwise serialize. Selected replies have no live
or stale timer token; normal timer/control priority remains unchanged.

Durability/restart: one existing atomic update stores old quorum commit plus new
uncommitted suffix; exact Written may send only the new ordinary suffix under the
full-old-tip quorum proof, advertising OLD durable commit. Local progress, old
application/results wait for exact Durable; new originals need later durable
quorum/application. Existing peer/context/alias guards, failed-barrier fencing,
unknown outcomes and store/session restart rejection remain. No WAL/wire format,
provider token, manifest/barrier, checkpoint cadence or quantum change.

Evidence: add cheap benchmark-only counters derived from existing returned
OwnerSteps grouped by visit (joined transitions/reply inputs/originals), with no
new provider metrics seam. Add --ready-replies0/2 to select only the new startup
producer bound and report it. Validate counters and disabled-zero controls; keep
optional heavyweight batch diagnostics separate. Fresh same-source asserted
native artifact comparisons with producer disabled/enabled isolate this change;
unchanged old458 production artifact can remain a separate reference, never
splice medians. Same timer/payload/window/ready16/maintenance/original outcomes.
Report actual rates/p99/refusals and physical barrier counts alongside observed
joined counts; no hit-rate/causal claim from aggregate storage alone.

Acceptance: actual three/five-voter serialized-vs-joined histories, arbitrary
validated quorum policy, exact/stale/altered/alias/header/term/configuration
boundaries, future revision/context exhaustion, no mutation on failed planning;
Written/old-commit/late-ticket/new-tail quorum controls; native modeled power loss
and uncertain publication; runtime two-source retention before/after leases,
input order/cursor/timer/output and bulk-control/refusal/partial-prefix controls;
separate repeated IDs and original receipts; default-false/wrapper fallback.
All affected Core/runtime/client/provider tests and actual TCP/QUIC recovery,
standalone harness, formatting/four strict root/standalone profiles. Commit/push
checked slices, fresh guarded artifact and paired both-host rate/tail/refusal/
barrier/join-count measurements before retaining a gain. Full P9 all seven
framework cells, sustained memory/debt/headroom/fairness/placement/P8 remain
active; no P10 or Mac job restart. No unused-helper milestone is completion.

Implementation clarification: the benchmark switch is accepted only by shared
assembly (explicit GROUPS), which configures each recovered Core before its first
event. Startup convenience assembly and --lanes retain defaults and reject the
explicit switch rather than report an uninstalled value. joined.csv contains
warmup/measurement rows for each physical replica; summary counters cover only
the measurement polls. Counts observe complete same-visit control-then-proposal
outputs, not eligible opportunities, fallback causes or CPU time. The checker
requires the file for new marked outputs, checks disabled-zero controls and
summary/phase arithmetic; historical outputs without this marker keep their
original schema and claims. The native profile remains unchanged.

Joined originals count proposed positions, not completed writes or durable votes;
the original outcome ledger remains authoritative for throughput and latency.
