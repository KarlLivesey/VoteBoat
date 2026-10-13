# Minimum bounded campaign guard and timing-boundary proposal

Plan only; no source changes. Keep the full original-count workload, reference
engine/configuration, default maintenance, all original checks and final follower
frontiers. Timeout settings guard stalled/broken measurements rather than define
throughput acceptance or change client pacing.

Data/API: standalone Settings gains campaign_timeout_secs, validated in 1..7200;
proposed default 3600 for both unified engines/all cells. Record the effective
budget in every successful report and failed campaign metadata. No budget derives
from a desired performance result. Runtime topology, batching, provider limits,
client work and the monotonic latency clock remain unchanged.

State sequence: untimed setup under existing setup checks; bounded warmup+measured
client completion+untimed follower verification/latency aggregation; then owned
cleanup. Capture throughput elapsed immediately after all client results arrive,
before flattening/sorting latency arrays, matching the pinned driver's boundary.
Retain every array and exact actual count for checked post-clock aggregation.
There is no partial success on timeout, dropped original, silent shorter workload,
retry, count reset or checkpoint suppression.

Failure/ownership: campaign timeout drops the workload's JoinSet and initiates
existing owner shutdown. Give cleanup a separate finite 30s guard and retain its
failure alongside the original campaign outcome. A runner hard-watchdog should
cover setup+campaign+drain+cleanup with explicit recorded allowance; it cannot be
shorter than the internal budget. Preserve raw exit/stderr/partial diagnostic
records; incomplete/timed-out cells are excluded from throughput ratios and fail
full-curve qualification. Do not restart a live campaign on a tool observation
timeout.

Untouched-reference limitation: its internal 400s constant prevents full 20M slow
singleton qualification regardless of external watchdog. Retain the original
untouched artifact and existing raw evidence. To run full 20M, prepare a separately
named guard-extended driver derived from the pinned source with an exact
single-constant timeout diff (400→3600), unchanged engine/library/default Config,
fresh asserted artifact proof and clearly reported provenance. Call this a
pinned-engine guard-extended driver, not untouched source. If untouched driver
source is mandatory, its slow 20M cell cannot finish; bounded shorter cells are
separate diagnostics and cannot silently replace the required full curve.
No payload, responder, provider, quorum, snapshot or scheduling change accompanies
this guard exception.

Acceptance: validate finite bounds/default/reporting with no wall-clock wait;
actual paused/controlled test-clock timeout cancels clients and preserves owned
cleanup/error precedence; an immediately successful small campaign gives every
original/request/frontier and unchanged per-request samples; actual off/on
maintenance checks remain; compare driver source except exact guard constant.
Fmt/strict zero, exact frozen source/artifact/runtime identities and quiet host
before any eventual timing. Root reviews and authorizes the implementation first.
