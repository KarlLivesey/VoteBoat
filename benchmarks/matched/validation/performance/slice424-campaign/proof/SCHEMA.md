# Bounded campaign and matched client-completion clock (424)

Base: exact hash map of the current qualified 422+423 merged source, copied before
editing into target/p9/campaign424/source. Parent publishes main independently;
this slice changes only standalone benchmark code/docs and has no upstream edits.

Settings carries campaign_timeout_secs, CLI --campaign-timeout-secs, default 3600,
validated 1..7200. Reports serialize that effective setting and fixed 30s cleanup
budget. A small owned guard wraps existing run/shutdown futures with Tokio timeout,
without pacing or per-client batching changes. Setup/frontier checks keep existing
budgets. No runtime feature/dependency expansion. Guard expiry produces a failed
campaign with phase/budget in the error; partial results never become throughput.

Main always runs finite cleanup after workload success/error/timeout. If both fail,
return a combined error whose source/first cause is the campaign error and whose
Display includes cleanup too. Client JoinSet drops on canceled workload; shutdown
still owns actor cleanup. Cleanup timeout bounds shutdown even when an actor stalls;
process runtime teardown remains separate and external drivers must guard it.
The untouched upstream driver retains its own 400s guard and is unchanged: primary
unified engine runs use the new common finite budget; full 20M untouched slow cells
remain unqualifiable without the separately proposed explicit guard-only variant.

The throughput clock stops after all client joins, before flattening their owned
latency vectors. Preserve every client result/count/sample, then validate exact
client/global original and request counts and all follower frontiers. Aggregation
has bounded input/result capacities from existing settings and checked sums; no
partial success or resetting. Latency sampling inside client loops remains exact.

Checks: parser default/min/max/invalid/missing argument; immediate controlled-zero
budget success and pending-owned JoinSet cancellation without long wall waits;
cleanup invoked after timeout and both errors retained with primary cause;
controlled cleanup timeout; aggregation exact sample preservation and malformed/
missing/overflow counts; existing 18 controls, fmt/strict zero; actual CLI default
and explicit budget small off/on maintenance+both engine originals. No timing or
rate claim. Freeze source-only patch/hashes and raw checks for parent promotion.

Reviewed cancellation ownership: VB shutdown awaits the first mutable handle and
removes it only after completion, preserving slot order. If finite cleanup is
canceled, all remaining handles stay in Cluster for Drop to abort. An actual
Cluster shutdown/cancellation test first completes normal protocol shutdown then
refills its three slots with controlled stalled actors; canceled shutdown retains
those exact handle identities and Drop releases all three. No source helper proof,
normal Raft actor scheduling or runtime feature changes are needed for the fixture.
Controlled-zero Tokio deadlines are real finite deadlines, not a paused clock.
