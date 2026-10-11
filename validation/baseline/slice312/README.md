# Replicated transaction applications312 — terminal Linux/macOS

This slice implements bounded coordinator/participant applications for the first
P8 two-group account transfer. It provides checked business intake, retained
phase state, locks, receipts, admission, reads and checkpoint recovery through
existing application traits. The networked driver and actual foreign-group Node
provenance remain313; applications alone are not a usable distributed transfer.
Logical voters and complete P8 qualification remain outstanding. This goal stops
at qualified functional P8, before P9 performance and P10 security.

## Actual qualification

The frozen730 compilation/test inputs match the root and both hosts before and
after execution. Linux uses the isolated divide-membership build; Mac uses the
idle diagnostic303 build, separate from the still-live original302 sweep.
The retained script runs configurations sequentially: all features, default,
no default features, and native only.

| Check | Linux | macOS |
| --- | --- | --- |
| Transaction application unit target, each profile | 11 passed / 0 failed | 11 passed / 0 failed |
| Public downstream application target, each profile | 6 passed / 0 failed | 6 passed / 0 failed |
| Formatting | Zero diagnostics | Zero diagnostics |
| Strict all-target Clippy, each profile | Zero diagnostics | Zero diagnostics |
| Before/terminal input guards | 730 match | 730 match |
| Script terminal | 0 | 0 |

The68 executions per host repeat17 tests across four configurations; they are
not68 distinct tests or a complete platform sweep. The catalogue checker also
passes. No lint level, threshold, consensus rule or dependency changed.

## Implemented contracts

One fixed coordinator and two canonical participant group incarnations bind an
immutable spec and ten destination-scoped phase operation IDs. Checked limits
bound lifetime transaction/account retention. Coordinator Start reserves all ten
IDs against retained and pending specs. Participants provision the coordinator's
whole transaction capacity and fixed accounts; the driver must enforce that
cross-application preflight before Start. Records are never evicted, preserving
terminal progress and original-content retries at capacity.

Prepare locks an account and retains planned arithmetic without balance change.
Commit applies once and retains the lock; Abort from an absent/refused participant
retains a tombstone. Release removes only this transaction's lock after the
coordinator's AppliedAll assertion. Durable business refusal differs from
pre-admission errors. Timeout/cancellation cannot choose a decision. Current
receipt position and canonical first phase position remain separate on retry.

Apply publishes a whole contiguous batch only after complete success. Bounded
reads account for the one fixed boxed status allocation; locked accounts return
Locked rather than an intermediate balance. Checkpoints retain exact role,
profile, limits, opening accounts, balances, locks and first-phase history.
Restore reconstructs local retained commands in original position order and
compares the complete state before publication. Safe voter membership changes
remain available; account ownership is fixed in this profile.

Selected tests cover commit/retry/reverse direction, funds/overflow/contention
refusal, absent Abort/late Prepare, another owner's retained lock, changed original
assertions, pending capacity/destination aliases, malformed atomic batches,
checkpoint truncation and semantic corruption, active locks plus committed suffix
and later transfers. The six downstream tests use only public application APIs
and existing traits; unit tests construct trusted internal phase assertions.

Initial compilation found a private codec re-export mismatch, fixed locally.
Initial strict Clippy found a large inline status enum; one fixed box with exact
nested accounting fixes the cause without changing persisted formats. Those
failed outputs and final checks remain here. The independent read-only
[application review](APPLICATION_REVIEW.md) found no blocker within this scope.
[Agent evidence](APPLICATION_EVIDENCE.md) distinguishes its own checks from the
integrated matrix. [Driver schema](PLANNED_DRIVER_SCHEMA.md) is planned313 work,
not implemented evidence.

Internal phase facts validate shape and immutable binding but are trusted host
assertions, not authenticated foreign certificates. Local checkpoint replay does
not prove their foreign origin. Original consumed Node Applied completions or
fresh quorum-backed status, actual network coordination, lost-observation resume,
partitions/leader changes and native cleanup remain required313 checks. This slice
makes no general transaction-program, global read, liveness or performance claim.

Raw outputs, script and frozen digests are retained here. retained.sha256 hashes
this evidence archive; source.sha256 identifies its compilation/test inputs.
