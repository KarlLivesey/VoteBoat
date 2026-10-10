# Slice227 — native preparing admission with an original unread request

The private counter fixture retains its original record and12s preparing budget.
It observes fresh local log output before reading any reply. The exact preparing
event returns the actual endpoint and unread channel for the original17015
kill/reopen cut. Only a complete exact `ERR NOT_LEADER` closes the refused channel
and selects another leader for that same record. The per-attempt log offset avoids
using old preparing output. Replies are bounded to1024 bytes; partial, positive,
UNKNOWN, conflicting and interrupted results cannot authorize preparing or an
automatic resend. Preparation is not commit evidence. Existing discovery/send
calls have their own limits and can delay observation of the unrenewed outer
deadline; this is not a hard12s bound on every child call.

The subsequent explicit configuration-recovery caller now recognizes the exact
documented authenticated-read interruption under its existing10s budget, keeping
the identical operation and record. Native accepted-record comparison and
deduplication decide the actual result. Production behavior, transport/authentication,
durability, deadlines, provider contracts and inventory are unchanged.

Validation:

- `preparing-all.log`:4 checks pass. Actual TCP/QUIC deliberately address a follower,
  record one explicit refusal, then observe preparing on the actual leader.
  TLS close-notify cancels that preparing target, the original record is explicitly
  resubmitted after learner return, and promotion/checkpoint/reopen/exact data
  retry/clean joins pass. A quorum-lost request's dropped reply returns Truncated
  with zero follower refusals; the native original remains accepted/uncommitted.
  Explicit original retry observes the exact native UNKNOWN, changed original
  gets a retained-record conflict, and durable status remains identical before
  quorum restoration. An actual committed reply is terminal for this preparing
  helper and an explicit replay returns the original duplicate receipt.
- `counter-all.log`: final full counter181 passes in58.57s, including original
  close-notify/deadline/17015 kill/reopen and runner/immutable-plan histories.
  `interruption-default.log`: original TCP interruption1 passes.
  `preparing-default.log`: default native3 passes. Service-spawning feature
  builds are sequential. Unaffected directory/transfer were not repeated locally.
- `preparing-initial-failed.log`:0/3. Two new tests queried their server while
  its pending channel was still owned, so cancellation reached deadline rather
  than channel close. The negative test wrongly expected a locally-durable UNKNOWN
  instead of the actual dropped-reply Truncated. Close/flush the channel first;
  distinguish actual interruption from a subsequent explicit UNKNOWN response.
- `preparing-recovery-interruption.log`:2/1. QUIC recovery receives the documented
  authenticated-read uncertainty; the old explicit configuration caller refused
  that exact line. The focused caller policy change above retains the failed
  evidence and existing record/budget. No general transport-success suppression.
- `strict-final.log`: formatting and all four strict all-target Clippy profiles
  finish at zero. Source hashes and actual commands are retained. Push uses the
  enabled hook and repeats those checks.
- `ci226-terminal.json`, `ci226-{ubuntu,macos}.log`: exact preceding source
  `132648ae8002ab13f0399d899a0fc74c0fac4477`, run38058728922. Ubuntu175/2: initial
  original19701 runner invocations get source3 NOT_LEADER on TCP/QUIC. macOS176/1:
  group7/incarnation3 original add42/5 gets an authentication deadline; subsequent
  status samples show terms46/47. Later suites are unrun. Original17015, forced
  preparation and static TCP/QUIC recovery cases pass on macOS. This does not prove
  the prior225 missed-event cause or fix the separate226 failures. No budget
  diagnostic occurs. Ubuntu's completed job was fetched through its direct job
  endpoint while macOS remained live; the empty download stderr is retained.

Matching227 platform, broader provider/fault/deployment, original250ms P7 and full
P0–P7 remain open. Next work is exact durability-operation attribution, independent
of background CI. Daybreak owns the broader security review; no security corpus
or new provider seam is added by this slice.
