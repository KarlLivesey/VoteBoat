# Actual-core snapshot fault replay

Linux and macOS each pass374 named executions across the complete Raft,
snapshot and snapshot-worker targets: all110, default110, core-only44 and
native-only110. Formatting and strict all-target Clippy for all four profiles
have zero diagnostics on both hosts. Before/terminal678-input guards verify;
current publication inputs match. The zero-context runtime patch applies over293.

Nine new histories extend the real native WAL/event-effect core replay with
native SnapshotStore/Retention operations and a bounded two-slot SnapshotIo
fault device. VBSIM2 records original node/operation identities and explicit
begin, bounded chunks, seal, publication, abort, compaction and stale-ticket
checks. Old VBSIM1 histories retain their semantics. Recovery drops admissions
and staged bytes, reopens actual native stores and restores a fresh Counter
through the public verified checkpoint API; the independent full-history oracle
only compares the result, including retained dedup outcomes.

Executed histories include process/power loss at five first-publication and
five anchored replacement phases, six failed-seal/lost-publication combinations,
aborted replacement, original retry and quorum read, one actual lagging-follower
snapshot installation with duplicate/reordered delivery and reopened storage,
and four delayed-WAL written/synced completion cuts after real compaction.
Sixteen seeded six-round schedules assert actual seal/publication/compaction,
recovery and stale refusals rather than passing an empty schedule. Exact encoded
and decoded traces produce identical observed core/provider states.

The defective-provider negative lies only about snapshot seal sync. Its reduced
trace retains the original committed operation, native publication, actual WAL
compaction and power loss. Missing anchored data causes verified recovery to
fail before returning a voter. Replacing the false sync with the real barrier
passes the same history. Missing unanchored publication and missing compacted
checkpoint are distinct failure targets; the reducer preserves the latter.

Preflight logs retain a missing delimiter, narrow test-module visibility/type
errors, one map-entry lint and the first incorrect assumption that verified
recovery retained a root without a WAL anchor. Focused fixes preserve the public
contracts. Frozen qualification follows those fixes; preflight is not acceptance.
A Node child-process permission failure during artifact preparation launched no
validation; shell-generated tracked diffs and filesystem-only preparation
succeeded before either host's frozen guard.

No production provider, public API, consensus or persistent format changes.
Local compaction and incoming installation use synchronous public helpers;
new traces cut around their calls and do not qualify every internal async
snapshot completion, physical filesystem interruption or broader combined
membership/lifecycle failure. Existing complete snapshot-worker tests execute,
but do not close that broader requirement. Reference-Raft comparison, fuzz,
reusable provider obligations and the original lifecycle failure remain open.
The independent original Mac285 process is not restarted or modified.
