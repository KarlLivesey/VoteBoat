# Metadata lookup after real leadership changes

The preserved Mac262 family reported an Unavailable metadata read. A separate
frozen diagnostic source reproduces two incorrect fixture assumptions: a moved
authority's lookup returns actual LeadershipChanged, and a wrong-ticket refusal
preserves an original completion whose outcome is also LeadershipChanged. The
original completion is present; the fixture incorrectly requires it to succeed.
Consumed unavailable reads can still own cancellation cleanup in the Node.

The correction uses a private ManifestReadSource wrapper to preserve only the
known leadership-refusal class. Positive fixture lookups re-campaign/reissue at
most four times, after original read slots drain. Inactive/fenced lookups remain
single attempts requiring Unavailable. Wrong-ticket checks still reject the
altered ticket, preserve the exact original object and consume it once; known
leadership-unavailable originals then drain cleanup. Successful originals retain
barrier-request and manifest checks. Other failures remain fatal.

Two controlled native tests enqueue a real Campaign after read admission, for
TCP and QUIC. They require unavailable authority, original-slot cleanup and a
fresh quorum read of the same root manifest. Existing two-move/reopen histories
retain their operation IDs, WAL/checkpoint cuts, inactive/fenced assertions,
cancellation checks and deadlines. No production, protocol, dependency, public
contract or timer changes.

| Scope | Linux passed/failed | macOS arm64 passed/failed |
| --- | ---: | ---: |
| Final all-feature histories and controls |4/0|4/0|
| Final default-feature histories and controls |3/0|3/0|
| Diagnostic original histories |2/0|0/2|
| Added invalid negative-classification assertion |4/0|2/2|

The diagnostic Mac failures explicitly retain LeadershipChanged. The subsequent
added assertion incorrectly requires every inactive/fenced attempt to reach the
application; Mac instead loses leadership first, returning the same required
Unavailable. That extra assertion is removed, preserving the original single-
attempt negative contract. Its failed source and results remain separate in
failed-classification; the final correction is not certified by those results.
The diagnostic wrapper's initial missing generic Debug bound is also retained
as setup evidence, supplying no protocol evidence.

Final formatting and default/all/core/native strict Clippy profiles have zero
diagnostics on both hosts. All662 Rust inputs verify before/after final checks;
the diagnostic uses its separate661-input manifest. Profiles run sequentially.
Native-only/core configurations exclude the TLS-gated metadata fixture and are
checked by strict compilation. Linux stable1.98.1 and Mac stable1.98.0 retain
existing conditions. Raw.sha256 preserves captured bytes; published.sha256 hashes
path-normalized text without trailing whitespace.

This resolves the demonstrated caller assumptions, not complete lifecycle or
platform acceptance. The preserved262 family and new frozen69-history Linux
qualification have separate source identities and results. Broader platform,
combined faults and P7 measurements remain explicit work.
