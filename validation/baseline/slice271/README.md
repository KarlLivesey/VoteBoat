# Slice 271: exact staged snapshot payload

A supplied SnapshotCodec previously could change decoded application bytes while
preserving metadata and length, then receive a sealed/published receipt. The
retained negative publishes `[7, 8, 9]` and loads `[6, 8, 9]`. Sealing now compares
the decoded payload with the exact original staged byte range before footer
append, sync or root publication. No persistent/wire format, capability, API,
quorum, worker, generation or application semantics change.

Three new regressions cover changed/truncated decoding before sync/publication,
process restart and simulated power loss, old root/pin preservation, exact Counter
checkpoint and original-ID deduplication, stale admissions, real-file fenced
close/reopen and successful transparent host-codec publication. These are finite
round-trip checks, not full replacement-decoder certification or security audit.
The before manifest is an after-test observation, not a before-run source guard;
its codec source and module patch reconstruct the negative from1d7d508.

Both Linux and macOS pass complete member_recovery, membership,
provider_conformance, snapshot and snapshot_worker targets sequentially in
all/default/core/native profiles:142/141/59/140 passes (482 per host), zero
failures. Formatting and all four strict Clippy profiles have zero diagnostics;
all664 inputs verify before and after each matrix. Metadata integrity and its
test pass, retaining partial review scope.

The first Linux sandbox run's40/2 member-recovery result is retained separately:
TCP/UDP bind calls returned EPERM. The same unchanged source passes with authorized
localhost socket access; no assertions or budgets were changed to obtain that
result. Frozen269 and old262/251 lifecycle jobs remain separate sources and
acceptance obligations. Full platform/P0–P7 completion remains open.

`published.sha256` checks normalized artifacts; `raw.sha256` records original bytes.
