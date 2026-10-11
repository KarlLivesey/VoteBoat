# Schema305 connector ownership control

Selected source is frozen. Changes: existing tests/discovery/driven.rs adds the
zero-ConnectUsage/pending-lookup ownership assertions for both canceled and timed
out original requests, before and after close. The next actual host-driver poll
completes that separate lookup lifetime. A small final ownership assertion helper
keeps the existing loop below the unchanged complexity threshold. Existing
PeerDriver disconnected history checks connector_is_drained true/false/true
across attachment, an original held receipt and ordinary close/poll. Inventory
adds only the public inspection operation and its provider ownership description.

Validation (Linux, all features, target/divide-provider/build):
- Exact driven cancellation/timeout test: 1 passed, 0 failed; two branch schedules.
- Existing authenticated reconnect/disconnected-output test: 1 passed, 0 failed.
- Whole-repository all-target/all-feature strict Clippy: exit 0, zero diagnostics.
- Formatting and git diff --check: exit 0, zero diagnostics.
- Logs: discovery.log, peer-getter.log, clippy-all.log and fmt.log.
- Actual hashes: source.sha256.

Preflights: the first exact test filter omitted its driven:: prefix, selected
zero tests and was corrected; it is not counted as passing evidence. Initial
strict Clippy refused the extended existing loop at cognitive complexity 27/25;
clippy-preflight-complexity.log preserves that failure. Extracting only the final
lookup ownership assertions corrected the cause without changing thresholds or
schedules; final focused test and strict check are clean.

Read-only re-review confirms helper requires both zero usage and actual selected
connector_is_drained. Original full binding/deadline equality, shrinking live
owners, nondecreasing supplied time, post-poll timer equality and successful
external process exit remain required. No remaining concrete blocker found.

Scope: deterministic core/host provider schedules and inspection delegation, not
an actual native-discovery-delay/executable pure-close integrated history. Root
owns actual304 before/after service qualification, both platforms and the other
feature profiles. No new providers, source behavior, dependencies, OS resources,
proof of arbitrary closing schedules, or retrospective Mac302 cause claim.
