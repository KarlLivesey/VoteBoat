# Physical checkpoint functional qualification402

Purpose/macro: finish actual deep cold checkpoint recovery and owning cleanup
on macOS, P8. User defers performance acceptance to P9. Original chapter11 I09
requires real state-boundary publication; chapter12 defines no15-second manual
checkpoint deadline. This slice changes only an integration test watchdog.

Shape: existing NativeWaitStage selects90s only for CheckpointPublished, matching
this same cold history's upper-checkpoint watchdog. Other stages retain15s. No
helper, production API, owner, provider timer, wire format or admission change.
The original applied frontier/base/SnapshotRef/router/worker completion predicate
remains unchanged. A larger observation window alone is not a passing result.

State/ownership: admission remains volatile; exact actual durable checkpoint
coverage must be observed before shutdown. Original manual event and all original
workers retain ownership until completion. Failed398 has only old snapshot900,
actual snapshot ownership with75MB reserved image accounting and pending persistence at elapsed17.6s; this does
not prove deadlock, stationary work or which manual request is currently active.

Failure/cleanup: retain original398 failure and live frozen Mac run. New bounded
run still fails if actual frontier/worker work does not finish. Close/catch-up
stages15s, provider deadlines and actual joins/rebind/removal are unchanged.
No production optimization or performance guarantee. Linux meaningful397
old-predicate negative remains valid because its frontier predicate is identical.

Checks: full logical_nested_native all/default sequential using isolated asserted
release target; fmt/four strict0. Mac complete cold TCP/QUIC under the new source
only after398 terminal handback, preserving actual barrier/retry/recovery/all27
closure. Record actual test elapsed results and required frontier assertions. Do not
invent checkpoint-stage timings or frontiers absent from output; precise cost
measurement stays P9.
