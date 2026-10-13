# Opt-in elapsed phase attribution qualification

Eleven Linux matched tests pass. The eight earlier controls retain their exact
original-result, prefix, maintenance and snapshot checks; applicable VoteBoat
actor paths now run attribution both off and on. Added controls check disabled
clock/counter behavior, checked interval arithmetic and atomic overflow, and
explicit rejection of unavailable upstream phases. Per-replica labels, fixed
phase count, append/barrier counts and original-item accounting are checked after
owned shutdown. Formatting and strict all-target Clippy exit zero.

Four debug CLI controls use separate one-client/16-server-worker runtimes, eight
logical clients, singleton/four-original requests and attribution off/on. All
128/12,000 originals, 128/3,000 requests and all three exact frontiers pass;
instrumented records preserve every original and expose 12 bounded phases.
These are functional checks, with no quiet performance window or rate claim.

The first strict check found a large temporary wake enum. It was removed; the
original biased select branches now finish their wait interval at branch entry.
No boxing/allocation or lint exemption was added. Raw failure and terminal checks
are retained in qualification; EVIDENCE.json hashes the records.

The default const-generic actor/store path reads no attribution clocks and updates
no interval counters. Existing protocol clocks and diagnostics remain. Attribution
uses fixed actor/store-local counters, finalized after shutdown, including
campaign, warmup and follower drain. Direct Core work, full persistence helpers,
nested store append/barrier, application, original results, send, inclusive
maintenance ticks/snapshot helpers, host wait and actor lifetime remain separately
labeled. Inclusive/nested intervals and concurrent replicas must not be summed
as CPU time or the critical path. Actual diagnostic trials require a frozen
asserted release and a coordinated quiet window; no trials have run yet.

Root's tracked pre-push check also passes with zero diagnostics: formatter,
strict default/all/no-default/native root profiles, and standalone formatter/
strict all-targets. Its actual log is retained in qualification.
