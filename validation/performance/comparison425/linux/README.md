# Linux425 compact gaps

Original runner43210 exited 0 once. All nine rotating cases preserve 1,048,576
measured originals, 1,024 singleton warmups, 262,144 requests and three frontiers
1,049,601. This uses exact ccee8eca, same fresh executable for both engines,
pinned OpenRaft, one host, one lock, opt3/debug2/assertions/overflow/thin-LTO/one
codegen unit, empty payload, three voters, 4096 clients, batch4 and default
maintenance. Before/after 1409 source inputs, four artifacts, compiler and driver
guards pass. Neither engine nor helper semantics changed for measurement.

| Variant | Median completed PUTS/s | Median request p99 ms |
| --- | ---: | ---: |
| VoteBoat off | 1,196,643 | 16.352 |
| VoteBoat on | 1,118,407 | 19.674 |
| OpenRaft | 2,857,665 | 8.703 |

Every trial and slower result remains in RESULTS.json. Observed on/off differences
are retained; they are not an optimization speedup or a precise causal observer
cost. This short volatile diagnostic establishes no sustained margin or durable
service capacity. The 8,422,500 completed-originals/s minimum remains unmet.

The three enabled leader runs each complete 204 checkpoints. Inclusive compact
elapsed times are 411.958 / 426.019 / 443.975 ms. The state_to_pin gap is
385.324 / 395.458 / 411.073 ms, roughly 93% of whole compaction. This interval
contains full GroupLog comparison, temporary clone destruction, guards and
observer/return overhead; it does not isolate which component dominates.
Clone construction itself is 13.336 / 16.432 / 17.483 ms, for about 4.33M retained
entries revisited per campaign. Pin_to_append is 0.916 / 0.993 / 1.124 ms;
barrier_to_reconcile is 6.918 / 7.183 / 7.704 ms. All three gaps certify exactly
each successful helper and have valid whole-observation maxima. All 27 phases,
four provider scopes, widths/queue bounds and snapshot-aware original accounting
pass. Do not add nested provider intervals or label elapsed sums CPU time.

Next: review an exact borrowed durable-state comparison seam. Preserve full
content/header/OperationId equality and default provider behavior, and remove
materialization/temporary cleanup where the provider already owns verified state.
No digest/metadata-only shortcut, helper duplication or durability change is
justified. Paired measurements must determine the actual benefit.

No local build, copy, check or competing benchmark ran during timing. The Mac
agent held its local collector until this runner was actually terminal. Driver
adaptation changes only required source/new-module guards and three new field
checks; workload, rotation, budgets and timer boundaries remain424's.
[Fresh build proof](../../matched425-linux/BUILD.json).
