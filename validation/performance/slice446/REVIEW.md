Independent read-only review of the final446 source found no blocker.

Actor integration changes the pending type, constructor and apply/delivery
signatures. The logicalP admission gate, replacement before duplicate error,
admitted proposal prefix and remove/send/late-error delivery loop remain exact.
The nonzero128-bit ID mapping `(x-1)>>2` plus `(x-1)&3` is lossless, including
u128MAX. Cached indices refer to live indexed blocks; last-slot removal erases
the map entry and invalidates the cache before free-index reuse. Missing removals
do not change pending state. Option replacement/take preserve transferred values
and exact drop ownership.

Four new controls cover an independent8192-action HashMap oracle, full-width and
boundary IDs, duplicate replacement/drop ownership, stale-cache/index reuse and
eight sparse occupancy/churn rounds atP16385. Arena/free backing capacity is
tested separately from initialized length. Sparse storage and initialization can
cost more than the old map; singleton slot/count/map removal overhead remains a
measured tradeoff. No performance gain is inferred from source review.

The reviewer performed no builds, tests or edits. Executed qualification is
recorded separately under main/; paired Linux/Mac measurement is required before
retention. No public provider, persistent format or durability authority changes.

Independent measurement review supports retention as a tradeoff: all six bulk
rate pairs improve, unlike436 small/mixed gains. Mac singleton remains repeatable
debt (all three rate and p99 losses), Linux tails/singleton remain mixed, and
extra sparse storage is a cost. Diagnostic singleton floor headroom does not
establish sustained acceptance or clear that debt. No universal/all-cell gain or
macro closure is claimed. Full raw measured scopes are linked in docs/P9.md.
