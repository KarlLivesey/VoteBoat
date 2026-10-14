# Slice446: four-slot pending reply cohorts

Selected before editing. Immutable435 profiles show insertion self3.40% on Linux
and6.97% on Mac, with result-send9.06% on Linux; contexts overlap. Prior436 client
inline waits,438 reserve and440 key-layout experiments are rejected and remain
unapplied. This selects an algorithm change to pending lookup, not another reserve,
hasher, tuple-layout or client-future experiment. Speedup is unproven until paired
Linux/Mac bulk and singleton measurements.

Private matched-host data shape: map a lossless cohort key to an arena block of
four independent optional reply senders. For positive full128-bit operation IDx,
key=(x-1)>>2,slot=(x-1)&3. Together these retain every bit; no fingerprint, digest
or benchmark-only ID assumption. RandomState remains. A private cached full key
and arena index avoid repeated dictionary lookups within one cohort. Arena free
indices reuse empty blocks; an occupied-slot count remains the logical pending
count. Keep per-original channels, IDs, request/receiver ownership and client
future representation exactly unchanged.

Ownership/transitions: actor exclusively owns the table. Insert replaces and
returns the same old sender on a duplicate, preserving existing error/drop order.
Remove takes precisely one slot; the last removal erases the dictionary entry,
invalidates any matching cache and releases the arena index for reuse. Other
cohorts cannot invalidate a live index. No unsafe code or shared mutable access.
Unknown IDs leave state unchanged. Replies are still delivered in the same loop
order, preserving delivered prefixes before late errors. Dropping the actor drops
every live sender exactly once; client cancellation does not cancel accepted work.

Bounds/failure: existing admission check remains authoritative atP=clients*4+1.
At mostP occupied cohorts exist for arbitrary sparse IDs. ReserveP arena blocks
andP free indices up front, so backing Vec capacity does not grow geometrically
past the declared four-slot shape. Test initialized length and backing capacities
separately; allocator rounding and actual RSS are not inferred. Dense IDs may use
fewer blocks, while sparse memory can exceed the old map. Reuse vacant blocks
before extending initialized arena length. Test maximum sparse occupancy and long churn, with no accumulated empty
cohort/debt. Normal standard-allocation failure behavior stays. No provider,
durability authority, persistent/wire format, generation, restart or timer change.

Checks: independent HashMap oracle over adversarial/nonmonotonic/full128-bit IDs,
boundary IDs1/4/5/u128MAX and upper-bit distinctions; duplicate replacement and
missing removal; interleaved cohorts, cache invalidation/index reuse; exact drop
ownership; bounded sparse/depleted/repeated rotations. Keep all original actor
duplicate/admitted-prefix/FIFO/late-error/channel controls, real snapshot control
and65 matched tests. Format/strict standalone and full enabled pre-push hook zero.
Only actor table integration and private new modules change. Publish checked code,
fresh asserted artifacts on both hosts, then repeated paired20M bulk and1M
singleton runs against immutable435 with matched maintenance/count/timer/outcome
guards. Preserve every regression; reject globally if tradeoffs fail. Pi remains
stopped. Full P9 floors, real durable headroom/fairness/placement/P8 remain required.
