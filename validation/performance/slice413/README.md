# Configured append and commit deltas

Already validated configured/checkpoint histories now use an append/metadata
delta when the complete ballot is unchanged and no configuration, snapshot or
rollback is introduced. Membership, historical ballot origin and prior payload
allocations remain intact. Changed ballots and complex transitions retain full
validation. Nonplain classification is preserved, keeping configured resource
and connection checks active.

[Schema](SCHEMA.md), [review](REVIEW.md) and [focused checks](QUALIFICATION.md)
define the exact ownership and acceptance. No public API, format, durability,
authentication or quorum policy changes. Current connection/resource inspection
still scans configured history; this is not wholly history-independent execution.

Final Linux broad checks: all840/0, core595/0; real TCP/QUIC retry, leader
replacement and file recovery2/0; formatting/four strict Clippy profiles0.
Eleven focused all-feature/eight core controls include exact full-validator
oracles, pending connection/membership equality, final retirement at original
durability, accepted overlays and publication/recovery. Actual modeled power loss
checks partial append/sync failure before-state and synchronized publication
failure after-state, original fencing, new sessions and stale old tickets.

Initial fixture import/recovery assumptions and an invalid broad test-target
command are retained separately. Corrected commands execute the actual targets;
no product validator or lint threshold was weakened. Mac qualification is pending
after its separate411/412 campaign. No measured413 gain is claimed yet.
