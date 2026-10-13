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
no product validator or lint threshold was weakened. [Mac affected qualification](mac/README.md)
passes532 all-feature/338 core with four strict profiles and1349-input guards.
No measured413 gain is claimed yet.

The existing volatile staging example now accepts stable/joint/checkpoint
histories and append-plus-commit updates. Its five controls verify complete
full-validator final states, unchanged neighbors, exact tickets, new recovery
sessions and rejection of old tickets. [Measurement schema](MEASUREMENT_SCHEMA.md)
defines scope and both-host paired acceptance; no physical durability claim
comes from simulated I/O.
