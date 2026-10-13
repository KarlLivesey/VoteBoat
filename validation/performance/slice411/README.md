# Written ordinary append overlap

An exact Written ticket can start bounded follower appends while the leader's
local barrier runs. Local matching, commit, application and original outcomes
still wait for the original durable gates. One old empty heartbeat collector can
remain as an exact bounded alias; live bulk and snapshot requests are preserved.

The physical owner reserves additional output before selecting overlap. Tight
space uses the original serialized path. Queued/leased early sends survive local
terminal completion under their original visit and lease, with retained bytes
charged. Public raw worker helpers and `Raft::admitted` retain their semantics.
No wire/disk format, authentication, quorum policy or durable token changes.

Current scope: established leader, committed current-term plain durable tip,
unchanged hard state/ballot/generation, ordinary Command append and caught-up
peers. Configuration/snapshot/transfer/logical and other ineligible paths retain
serialization; this does not establish performance for all P8 compositions.

[Core schema](CORE_SCHEMA.md), [owner schema](OWNER_SCHEMA.md) and
[measurement plan](MEASUREMENT_PLAN.md) define the dependency and acceptance.
The independently developed [owned-store helper412](../slice412/README.md) shares
the final qualification source; the two optimizations have different scope.

Linux final-source affected checks: all-feature716/0, core-only547/0, formatting
and four strict Clippy profiles0. Four owner controls prove actual follower
persistence before the local barrier, no early application, held-send terminal
handling, failure/fencing and budget fallback. Core all5/core4 controls cover
exact tickets, aliases/currentness and serialization boundaries. Existing broad
library, Raft, owner, worker, provider and application controls also pass.

The first constrained-space fixture incorrectly assumed a larger inspector
bound; its corrected setup reserves the bulk ceiling minus16 bytes explicitly.
The original3/1 and final4/0 records are retained. The sandbox's first broad run
refused21 loopback socket bindings before fixtures ran; the same frozen source
passes with loopback available. Neither refusal is hidden by skipping a test.

Final real TCP/QUIC service retry, leader replacement and native-file recovery
pass2/0 (11.71s). [Mac qualification](mac/README.md) passes534/454 on exact1345
inputs, with formatting/four strict profiles and source/compiler guards clean.
No timed gain or minimum-floor comparison is claimed from functional checks.
