#474: current durable service on default Btrfs versus NoCoW

Before setup/timing. Purpose: test whether the deployed filesystem policy is a
material part of measured durable latency, before changing native formats or
retuning pipeline machinery. This directly advances P9.2/P9.3; it is not an
engine speedup, an ext4/XFS comparison, or full performance acceptance.

Use the same immutable published455 native executable and its existing exact
source/compiler/root-library/profile/CLI proof for both roles. No rebuild is
needed: production restored469 equals455; this fixture was already qualified.
Native opt3/debug0/assertions/overflow/default LTO/codegen remains distinct from
the framework profile. Preserve compiler/artifact/source/validator identities.

Create two new empty parents under the same target filesystem: default CoW and
NoCoW (`chattr +C` only on the new empty parent). Verify attributes before any
WAL file creation and check resulting WAL/manifest attributes afterward. The
mount is Btrfs compress=zstd:1 on one NVMe. NoCoW also affects compression and filesystem data checksumming, while the
engine WAL CRC/recovery checks stay unchanged. Thus
this comparison intentionally measures the combined policy, not isolated CoW.
No existing data directory or global filesystem/CPU/kernel setting is modified.
No mounted ext4/XFS scratch filesystem is available; do not format or mount a
device or use tmpfs as a disk-backed substitute.

Twelve serial actual-service cases: one group/three replicas, TCP and QUIC,
three adjacent default/NoCoW pairs, rotating order AB/BA/AB by trial, opposite
starting order for the other protocol. All replicas share this one physical
device; record that topology rather than extrapolate to three-machine capacity.
Exactly15000 scheduled offers at500/s,64-original window, ready16,64 warmups,
active2s maintenance, no forced follower pause. Reuse these fixed workload
controls from the completed common-window64 campaign; do not splice old cases
into the new comparison. This isolates filesystem policy at a fixed load; wider
window/offered-rate evidence is the following deliverable. The current fixture
caps windows at64, so256/4096 require a separately checked bounded fixture change.

Finish all builds/checks/copies/publication before timing, guard the immutable
inputs, then grant a quiet host. Collect commands/stdout/stderr/process exits,
whole-command resources, original offer ledger, applied/admitted/refusal counts,
latency distribution, during-horizon versus total/drain throughput, active
checkpoints/catch-up/reclaim, storage/journal counters, exact recovery/retries and
worker cleanup. Keep every failure, regression and tail; never replace a case.
Run original independent CSV/result checks only after timing ends. Counters and
nested elapsed scopes are not additive CPU; logical queue peaks and whole-command
RSS do not establish a continuous memory/debt plateau. Report actual completed
originals/s, p99 and all paired outcomes. No consistency or headroom claim follows
from one offered-rate point.

No production/API/wire/storage-format change is made. WALs are disposable test
states at unique never-reused paths, preserved for recovery evidence. No live
service ownership or accepted operation is abandoned. A failed setup creates no
measurement; preserve it before making the smallest correction. Record terminal
handles and never restart after an observation timeout. All raw artifacts stay
under their original paths and are archived only after actual termination.

Next: checked larger-window fixture and offered-load sweep with unchanged ready
quantum first; then decide a durable publication design against measured causes.
A WAL-only marker that can lose acknowledged-prefix damage detection is outside
the preserved contract. Alternating-slot designs also need explicit latest-slot
corruption, stale-slot fallback, partial publication, restart, cleaning and
migration rules before implementation. Full P9 seven-cell/both-host healthy
margin, durable/headroom/fairness/memory/debt/placement/P8 scope remains active.
