# Delayed native persistence completion schedules

Extend the existing actual-core replay driver, using the native WAL model and
public append/admitted/barrier/complete contracts. Explicit Delay, Sync, Complete
and Stale actions separate an accepted write, a successful real store barrier
and delivery of its returned completion. No DurableLog success is manufactured.
Each node owns at most one held admission and one prior actual completion.

Four new checks cover:

- Vote requests remain withheld after admission and sync until exact completion
  delivery; a premature Complete attempt releases nothing.
- Power loss before sync discards the vote; after sync, recovery preserves it
  even if the old core never saw completion. The real old-session token is
  rejected against a fresh pending dependency, without changing observed state.
- Either follower can complete first. An admitted or merely synced follower
  supplies no acknowledgement; only actual completion enables the leader's
  durable quorum. The original command8801/value8 survives both quorum members'
  cold recovery while the other follower's unsynced write is lost.
- Sixteen additional259-action schedules replay with identical logical state,
  actual core dependency presence, held stages and refusal counts. Retained
  dependencies never exceed three. Original263 schedules and reducer still pass.

| Scope | Linux passed/failed | macOS arm64 passed/failed |
| --- | ---: | ---: |
| All replay/codec/reducer checks |9/0|9/0|
| Complete Raft target, all features |50/0|50/0|
| Complete Raft target, no default features |29/0|29/0|
| Complete Raft target, native only |50/0|50/0|
| Complete Raft target, default |50/0|50/0|

Formatting and all four strict Clippy profiles have zero diagnostics on both
hosts. All658 Rust inputs verify before/after each final full-target run. Profiles
execute sequentially; independent lifecycle checkouts remain immutable.
The separate initial passing run is preserved under initial/. Final review
permits Busy only for an actually held admission, accepts WrongIdentity only
for received stale traffic, and adds actual dependency presence to comparisons.
Unexpected core/storage errors remain loud. Linux stable1.98.1 and Mac
stable1.98.0 retain their existing conditions. No production, dependency, public
contract or protocol changes.

This is ordinary-majority native WAL-model evidence with one pending update per
node. Barriers themselves run synchronously; completion delivery is independently
ordered across nodes. Busy delivery can drop a message; no liveness claim is
made. Process-crash versus power-loss, failed barriers, snapshots, timer-runtime
actions, within-store concurrent requests, reference comparison and broader
composed fault schedules remain outside this slice. Raw.sha256 hashes captured
input bytes; published.sha256 hashes path-normalized text without trailing space.
