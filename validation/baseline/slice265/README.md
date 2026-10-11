# Process crash, power loss and failed sync

Extend the existing actual-core/native WAL-model schedule with ProcessCrash,
FailSync and an exact eight-byte Counter command. Process crash retains device
bytes; power loss resets them to the synchronized image. Recovery adopts and
synchronizes a valid complete retained frame before returning a fresh session.
That permits unacknowledged work to survive without inventing an earlier receipt.

Four new checks cover:

- Vote frames across both crash modes before and after sync. Neither admission
  nor sync alone releases the old core's vote requests. Fresh sessions advance
  exactly once; recovered term/vote matches the retained device image.
- A real failed barrier returns Uncertain("failed sync") and fences the native
  store. Calling the existing storage_failed contract fences the core, clears
  its dependency and prevents vote effects. Further events receive Fenced.
- Original Counter operation9911/delta9 resolves after either crash mode. Process
  recovery retains its first log entry; power loss removes it. Reproposal and a
  full three-replica cold restart retain value9. Receipts show one application
  effect despite duplicate IDs in the consensus log, including another retry.
- A saved [37-action schedule](composed.vbsim) with two real failed syncs,
  process crashes, re-election, original-ID retries, power loss and stale
  completion delivery reproduces identical observed states.

| Scope | Linux passed/failed | macOS arm64 passed/failed |
| --- | ---: | ---: |
| Replay/codec/reducer checks |13/0|13/0|
| Complete Raft target, all features |54/0|54/0|
| Complete Raft target, no default features |29/0|29/0|
| Complete Raft target, native only |54/0|54/0|
| Complete Raft target, default |54/0|54/0|

Formatting and default/all/core/native strict Clippy have zero diagnostics on
both hosts. All659 Rust inputs verify before/after the full-target runs. Profiles
execute sequentially; live lifecycle sources remain separate and immutable.
Original263/264 replay and reduction checks remain intact. No production,
dependency, public-contract or protocol changes. The initial missing-getter
compile error is preserved as setup evidence; session comparisons use the
existing public LogStore::binding contract. Linux stable1.98.1 and Mac
stable1.98.0 retain their existing conditions.

These are in-memory device-model failures using the actual native store and
core, not physical OS/device crash experiments. Counter is reconstructed through
its public StateMachine contract from real emitted/recovered committed entries;
its reads are local applied checks, not distributed read-index claims. The fixed
local completion drain allows four iterations, uses real barrier results and
fails if it does not drain. One held dependency/prior completion per node remains.
Snapshots, partial writes/publication, permanent device faults, timer actions,
reference comparison and broader composed schedules remain outside this slice.
Raw.sha256 hashes captured bytes; published.sha256 hashes path-normalized text
without trailing whitespace.
