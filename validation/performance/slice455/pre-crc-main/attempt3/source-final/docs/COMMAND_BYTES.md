# Immutable command bytes

`log::CommandBytes` is the common owned value for `EntryPayload::Command.bytes`.
Construct it from an owned `Vec<u8>` or a borrowed byte slice. Nonempty content
is captured into immutable shared backing; clones retain that backing. Empty
values own no allocation. The value exposes slice reads, `AsRef<[u8]>`, `Deref`
and `to_vec()` for an independent mutable copy. Replace the whole value to change
a command. No mutable slice, underlying Arc, pool or runtime is exposed.

```rust
use voteboat::log::{CommandBytes, EntryPayload};

let original: CommandBytes = vec![1, 2, 3].into();
let shared = original.clone();
let mut edited = shared.to_vec();
edited[0] = 9;
let replacement: CommandBytes = edited.into();
assert_eq!(original.as_slice(), &[1, 2, 3]);
assert_ne!(original, replacement);
let _payload = EntryPayload::Command {
    operation: voteboat::identity::OperationId::new(1).unwrap(),
    bytes: original,
};
```

Equality remains byte-exact. Identical immutable backing can compare directly;
independent allocations compare their full content. Operation IDs, indices,
terms, configuration, snapshots and other effect fields retain their separate
exact checks. Sharing content grants no durability, quorum or generation
permission. Altered persistence effects are still rejected by exact preflight.

Retained accounting charges every owner conservatively: empty content is zero;
nonempty content is its length plus two `usize` shared-control words. The charge
excludes the inline value already included in `size_of::<LogEntry>()`. Every
retained clone is charged the full amount; sharing never discounts a component's
credits. Dropping an owner releases only its reference, so queued work can retain
content after log reclamation while staying within its own existing bounds.

`Proposal.bytes` remains a raw Vec. Batch admission preserves its existing
role/count/allocated-count/index/length/staging checks, then bounds the temporary
source-Vec plus shared-allocation conversion peak before capture. Conversion-peak
refusal happens before accepted work and is an explicit staging-budget refusal.
Legacy single proposals keep their existing checks and do not acquire the batch
ceiling; their one-command conversion allocation is bounded by command length.
The immutable log value has no mutable spare capacity. Entry Vec capacity still
counts and normalizes under the log contract.

Wire and WAL command tags, IDs, lengths and bytes retain their formats. Decoders
validate the existing length/truncation/error order, charge the shared allocation
before allocating, and capture directly from the validated slice. Encoded byte
length and retained-memory charge are different quantities. Provider counting,
configuration footprints and decoder memory limits must agree on the latter.

This changes Rust command construction and mutation syntax, not persistent or
wire compatibility. Log stores and applications use the same domain value;
provider selection, tickets, retry, fencing, ordered application and every
original outcome remain controlled by their existing contracts. Actual layout,
qualification and performance evidence are recorded separately in docs/P9.md.
