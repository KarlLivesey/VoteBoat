# Native wire format 1

`NativeWireCodec` implements `WireCodec` with wire version 1 and a fixed
24-byte prefix. It serializes the current static-configuration Raft RPCs.
It has no persistent-format dependency: WAL and snapshot-file changes do not
silently change peer bytes. Future incompatible messages require a named wire
version and construction-time compatibility selection. Unknown versions, flags,
RPC kinds and policy kinds are rejected.

All integers are unsigned, little-endian. Booleans are exactly 0 or 1. Checked
IDs/incarnations/sessions/configurations and operation IDs must be nonzero.
Protocol terms and request-context sequences must be nonzero. The transport
supplies the expected `WireScope` from its authenticated connection; every
message must match its sender node, sender store binding and recipient node.
The same frame may carry different groups/configurations. Decoded claims never
choose that trusted scope. This codec does not authenticate a connection.

## Frame

| Offset | Bytes | Meaning |
| --- | --- | --- |
| 0 | 8 | ASCII `VBWIRE01` |
| 8 | 2 | Wire version, 1 |
| 10 | 2 | Flags, 0 |
| 12 | 4 | Total frame bytes, including prefix and final checksum |
| 16 | 4 | Message count, positive |
| 20 | 4 | CRC32C of prefix bytes 0 through 19 |
| 24 | variable | Message records |
| total minus 4 | 4 | CRC32C of every preceding frame byte |

The prefix is validated before allocating a frame buffer. The declared length
must fit `max_frame_bytes`, the count must fit `max_messages`, and the frame
must contain at least 133 bytes per declared record plus 28 framing bytes.
`frame_length` accepts exactly the fixed prefix. `decode_batch` accepts exactly
one full frame; trailing stream data belongs to the next frame, not this call.
Every record is a `u32` byte count followed by exactly that many message bytes.
The count excludes its own four bytes. A message is at least 129 bytes.
Trailing record, batch or frame bytes are errors. No partial batch escapes.

CRC32C detects accidental corruption; an attacker can recompute it. It provides
no cryptographic provenance, authentication, replay defence or quorum evidence.

## Message envelope

Every record starts with this 128-byte envelope, followed by one RPC kind byte
and its payload. Store identities are stable; store sessions scope recovered
admissions. The request context's origin may differ from the current sender,
as responses retain their request's original context.

| Bytes | Field |
| --- | --- |
| 16 | Group ID |
| 8 | Group incarnation |
| 8 | Configuration ID |
| 8 | Sender node ID |
| 16 | Sender store ID |
| 8 | Sender store incarnation |
| 8 | Sender store session |
| 8 | Recipient node ID |
| 8 | Term |
| 16 | Request-context origin store ID |
| 8 | Request-context origin store incarnation |
| 8 | Request-context origin store session |
| 8 | Request-context sequence |

## RPC payloads

| Kind | RPC | Payload after the kind byte |
| --- | --- | --- |
| 0 | Vote | `u64 last_index`, `u64 last_term` |
| 1 | Voted | Boolean granted |
| 2 | Append | `u64 previous_index`, `u64 previous_term`, `u64 leader_commit`, `u32 entry_count`, entries |
| 3 | Appended | Boolean success, `u64 matching_index` |
| 4 | ReadProbe | Empty |
| 5 | ReadAck | Empty |
| 6 | Snapshot | `u64 index`, `u64 term`, `u64 application_schema`, policy tree, voter stores, application bytes |
| 7 | SnapshotAck | `u64 index`, positive |
| 8 | Compacted | `u64 index`, positive, `u64 term` |

Each entry contains `u64 index`, `u64 term` and one payload kind byte.
Kind 0 is a no-op with no additional bytes. Kind 1 contains a 16-byte operation
ID, a `u32` command byte count and that many command bytes. Entry count and
command lengths are checked against limits and remaining input before allocation.
Entries must extend the previous index contiguously, have positive nondecreasing
terms and have no term greater than the message term. An empty log boundary has
both index and term zero; a nonempty boundary has both positive.

Snapshot bootstrap group/configuration come from the message envelope. Its
index is positive and less than `u64::MAX`; term is positive and at most the
message term; schema and application length are positive. A policy tree uses:

- Kind 0: `u64 voter_node`.
- Kind 1: `u32 child_count`, then that many trees, for strict majority.
- Kind 2: `u32 child_count`, then `u64 weight` and a tree per child.

Branches are nonempty. Counts, total nodes, depth and voter count are bounded.
The validated quorum policy rejects duplicate voter leaves, zero weights and
weight-total overflow. Voter stores follow the tree: `u32 count`, then tuples
of `u64 node`, 16-byte store ID and `u64 store_incarnation`. Their exact voter
keys must equal the policy's leaves. Finally, `u32 application_byte_count`
precedes the application bytes. A snapshot-file checksum/footer is not embedded;
the enclosing wire frame has its own integrity checks. Local installation still
requires the separate snapshot stage/seal/pin and authoritative log dependencies.

## Resource and evidence limits

The default frame ceiling is 1 MiB, with at most 128 messages, 1,024 entries per
message, 64 KiB per command and 512 KiB of snapshot application data. Conservative
decoded retention is limited to 4 MiB; policy limits are depth 32, 4,096 voters
and 16,384 nodes. These are configurable construction limits within hard caps.
The current protocol transfers complete snapshots; it has no wire chunk RPC.

Before each allocation the decoder charges message/entry arrays, command or
application payloads, policy branch arrays, snapshot objects and bounded policy
index units. This is a retained-object budget, not an exact allocator or peak
RSS promise. Encoding first validates and measures with no frame allocation,
then requests exactly the final frame capacity. It charges the same decoded
objects so a successful encode fits the matching decoder's limits.

Socket receive buffers, partial-frame retention, encoded send buffers, output
queues and decoded ingress have separate ownership/budgets. No network I/O is
implemented here. Successful decode establishes no new consensus authority:
the core still validates membership, term/configuration, log provenance and
fresh read contexts. Production assembly must supply authenticated sessions.
