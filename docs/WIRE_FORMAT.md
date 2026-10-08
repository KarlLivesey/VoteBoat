# Native wire formats 1–4

`NativeWireCodec::new` selects wire version 1 for existing static-configuration
assemblies. `NativeWireCodec::with_membership` explicitly selects version 2,
adding configuration entries and configuration-aware snapshots.
`NativeWireCodec::with_authority` selects version 3, retaining format 2 payloads
and adding direct witness authorization. `NativeWireCodec::with_readiness`
selects version 4, adding learner readiness requests/replies. All implement
`WireCodec` with a fixed 24-byte prefix. A selected codec accepts only its own
version; session/roster wire versions must match before transport admission.
There is no automatic downgrade. Codec capability does not enable online Raft
reconfiguration, which remains disabled pending live protocol validation.
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
| 8 | 2 | Wire version, 1, 2, 3 or 4 as explicitly selected |
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
as responses retain their request's original context. Configuration IDs also
identify the requester's accepted head and are echoed by responses. They do not
claim responder activation or identify the snapshot base. See
[replication scope rules](REPLICATION_SCOPES.md).

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


## Format 2 membership extensions

Format 2 retains the framing family magic, envelope, CRCs and existing RPC/entry
layouts. Format 1 refuses the new mandatory tags even if a frame is relabelled
and its checksums recomputed. Persistent-format compatibility is separate.

Append entry kind 2 contains configuration subformat byte 1, a 16-byte operation
ID, an 8-byte expected configuration ID and one change kind:

- Kind 0 (learners): one complete configuration.
- Kind 1 (joint): an 8-byte joint configuration ID and the complete final target.
- Kind 2 (final): an 8-byte final configuration ID.

A complete configuration is its 8-byte ID, validated recursive policy tree,
voter-store map and learner-store map. Each map uses the store tuple/count layout
above. Keys are unique, voter keys exactly match the policy, learner keys are
disjoint, and their combined count fits the configured policy voter limit.
Record retained size also fits `max_command_bytes`. The decoder checks bounded
shape/IDs/policies; journal ordering, expected-configuration comparisons,
learner readiness and commitment are the log/core's obligations.

Snapshot RPC kind 9 contains an explicit 8-byte original bootstrap configuration
ID, followed by index/term/schema and bootstrap policy/store map as in kind 6.
Next is a boolean membership-base-present flag. If true, it carries:

1. Mandatory membership subformat byte 1 and the complete stable configuration.
2. An 8-byte last configuration-record index, bounded by the snapshot index.
3. A boolean joint flag; if true, joint operation ID (16 bytes), joint ID and
   joint record index (8 bytes each), then the complete target configuration.
4. A `u32` operation count followed by 16-byte configuration operation IDs.

The operation set is unique, nonzero and bounded to 16,384. Structural checkpoint
validation checks the joint record, staged store identities, reserved IDs,
operation retention and original bootstrap binding. Then a `u32` application
length precedes the application bytes. A false base-present flag represents the
original bootstrap state, including a checkpoint predating the first transition.
Kind 6 remains available when no membership base is needed and the bootstrap
configuration equals the envelope configuration.

The sender's current configuration in the envelope may differ from the
checkpoint's base configuration. The codec preserves both; the live protocol
must establish the sender's authority and bind the incoming base to the local
history. Decoding metadata never supplies that provenance. Installation still
requires durable snapshot publication/pinning and exact authoritative WAL
completion before dependent effects escape.

Configuration trees, voter indices, store maps, boxed records/bases and retained
operation sets are charged to the decoded-object budget before their respective
allocations. The 192-byte map/set-unit charges conservatively include B-tree
metadata. Counts are checked against limits and remaining bytes before traversal.
Snapshot-base retained size is checked before encoder semantic validation.
Validation may use bounded temporary clones; the limit is retained decoded
objects, not an exact peak allocator/RSS bound. Encoding and decoding enforce
matching retained-object ceilings.

Tests in `tests/wire.rs` cover learner/joint/final roundtrips, recursive weighted
targets, stable/joint checkpoint bases, original bootstrap checkpoints behind a
newer sender, strict version selection, every frame truncation/bit flip,
valid-checksum hostile IDs/counts/maps/operation sets and exact memory limits.
`tests/transport.rs` drives configuration Append and membership Snapshot through
native framed transport with short host-channel I/O, exact queue credit release
and session-version rejection. `tests/membership.rs` verifies successful format-2
decode cannot bypass the core's online-change refusal. These are codec/transport
checks, not dynamic-membership Raft safety or authenticated network histories.

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

## Format 3 direct witness extensions

Format 3 retains all format 2 layouts and adds RPC tags 10/11. Native session
startup defaults to format 1; `NativeTlsConfig::with_wire_version(3)` selects
format 3 for its encrypted identity hello, codec and roster. Both peers must
select the same version. There is no automatic upgrade or fallback.

- Kind 10, AuthorityRequest: candidate node (`u64`), store ID (16 bytes), store
  incarnation (`u64`), requested configuration (`u64`).
- Kind 11, AuthorityReply: the same fields, committed index (`u64`), committed
  term (`u64`), granted (one Boolean byte).

The candidate differs from both envelope nodes, and the requested configuration
is newer than the envelope's base configuration. Request context origin equals
the authenticated sender binding. Granted boundaries are positive and term is at
most the envelope term; denied boundaries are both zero. The receiver matches
replies against its fresh pending context and exact trusted witness store.
Formats 1/2 reject these tags even in relabelled, resealed frames. Format 3 keeps
existing membership snapshot tag 9. These are membership control assertions,
not ballots, read probes or term updates. See
[replication authorization](REPLICATION_AUTHORITY.md) for provenance and limits.

## Format 4 learner readiness

Format 4 retains format 3 and adds tags 12/13. Both carry learner node (`u64`),
store ID (16 bytes), store incarnation (`u64`), learner store session (`u64`),
required committed index (`u64`), its term (`u64`), application schema (`u64`),
required command bytes (`u64`) and snapshot bytes (`u64`). Tag 13 appends one
Boolean readiness byte. Byte counts convert to `usize` with checked conversion.
They are capability requirements, not payload lengths or allocation requests.

Group/configuration/term/context come from the envelope. For tag 12 the leader
is the sender and learner is the recipient; context origin equals sender binding.
For tag 13 the leader is the recipient and learner is the sender; the learner
store/session must equal the authenticated sender binding. Required index/term
and all requirements are positive; required term is at most the envelope term.
The full reconstructed request must match the leader's pending request.

The fixed-size request is boxed in memory and charged separately to decoded and
outbound retention. Older selected formats reject these tags even when relabelled
with valid checksums. TLS and QUIC select exact format 4 explicitly, with no
downgrade. Neither request nor reply updates terms, ballots, reads or membership.
Successful verification uses the original owner visit and selected asynchronous
snapshot worker; see [learner readiness](LEARNER_RECOVERY.md). Configuration
delivery remains gated pending the online activation release checks.

## Configuration capacity query

WireCodec::configuration_capacity counts the proposed record, declared command
and prospective checkpoint through the selected native encoder's validation/count
pass. It includes bootstrap policy, stable/joint membership and retained operation
identities in checkpoint framing and retained decoding budgets. Opaque command
and application bytes are counted virtually, without allocating buffers of their
declared size. Invalid journal/envelope, unsupported format, policy/retained limits
or encoded frame overflow reject. Positive results name the selected format and
three WireFootprints; they establish neither quorum nor commitment. Formats 2–4
support this native query; format 1 refuses membership. Existing bytes are unchanged.
Downstream tests compare actual encode/decode sizes and exact/one-byte-short
boundaries, including growth of checkpoint operation history.
