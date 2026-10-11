# Reference Raft tests

`tests/raft/reference` compares VoteBoat's public majority core and actual native
log provider with pinned TiKV raft-rs 0.7.0. The reference and discarded slog
logger are development dependencies. Normal library/service builds do not depend
on either reference code or protobuf. This is no production adapter.

Run `cargo +stable test --locked --offline --all-features --test raft reference::`.
Fetch the lockfile first on a new machine. On ARM macOS, prepare the test-only
compiler in the same shell before tests, all-target lint or push:

```sh
export PROTOC="$(bash validation/reference-tools.sh)"
```

The helper verifies the official protobuf21.12 ARM archive and executable hashes,
which report `libprotoc 3.21.12`. It caches under the temporary directory, or
`VOTEBOAT_REFERENCE_TOOLS` when explicitly set. Remove that directory after use.
Other platforms use raft-proto's existing compiler selection; do not export an
empty PROTOC there. CI prepares the ARM tool before test builds.

VBDIF1 schedules preserve seed, node, operation and read IDs, signed Counter
deltas and exact external action order. Both engines use independent bounded
queues and honor their own persistence dependencies. Reference Ready entries and
hard state are stored atomically before persisted messages/advancement; committed
entries and LightReady are then applied in index order. This memory store models
successful atomic persistence, not physical disk behavior.

The controlled three-voter contract uses explicit campaigns without pre-vote,
quorum leases or automatic timeout campaigns. Proposals and reads address each
engine's actual leader, including an isolated old leader. Follower forwarding is
excluded because these APIs differ. FIFO delivery independently drains each
engine; partitions drop crossing packets. At declared settled barriers, compare
committed `(operation, delta)` prefixes, actual VoteBoat Counter results and an
independent reference dedup evaluator. Read values require each engine's real
quorum protocol and applied boundary. Internal no-op indices/messages need not
match. Retries preserve original IDs and deltas; no oracle supplies recovery.

Thirty-two six-round schedules run twice by exact codec replay. Each has seven
campaigns,24 proposals,12 ready reads,six unavailable reads,six partitions and19
settled checks. Isolated accepted97-delta writes are absent from the healed
committed prefix. A deliberately corrupted reference application must mismatch
after a real committed command; deletion reduction preserves that failure and
the corrected host passes the reduced schedule. Decoder bounds/refusals are
checked separately.

VBDIF2 adds explicit hold, storage write, sync, completion, process/power restart
and stale callback actions. VoteBoat uses actual native WAL bytes and original
admission tickets/barrier completions. The reference pins its actual Ready and
owns separate written and synchronized Entry/HardState images. Only explicit
completion releases persistence-dependent effects. A fresh core and application
replay each engine's own surviving prefix; acknowledged commands are comparison
evidence, never recovery input. Native old-session completions must fail without
progress; the reference host rejects old Ready epochs before touching RawNode.

Eighteen histories cut leader commands at accepted/written/synchronized phases
under process and power loss. Six more restart acknowledged completed commands,
then exercise stale callbacks, original retries and real quorum reads. A held
two-follower history proves synchronization without completion cannot release
their acknowledgements. Exact codec replay retains these cuts. A deliberately
false reference sync loses an acknowledged original operation under power loss;
failure-preserving reduction retains that write and cut, and the corrected host
passes the same schedule.

The reference image models loss of unsynchronized state, not serialized/torn
physical disk behavior. Selected native physical fault tests remain separate.
This scope does not certify hierarchy, joint membership, message-by-message
equivalence, arbitrary vote/read/follower crash schedules or differential
checkpoint recovery. Differential checkpoint recovery is the next deliverable.

Reviewed primary sources: [raft-rs tagged manifest](https://github.com/tikv/raft-rs/blob/v0.7.0/Cargo.toml),
[RawNode/Ready contract](https://github.com/tikv/raft-rs/blob/v0.7.0/src/raw_node.rs),
and [official compiler release](https://github.com/protocolbuffers/protobuf/releases/tag/v21.12).
Locked protobuf-build0.14.1 selects system protoc3.x or a bundled binary; its
bundled Mac option lacks ARM support, which is why the temporary tool is needed.
