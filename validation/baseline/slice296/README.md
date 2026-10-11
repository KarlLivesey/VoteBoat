# Compatible known-Raft comparison

Linux/macOS each pass251 named complete Raft executions: all74/default74/core29/
native74. Formatting/four strict all-target Clippy profiles are zero on both
hosts. All686 runtime/CI/test-tool input guards match before/terminal/current
publication. The runtime patch applies over frozen295. Test-tool/CI changes are
retained separately in linux-tooling.patch; the actual helper/workflows are in
the input manifest. Both hosts' all-feature normal dependency graphs match295
exactly apart from root path.

Three new tests run in each native-enabled profile.32 controlled six-round
VBDIF1 traces execute twice by exact replay. Every trace asserts seven campaigns,
24 proposals,12 quorum-ready and six unavailable reads,six partitions and19
settled checks. Original IDs/deltas, duplicate retries, isolated accepted writes
and suffix repair are preserved. Actual VoteBoat Raft and NativeLogStore/ModelIo
are compared with independent raft-rs0.7.0 RawNode/MemStorage queues. Compare
committed command prefixes, the actual Counter application and independent
reference dedup values, and real quorum-backed read results. No-op indices and
internal message ordering are not required to match. Isolated97-delta commands
are never committed after repair. Follower forwarding, automatic timeout
campaigns and pre-vote/lease modes are outside this declared compatible scope.

The reference host persists Ready entries/hard state atomically before persisted
messages or advancement, applies committed entries in order and handles
LightReady/advance_apply. Its memory store models successful atomic persistence,
not actual filesystem/device interruption. Deliberate reference-host application
corruption must fail after a real commit; deletion reduction retains campaign1,
pump, original operation41053/delta-3 write,pump,corrupt1,check. Removing corruption
passes that same reduced schedule. Decoder version/ID/length/action bounds are
checked. No oracle supplies either engine's state.

Pinned raft/slog are development dependencies only; default reference logging is
disabled and the selected logger discards output. ARM macOS needs protoc3.x:
the shared helper validates the official protobuf21.12 ARM archive and executable
hashes and reports libprotoc3.21.12 from a temporary cache. CI uses this helper.
Linux uses the reference dependency's existing compiler selection. See
[reference contracts/setup](../../../docs/REFERENCE_TESTING.md).

Preflight logs retain sibling codec visibility errors. The first Mac script
stops at a Bash3.2 empty-array/nounset interaction after fmt/all-feature Clippy;
only script portability changes before its final matrix, with unchanged Rust
inputs. Original Linux script and Mac preflight script are retained. No lint
waiver, production protocol/API/storage/dependency change or design-pack mutation.
Interrupted durability/checkpoint differential, broader combined faults, fuzz,
provider conformance and the retained Linux285/288 shutdown fault remain.
Original Mac285 completed independently. Full P0–P7 is active; performance and
security are the user's later phase, P8/Windows deferred.
