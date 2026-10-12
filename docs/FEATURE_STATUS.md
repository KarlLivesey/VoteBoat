# Feature status

The goal is **all P0–P8 functionality, qualified on Linux and macOS**. P8 is
still incomplete. Performance work is P9, dedicated security work is P10,
and Windows is deferred. Stop after P8; do not begin those later phases automatically.

| Capability | Current evidence |
| --- | --- |
| Durable Raft, quorum reads, retries and restart | Native WAL/checkpoints, public Rust embedding and networked counter service; Linux/macOS acceptance records |
| TCP/TLS and QUIC | Native providers through public session, connector and transport contracts |
| Shared Multi-Raft, recursive quorums, routing and split/merge | Implemented paths with protocol, durability and selected native acceptance records |
| Broader transactions and striped single-group storage | Selected Linux/macOS matrices in frozen318/321 |
| Subgroup-backed logical voters | Selected Linux/macOS facade and recovery controls; current deeper composition remains under qualification |
| Ordinary physical-voter relays | Guarded Linux/macOS static-tree TCP/QUIC histories and relay-backed logical storage pass, including branch loss/restart, retained TLS closure, checkpoint/recovery and cleanup |

The remaining functional work is:

1. **Logical membership lifecycle.** The first genuine twelve-owner learner
   replacement passes Linux TCP and QUIC, including export/import, readiness,
   Joint/Final, Outcome/Release, business operations and cleanup. Imported-target
   same-store cold recovery also passes both protocols, with the nine source
   owners kept live. Linux also passes all-twelve-store Joint WAL reconstruction
   and committed/uncommitted-Unknown import cold resolution. Repeated replacement
   now passes Linux TCP and QUIC, including both replacements and all fifteen
   owners' checkpoints, cleanup and port rebinds.
   The Mac's first replacement passes over TCP and QUIC on the preflight-fix
   source, with final source and binary guards verified. The Mac also passes
   imported-target and committed/uncommitted-Unknown recovery over both protocols.
   Repeated replacement
   reaches durable Final but misses its fixture wait; the corrected phase wait
   passes Linux TCP; QUIC reaches the same shutdown failure as Mac Joint recovery.
   A guarded diagnostic identifies the failed conjunct: parent and bridge
   observations are settled, while running children still own consensus work.
   The corrected fixture closes and fully drains those same children next;
   Linux repeated TCP now passes all fifteen owners' cleanup, and Joint cold
   recovery passes both protocols. The latest repeated-QUIC run also passes
   every cleanup check; its preceding child-close timeout remains unexplained.
   Matching Mac qualification is running.
2. **Mac logical composition.** Complete twenty-seven-owner cold recovery passes
   Linux TCP and QUIC: same-store restore, duplicate results, fresh reads and all
   owner joins. The matching Mac cut passes formatting, all four strict Clippy
   configurations and 36 controls, but fails its initial TCP write and QUIC Claim.
   A later guarded QUIC observation completes the original write and all three
   top owners converge, then fails the checkpoint gate before cold recovery.
   A subsequent checkpoint observer instead fails during the write, before
   checkpointing, while lower durable work advances. The write test now keeps
   its original write limit and gives subsequent exact replica convergence a
   separate bounded wait. Full Linux cold histories pass both protocols;
   matching Mac qualification is queued behind membership recovery.
3. **Final current-source qualification.** Audit the complete P0–P8 requirement
   map, resolve remaining demonstrated failures, and run the required Linux/macOS
   checks against a coherent source snapshot. The current Linux matrix is running;
   completion remains unproven until platform and requirement evidence is reviewed.

Every implementation slice must pass formatting and strict all-target Clippy in
all four supported feature configurations. The pre-push hook remains enabled.
Frozen passing cuts establish their recorded scope; they do not certify the
moving working tree or an untested combination. CI remains background feedback.

The static logical constructor now rejects release profiles that cannot hold a
whole supported commit batch. Linux controls and native facade histories pass;
the matching Mac controls and both facade histories now pass too. This does not
resolve the Mac nested-write failure.

The latest bounded roadmap audit found no additional missing feature set beyond
the logical composition and membership gates above. A preserved Mac hundred-group
test misses its ten-second wall limit with persistence/snapshot work pending;
that record is not evidence of a new missing feature or a performance target for P8.

See the [current mini plan](IMPLEMENTATION.md#current-mini-plan372),
[requirement and acceptance map](BASELINE_ACCEPTANCE.md), and
[validation record](../validation/REPORT.md). Historical failures and exact
source, executable and test evidence remain in those records.
