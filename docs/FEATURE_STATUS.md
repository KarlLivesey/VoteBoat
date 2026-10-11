# Feature status

Current priority: usable, feature-complete Rust embedding and networked service
on Linux and macOS. Current work covers feature sets only. Performance tuning
is deferred; completing feature qualification does not automatically start it.
The current goal now includes functional P8 work and stops at its qualified
completion. P9 performance and P10 security remain separate later phases.

| Capability | Implemented path |
| --- | --- |
| Durable writes, quorum reads, retries and restart | Native Raft, WAL/checkpoints, public Node and counter service |
| TCP/TLS and QUIC | Public connector/session/transport contracts and native providers |
| Shared Multi-Raft and bounded work | Shared scheduling, workers, admission and owned shutdown |
| Recursive quorums | Validated policies at elections, commitment and reads |
| Membership changes | Learner catch-up/readiness, joint/final configuration and recovery |
| Responsibilities and routing | Replicated manifests, selective placement, recursive directory and cache |
| Split and compatible merge | Durable fence/import/publish/activate, retry lineage and retirement |
| Replaceable infrastructure | Public application, storage, transport and runtime seams with native and downstream test providers |

These mechanisms are implemented. The current Linux/macOS acceptance sweep is
still running; **feature-complete qualification is not yet claimed**. Linux's
original complete four-profile sweep passes5,661 test executions with zero
failures ([terminal evidence](../validation/baseline/slice302/linux/README.md)).
A controlled shutdown policy defect
is corrected: the service respects an empty QUIC connection's original close
timer after application work settles. The corrected complete counter target
passes203 tests on Linux and202 on Mac, including the previously failing Mac
multi-group history. Formatting and all four strict Clippy profiles are clean
on both hosts. The public
examples expose bounded profiles, and do not automate every library combination.

The finite remaining baseline acceptance is the current platform sweep and resolution
of any failures it demonstrates. The prepared batch adds seeded parser campaigns,
per-bucket split client histories and shared connector assertions. The shutdown
correction pins existing protocol ownership once; it changes no provider timer
or consensus rule. The original Mac failure lacked ownership observations, so
its precise internal cause remains unknown. See the linked plan and actual evidence
in [IMPLEMENTATION.md](IMPLEMENTATION.md).

Further reference checkpoint comparison, extra provider permutations and global
orchestration are not new prerequisites for this feature milestone. External
storage adapters still require implementation and conformance; the public seam
does not certify a hypothetical backend or permit an unverified live migration.
Subgroup-backed logical voters and broader transactions are newly included in
P8 and **not yet usable native features**. Existing recursive quorums and outbox
support do not implement them. Their protocol/model/recovery contracts precede
native qualification. The [transaction protocol model311](../validation/baseline/slice311/README.md)
now passes on Linux/Mac in four profiles, including actual weakened-rule
counterexamples. [Replicated transaction applications312](../validation/baseline/slice312/README.md)
now pass17 unit/downstream tests per profile on both hosts, with zero formatting
and four strict Clippy profiles. The actual networked transaction driver and
logical voters remain.
Striped single-group WAL and representative-relay optimization
move to P9, along with the original performance gates and required research cost
comparisons. Default research promotion remains gated by that evidence. Windows
remains deferred. This goal completes only after its functional P0–P8 scope is
qualified, then stops for the user's performance-planning review.

Existing evidence includes the [mixed Linux/Mac service](../validation/baseline/slice256/README.md),
[native snapshot recovery](../validation/baseline/slice294/README.md),
[known-Raft comparison](../validation/baseline/slice296/README.md) and
[interrupted recovery and owned close](../validation/baseline/slice297/README.md).
Historical failures stay in those records and are not erased by selected later
passes. The detailed [acceptance map](BASELINE_ACCEPTANCE.md) tracks their scope.
