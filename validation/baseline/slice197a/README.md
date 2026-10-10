# Slice197a — local drain admission and campaign gate

Base revision: `96d7bee`. Linux local execution; source digests are in
`sources.sha256`. This advances coordinated drain197 without declaring it done.

The owning Node validates an exact sorted manifest of at most1024 assigned
GroupIdentity/ConfigurationId pairs before gating ordinary writes and reads.
It admits at most32 campaign-suppression events per poll, retaining queued
identities through backpressure. Existing leaders keep heartbeat and transfer;
followers continue voting and replication. Explicit maintenance admissions use
the original application, quorum, ticket and shutdown contracts. A suppressed
candidate discards volatile election collection after pending durability is
resolved, while retaining its durable vote.

Status distinguishes unsuppressed, leader, stale configuration, fenced and
pending-core states. Held client/read completions prevent local quiescence.
The gate is volatile and one-way until shutdown. Recovery enables campaigns;
a host must reapply durable drain intent before exposing recovered work.
Local quiescence is not remote quorum, replica removal or durable drain success.
Full durable operation recovery, cancellation/resumption, placement/membership
orchestration and executable drain commands remain197b.

Evidence:

- `regression.log`:247 passing all-feature tests (effect_owner164, raft39,
  runtime28, startup16). Eighteen focused additions exercise delayed ballots,
  pending self-vote, explicit/TimeoutNow refusal, recovery, stale timers,
  heartbeat preservation, exact manifests, stale configurations, queue pressure,
  preserved requests and held completions. Native TCP/QUIC histories commit
  maintenance, hand off leadership, stop/join the source and continue writing
  through the remaining two voters.
- `minimal-tests.log`:206 passing no-default-feature tests (effect_owner158,
  raft27, runtime21), including oversized manifest refusal. These overlap the
  above histories and do not constitute an independent proof.
- `node.log`, `native.log`:earlier focused runs, overlapping the regressions.
- `fmt.log`, `clippy-all.log`, `clippy-default.log`, `clippy-minimal.log`:
  formatting and strict all-target feature profiles pass with zero diagnostics.
- `inventory.log`:101 contract metadata/path entries pass. This verifies
  metadata consistency, not runtime correctness.

No new persistence format, worker, runtime or dependency. No macOS execution,
remote availability certification, arbitrary power loss or completed P0–P7 claim.
