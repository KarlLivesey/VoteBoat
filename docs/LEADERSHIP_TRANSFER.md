# Targeted leadership transfer

The deterministic Raft core exposes `Event::TransferLeadership` with an
operation ID, exact target node/store and expected configuration. This is a
volatile protocol primitive. [Durable Rust maintenance](MAINTENANCE.md) now
wraps it with replicated intent/status; the authenticated executable
`move-leader` command remains slice196b2.

The source must be the current leader in a committed stable configuration. It
refuses self-transfer, learners, unknown/replaced stores and joint or uncommitted
membership. One attempt per group is allowed. Repeating its exact request is
idempotent while pending; another request returns `Busy`.

During the attempt, new proposals, configuration changes and local campaigns
return `Busy`. Existing replication and ordinary quorum-backed reads continue.
The source captures its entire current tail and sends `TimeoutNow` only after:

- that tail is locally committed and belongs to the source's current term;
- the exact target has acknowledged a durable matching prefix through it;
- the core has no unresolved persistence or application dependency.

The target validates group, configuration, origin, exact last-observed leader,
term and durable tail. It then starts an ordinary election, persisting its new
term and self-vote before sending vote requests. The existing validated quorum
policy still decides election and commitment; the signal grants no ballot.

`Raft::leadership_transfer()` reports the captured request/context/term/index
and whether a signal was emitted. Signal emission is **not completion**. Lost
messages can be retried on heartbeats. Role, term or configuration changes,
storage failure and restart invalidate the volatile attempt. Absence after a
restart is not evidence of success.

`Event::CancelLeadershipTransfer` requires the exact current request context.
It releases local quiescence but cannot retract a delivered signal. The caller
owns deadlines, durable operation tracking and subsequent outcome observation;
a timeout or cancelled attempt may have an unknown outcome. Do not claim a
successful maintenance handoff from a cached leader, admission or sent signal.

Select `NativeWireCodec::with_leadership_transfer` and matching native TLS wire
version8 on every peer. Versions1–7 reject the new RPC; defaults are unchanged
and there is no downgrade. Native startup supports explicit version8 for both
TCP/TLS and QUIC. Session-version matching and ordinary native startup recovery
are tested separately from the deterministic handoff histories. Slice196b1 also
exercises the durable Rust workflow over native TCP/QUIC. Executable command
and disconnect histories remain part of196b2.

See [slice196a evidence](../validation/baseline/slice196a/README.md) for the
tested boundaries and limitations. No persistent Raft/application format or
storage provider contract changes.
