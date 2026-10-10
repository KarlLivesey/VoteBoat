# Slice196a — deterministic leadership handoff

Base revision:72e4aa2d8dbe9505ac5e5c5368b7fe26d6231d6f. RPL-1.5.
`sources.sha256` identifies the changed production/test inputs. This slice
changes no application, WAL or snapshot format and adds no dependency.

The public core request binds one operation, exact target node/store, accepted
configuration, source term, captured tail and request context. New proposals
and configuration changes quiesce while existing replication continues. The
signal waits for local current-term commitment and target durable catch-up.
The target persists a normal new term/self-vote before ordinary quorum election.
Signal emission is not success; cancellation cannot retract delivery, and
restart discards the volatile attempt.

Eleven new downstream tests use the public core and host LogStore implementation:
ordinary and recursive elections, lagging targets, uncommitted source tails,
target catch-up persistence, target ballot persistence, duplicate/stale/forged
signals, source/target restart, exact cancellation, invalid targets and missing
election quorum. Two additional private membership tests use explicitly synthetic
durable fixtures to reject learner, joint and uncommitted membership states.
They are not native storage or distributed reconfiguration evidence.

The new wire test roundtrips the complete u128 operation ID, rejects versions1–7,
checks every truncated prefix and refuses bad origin/term/tail fields. Existing
TLS/QUIC tests now include exact version8 agreement and7/8 mismatch refusal.
One new startup test creates three native replicas, writes, drains and joins,
reopens the original stores and checks application state for both transports.
It validates version8 assembly; it does not send a leadership handoff over the
network. Existing service regressions preserve their selected older/default
wire formats.

## Recorded validation

| Log | Actual result |
| --- | --- |
| `core.log` |102 passing core-only tests across library, Raft, membership, quorum and wire targets. |
| `native-core.log` |350 passing all-feature library, configuration capacity, effect-owner/Node, member recovery, membership, Raft and wire tests. |
| `tls-version.log` | Exact version agreement/mismatch test passes, including8 and7/8 refusal. |
| `quic-version.log` | Equivalent QUIC version test passes. |
| `startup-version.log` | New TCP/QUIC three-node version8 create/write/drain/reopen test passes. |
| `counter-service.log` | Two authenticated TCP/QUIC executable command/recovery regressions pass. |
| `transfer-service.log` | Three original executable profile/authentication/split/recovery regressions pass; heavy slice195 cuts are explicitly excluded. |
| `fmt.log`, `clippy-{all,default,minimal}.log` | Formatting and all three all-target strict Clippy configurations pass with zero diagnostics. |
| `rustdoc.log` | All-feature documentation builds with warnings denied. |
| `inventory.log` |99 contract records pass shape/path checks, not consensus correctness. |

Counts are test executions in each scope, not distinct correctness properties.
The final native regression and lint commands ran after the compatibility fix.
Commands are in `commands.txt`. Tests needing loopback TCP/UDP ran outside the
restricted network sandbox, with local authorization.

Two unsuccessful attempts are retained. `native-core-sandbox-denied.log` records
the original UDP PermissionDenied failure; the authorized rerun succeeds.
`startup-version-before-compatibility-fix.log` records Node rejecting version8
against its exact version7 repair gate. The focused fix explicitly permits the
7/8 repair-compatible pair while keeping version5/6 requirements unchanged.
The final startup and effect-owner regressions pass.

## Remaining scope

Slice196b still owns durable operation identity/status, Node administrative
admission/result ownership, authenticated executable move-leader commands and
actual native transfer/restart histories. This primitive has no durable success
receipt. A host must own deadlines and resolve unknown outcomes; stale cached
leadership is not proof. There is no joint-membership transfer or guarantee that
a requested target wins without its ordinary quorum.

These finite histories are not an exhaustive distributed proof, fault generator,
power-loss test, separate-host/macOS run or full P0–P7 acceptance. Existing
combined lifecycle, provider, deployment and original P7 performance gates
remain open. Coordinated drain197 depends on completed196, not just this signal.
