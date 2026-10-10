# Native split commands

`voteboat-transfer` runs a provisioned metadata, source or target replica and
provides authenticated `status`, `start` and `resume` commands. It uses the public
`TransferOperation` decisions and existing durable source/import/publication/
activation guards. Different groups may have different leaders.

The initial profile supports one native byte-bucket counter source and 2–16
targets, with a fixed three-voter default or explicit deployment file. It is an
executable whole-responsibility split; the Rust contracts support broader
compositions separately. Each source/target retains up to32 data operations.
Metadata/source/target roles have separate directories and log owners.

## Local example

Build with `cargo build --locked --all-features --bin voteboat-transfer`.
The example uses the repository's local test certificates. Deployment uses
separately provisioned TLS material as described in [service access](COUNTER_SERVICE.md).

```sh
V=target/debug/voteboat-transfer
D=/tmp/voteboat-transfer-demo
TLS=tests/fixtures/tls
mkdir -p "$D"
$V plan 1 20 21 22 10 128 200 201 > "$D/profile"
printf 'voteboat-service-access-v1 1\n' > "$D/access"
printf 'voteboat-transfer-endpoints-v1\n' > "$D/endpoints"
for spec in 1:14000 20:14128 21:14256 22:14384; do
  group=${spec%:*}; base=${spec#*:}
  mkdir -p "$D/$group"
  printf '3 admin %s 1\n' "$group" >> "$D/access"
  for node in 1 2 3; do
    printf '%s 1 %s 127.0.0.1:%s\n' "$group" "$node" "$((base+100+node))" >> "$D/endpoints"
  done
done
for spec in 1:14000 20:14128 21:14256 22:14384; do
  group=${spec%:*}; base=${spec#*:}
  for node in 1 2 3; do
    $V serve create "$D/$group/$node" "$node" "$base" "$TLS" "$D/profile" "$group" "$D/access" tcp > "$D/$group-$node.log" 2>&1 &
  done
done
$V command "$D/profile" "$D/endpoints" "$TLS" 3 1 initialize
$V command "$D/profile" "$D/endpoints" "$TLS" 3 1 grant
$V command "$D/profile" "$D/endpoints" "$TLS" 3 20 initialize
$V command "$D/profile" "$D/endpoints" "$TLS" 3 20 add 1 7 10
$V client "$D/profile" "$D/endpoints" "$TLS" 3 status
$V client "$D/profile" "$D/endpoints" "$TLS" 3 start
$V command "$D/profile" "$D/endpoints" "$TLS" 3 21 read 7
```

The final read returns `OK value=10`. Retrying operation1 with the same key and
delta at target21 returns its original duplicate result. Source20 now refuses
data commands. Active children continue serving with metadata/source stopped.
Select `quic` instead of `tcp` for peer replication; operator commands use TLS/TCP.

## Profiles and recovery

`plan AUTHORITY SOURCE LEFT RIGHT RESPONSIBILITY SPLIT LIFECYCLE PUBLICATION`
generates the simple two-target profile. Its groups/responsibility use incarnation1,
epoch/generation1→2 and adapter/scheme1. Metadata initialization/grant use1000/1001;
source initialization uses100. Conflicting IDs or mappings are rejected.

Custom profiles use this bounded format, with canonical `TransferIntent::encode`
bytes. Every referenced group must appear exactly once:

```text
voteboat-transfer-profile-v1 LIFECYCLE PUBLICATION
intent CANONICAL_INTENT_HEX
metadata GROUP INCARNATION BOOTSTRAP_OPERATION GRANT_OPERATION
source GROUP INCARNATION BOOTSTRAP_OPERATION 0
target GROUP INCARNATION LIFECYCLE_OPERATION 0
target GROUP INCARNATION LIFECYCLE_OPERATION 0
```

The endpoints file starts with `voteboat-transfer-endpoints-v1`; subsequent rows
are `GROUP INCARNATION NODE ADDRESS`. Alternatively, a row can be
`GROUP INCARNATION COMMAND_PEERS_FILE`, using the existing command-peer format and
its explicit server names. Each group permits at most64 endpoints. The server's
optional `--deployment FILE` selects explicit provisioned peer identities and
addresses. Parent directories must already exist; recovery never creates a fresh
store in place of a missing one.

Restart a replica with `serve recover`, its original directory, group, TLS and
profile. Then use `client ... resume`. It re-reads every phase using the original
operation IDs. `step` executes at most one next action; `status` only reads.
The client stops after128 attempts or roughly120 seconds plus the current round's
bounded request deadlines. A pending/unknown result remains unresolved; resume the same profile.
There is no timeout unfreeze or rollback.

The [recorded recovery tests](../validation/baseline/slice195/README.md) restart
all role processes at each of ten split boundaries, using TCP/WAL and
QUIC/checkpoints. They also interrupt admitted fence/publication commands before
quorum acknowledgement. Original IDs, serving restrictions and child retries
are checked. This is selected process-crash coverage, not arbitrary power loss.

## Retiring the original source

Select `plan ... --retirement` **before creating the source stores**. This emits
`voteboat-transfer-profile-v2-retirement`; its source uses the existing
`RetirementGuard`. Keep that exact profile file for recovery. The source records
its profile digest and node/store/group identities in `transfer-source-profile`
under the native directory lock, before serving. V1 cannot open a marked source;
v2 recovery refuses missing, corrupt or changed bindings. This is a fresh-store
profile, not an upgrade or a rolling-migration mechanism. Interrupted initial
binding requires explicit inspection; never delete old stores to bypass refusal.

After the split completes, explicitly release external retention promises:

```sh
$V client "$D/profile" "$D/endpoints" "$TLS" 3 retire 20 700
$V command "$D/profile" "$D/endpoints" "$TLS" 3 20 retirement-status
```

Here `700` is the operator's stable retention-release ID. The client obtains
fresh authenticated quorum observations, requires every original activation,
and constructs a proof bound to source20's original operation and fence. It
submits retirement through that source's existing Raft log, then confirms the
same release with a quorum read. `retirement-status` requires Inspect;
`retire-group PROOF_HEX` requires Configure. The latter accepts the bounded proof
encoding for hosts using `TransferOperation::retirement_proof` directly.

If a connection or process fails, repeat the same profile, source and release.
An already-retired source answers without requiring the metadata/target groups
to remain available; a different release is refused. Status retains the original
retirement index and is historical evidence, not current target health. Retired
sources refuse ordinary reads, writes and exports. They retain freeze/lineage
evidence across WAL and checkpoint recovery and cannot thaw.

Retirement releases live application payloads. It does not delete the replica,
erase backups, remove membership, or bypass snapshot/WAL retention. External
retention promises must actually be released by the operator. General retention
policy and physical cleanup remain separate work.

## Contracts and limits

Read replies originate from `Node::complete_read`. The transient observation
format preserves group/configuration/committed-prefix data, accepts at most128KiB
and rejects truncated, trailing or inconsistent structure. Decoding is **not**
authentication, freshness or a portable quorum certificate: the client verifies
the live TLS endpoint and requested group/query before using the observation.
An administrator is a trusted lifecycle publisher; the server checks the fixed
profile and existing application guards, not a new Byzantine evidence protocol.

Mutations and immutable exports require Configure permission; observations and
data reads require Read permission. The server admits one connection with one
pending read/proposal, up to270KiB command bytes, a15-second connection deadline,
64KiB application commands/images and existing native queue bounds. Disconnect
cancels the exact read/client ticket; accepted writes may still commit. Completions
are drained after disconnect. Normal shutdown joins native workers; failure
transfers to the existing recovery owner and reclaims its workers.

Before publication, a source configuration change after import requires explicit
resolution rather than rewriting provenance. Historical `Complete` does not
authorize a target that later loses ownership. Arbitrary persistence cuts,
concurrent membership changes, recursive profiles, general retention policy,
macOS and separate-host deployment remain distinct acceptance work. See
[split recovery](SPLIT_RECOVERY.md) and the [baseline ledger](BASELINE_ACCEPTANCE.md).
