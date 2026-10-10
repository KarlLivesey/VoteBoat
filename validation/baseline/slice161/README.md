# Slice161 — authenticated new-voter interruption

Base `f7886bf7bc82ed0f16276c1cc1b20241b9c4816d` plus this commit's tests.
Linux local evidence. No production Rust, wire format, persistence format or
quorum rule changes. Public test credentials are separate from existing fixtures.

## Commands and results

```sh
cargo +stable test --locked --offline --all-features --test counter_service new_voter -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service -- --skip new_voter::
cargo +stable test --locked --offline --all-features --test connect
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable fmt --all -- --check
node validation/check-inventory.mjs
```

The new two tests execute four histories: TCP/QUIC × WAL/checkpoint, all pass
in39.86s. The35 remaining service tests pass in20.48s. Both strict Clippy profiles
have zero diagnostics; formatting and diff checks pass. Inventory metadata and
path validation passes89 records, without claiming protocol conformance from
metadata. Raw outputs and source/fixture hashes are retained alongside this file.

The14 connection tests also pass. The preceding revision's macOS job
[114107132231](https://github.com/KarlLivesey/VoteBoat/actions/runs/38016277101/job/114107132231)
passed the earlier QUIC mailbox correction but found a fixed-poll-count TCP
preface assumption. The fragmented-preface fixture now waits for observed
anonymous/handshaking states within two seconds, keeping virtual time at zero.
Its exact timeout/cancel outcome, ticket, queue and socket-close assertions stay
unchanged. This changes no production transport timeout. The failed job excerpt
is retained; corrected macOS execution remains unverified.

## What is checked

All configuration state comes from real executable commands. After retiring
original node3, the service adds learner4 with store404/incarnation7 and imports
its checkpoint through the existing enrollment command. The authenticated
promotion request is interrupted while readiness is pending and4 is offline.
The leader is killed, its original files are reopened, and the new learner's
return does not resurrect the absent request. Explicit identical resubmission
then commits joint configuration6 with target voters2/4 and retained learner1.

After4 observes committed joint state, it is stopped and its files inspected.
The WAL case retains the joint entry beyond the checkpoint; the checkpoint case
has the joint membership at its compacted base. Final configuration7 is submitted
while4 is absent: the selected node reports a durable accepted Final, committed
Joint and `wait_for_commit`. Voters1/2 alone cannot satisfy the new policy.
Reopening4 permits finalization through normal election/current-term progress.
The exact final record retries successfully, and all original operation IDs,
application receipts, exact voter/store identities and value43 survive another
complete cluster restart. Retrying the original value42 operation stays duplicate.

The shared test helper now honors the selected TLS directory. A separate static
four-node fixture uses matching DNS names and public P-256 credentials, with
fixed2020–2050 validity. `certificates.txt` records identities and validity.
The existing explicit leader retry helper is reused for admin and data calls;
the production CLI still stops on uncertain writes. Its `auto` mode searches
nodes1–3 only; these tests explicitly address the selected leader, including4.

## Limits

These are process interruption histories, not power-loss or arbitrary-fault
proofs. They do not hold a positive stale readiness proof across a replacement
incarnation. Broader revocation, checkpoint mismatch and combined fault coverage
remain in the full P0–P7 ledger.

The older full all-feature run was observed live again; `pending-baseline.json`
records its partial progress, not a passed result. It compiled before160 and
does not include these new tests. The two earlier broad routed runs are likewise
not replaced by this focused result. macOS requires actual CI execution.
