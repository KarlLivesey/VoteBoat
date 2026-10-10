# Slice207b3 — transfer and directory peer credential rollout

Starting revision35987b4. The remaining executable families compose the same
bounded peer loader, durable preparation worker and native credential publisher
as counter. Transfer uses the actual profile group and directory its authority
group for authorization. Command credentials remain separate; directory now
uses the existing command-access adapter as well. No new public contract,
dependency, journal format or consensus behavior is introduced.

Four new all-feature process histories pass: transfer TCP/QUIC and directory
TCP/QUIC. Transfer selects the retirable-source split profile with metadata,
source and two targets; stages retain their original operation identities,
data and retries through rotation and restart. Directory checks committed
manifest lookup and duplicate original publication/initialization receipts.
Each exercises changed CA/cert/key material, a flushed-but-unread request,
durable record observation followed by process loss, original-ID status/retry,
and stale/changed/omitted startup refusal. QUIC includes checkpoint recovery.
Transfer checks invalid preparation with the current generation retained;
directory checks independent command-access replacement and restart.

The tests deliberately flush a command without reading its reply. Observing
the local record then killing its process proves recovery after an unread reply;
it does not prove that in-memory publication never happened. The shared worker
tests separately hold preparation and join an unpublished result at shutdown.
These are selected process/file histories, not hardware power-loss or macOS
acceptance. Automatic cluster rollout and secret distribution remain external.

Evidence:

- peer-initial.log:4 new process histories pass.
- unit.log:13 executable unit checks pass, including the reused uncertainty,
  single-flight and shutdown/publication assertions in both binary assemblies.
- all.log: all50 affected all-feature regressions pass (13 executable unit,
  19 directory service and18 transfer service tests). The two existing deep
  QUIC split/merge histories run beyond60 seconds and both complete successfully.
- default.log:6 separate default-feature peer histories and unit checks pass.
- pre-push.log: formatting and all four strict Clippy feature profiles pass
  with zero diagnostics.
- voteboat-207b3-lint-initial.log / voteboat-207b3-lint-second.log: retained intermediate transfer
  function-size failures (102, then101 lines); direct application construction
  removes redundant temporaries without changing branches or lint thresholds.
- voteboat-207b3-lint-third.log / lint-initial-tests.log: passing all-feature
  lint snapshots after the code and test changes respectively.
- inventory.log / metadata.log:106 contract entries and13 obligation metadata
  checks. The shared conformance ledger still reviews5 contracts/35 operations;
  101 entries remain unreviewed by that ledger.
- source.sha256 / source-check.log: final source and evidence fingerprints.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test transfer_service --test directory_service peer_credentials -- --nocapture
cargo +stable test --locked --offline --all-features --bin voteboat-transfer --bin voteboat-directory
cargo +stable test --locked --offline --all-features --bin voteboat-transfer --bin voteboat-directory --test transfer_service --test directory_service
cargo +stable test --locked --offline --bin voteboat-transfer --bin voteboat-directory --test transfer_service --test directory_service peer_credentials
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```
