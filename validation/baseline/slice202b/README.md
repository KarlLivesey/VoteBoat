# Slice202b — live transfer credentials and interrupted split recovery

Starting revision07b9796. The transfer executable now shares the existing
Credentials owner and reload/status commands with the counter executable.
Preparation remains bounded and off-thread; the durable owner-bound journal
precedes generation publication. Both normal and failed drive exits finish
accepted credential work before the node releases its data-directory ownership.
Uncertain publication closes access and stops the service for recovery.

The new TCP and QUIC histories interrupt source-fence admission without a live
quorum, restart the group services, change administrator3 to reader and reader2
to administrator, then reload every group replica. Original reload retries are
idempotent and mismatched expected generations fail. The old principal cannot
advance the transfer or reload credentials but retains scoped inspection.
The new administrator resumes the unchanged profile; all-node restart retains
the completed split, original data/retry results and access decisions. QUIC
checkpoints through committed prefixes. Older-generation and altered same-
generation credential bundles are refused at startup for every role.

The interrupted cut demonstrates admission, not guaranteed pre-failure commit.
This changes command access only: no peer credentials, voter membership,
ownership protocol or application persistence format changes.

Evidence:

- before.log: the TCP regression fails against the old service because
  reload-access is an unsupported metadata command.
- revocation-initial.log: both new histories exposed an incorrect stdout
  assertion in the test; the CLI reports refusal on stderr. The assertion was
  corrected without weakening the required refusal.
- revocation.log: both final TCP/QUIC histories pass.
- transfer-all.log: all16 tests in the complete all-feature transfer target pass.
- counter-reload.log: both existing TCP/QUIC counter reload histories pass.
- worker.log: shared credential worker ownership/uncertainty checks in the
  transfer executable; both pass.
- transfer-default.log: all10 default-feature transfer tests pass, run after
  all-feature executable tests finish.
- clippy-initial.log and pre-push.log: strict lint and final formatting plus
  all four lint configurations.
- source.sha256 and source-check.log: final source and evidence identity.

```sh
cargo +stable test --locked --offline --all-features --test transfer_service revocation::
cargo +stable test --locked --offline --all-features --test transfer_service
cargo +stable test --locked --offline --all-features --test counter_service credential_reload::
cargo +stable test --locked --offline --all-features --bin voteboat-transfer credential_reload::
cargo +stable test --locked --offline --test transfer_service
sh .githooks/pre-push
```

Current macOS/separate-host acceptance, peer credential rotation, a full audit
history and broader lifecycle/fault combinations remain open. This evidence
does not certify arbitrary providers or full P0–P7 completion.
