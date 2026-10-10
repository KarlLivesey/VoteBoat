# Slice211 — Final membership before split import

Four new native histories finalize source membership after its durable split
fence, before either target imports. They cover TCP/TLS and QUIC with WAL-only
recovery or an actual retained Joint checkpoint plus the later Final WAL suffix.
Four existing after-activation histories use the same finalization path.

The old source base stays unchanged through abort/drain/worker-join/reopen. Its
base membership is exactly Joint2 (or bootstrap1 without a checkpoint); accepted
and committed membership reconstruct Final3. Every source's original-operation
status retains the exact positive Final receipt index/term, and node1 is learner.
Frozen source state and export images remain byte-identical. The public split
operator then imports, publishes and activates both scopes; each phase exercises
source write refusal and inactive/active target serving. Child writes and original
retries work with all ancestors/source closed. Child cold recovery preserves
exact values, duplicate receipts and independently checked outbox counts.

Executed evidence:

- `membership-before.log`: original four after-activation cases pass; all four
  new cases reach activation then fail a reused hard-coded import configuration2
  assertion. That field comes from the fresh quorum observation, so before-import
  Final3 correctly yields3. The original immutable fence/export checks remain.
- `membership-compile.log`/`checks-compile.log`: the added exact-receipt assertion
  initially names ProposalPosition in raft; its actual public namespace is
  runtime. The focused namespace correction introduces no protocol change.
- `membership-all.log`: all8 corrected native membership/split histories pass.
- `membership-default.log`: all4 default TCP histories pass after the all-feature
  executable counter run has ended.
- `clippy-all.log`: initial structured-history all-feature lint passes.
- `checks.log`: formatting and all four strict Clippy configurations pass.

This is selected process-abort/checkpoint/WAL recovery evidence, not physical
power-loss, arbitrary new-voter/revocation schedules or full P4/P6 certification.
No production provider, persistence format, timer or consensus rule changes.
The complete P0–P7 goal remains active; P8/Windows stay deferred.

Reproduce in a socket-enabled environment, with executable feature builds
sequential:

```sh
cargo +stable test --locked --offline --all-features --test routed split::membership
cargo +stable test --locked --offline --test routed split::membership
sh .githooks/pre-push
```
