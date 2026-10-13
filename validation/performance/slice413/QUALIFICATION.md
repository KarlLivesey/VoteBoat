# Configured413 focused qualification

Working-root source after 8d50b0e; no frozen 411/412 snapshots modified.
CARGO_TARGET_DIR=target/p9/configured413/checks, configurations sequential.

- all-feature lib configured:: shared6 + native2: 8 passed, 0 failed
  (terminal21340 exit0; shared-native-all.log)
- all-feature lib configured_deltas:: Core2: 2 passed, 0 failed
  (direct command exit0; core-all.log)
- no-default lib configured: shared6 + Core2: 8 passed, 0 failed
  (terminal63391 exit0; shared-core-only.log)
- all-feature public log_store configured:: power-loss control: 1 passed,
  0 failed, four fault phases (terminal10320 exit0; power-loss-all.log).
  Exact before for torn append/sync failure, exact oracle after for synchronized
  PublishBefore/PublishAfter; original instance fenced, recovered session new,
  all old-session tickets refused. Device.power_loss discards unsynced bytes.
- strict --lib --tests all-features: zero diagnostics, terminal31134 exit0
- strict --lib --tests no-default: zero diagnostics, terminal4924 exit0
- strict --test log_store all-features after additive power-loss control:
  zero diagnostics, direct command exit0 (clippy-power-loss.log).

Preserved initial missing SnapshotRef import and process-reopen fixture failure
logs. These were test-only corrections. The Memory fixture retains written bytes
on reopen after a sync failure, so the internal uncertainty control allows only
exact before or complete oracle-after state and still forbids receipt/fenced use.
The additional public ModelIo control independently exercises actual power loss
with a synchronized byte frontier and phase-specific exact expectations.

No Mac tests, broad repository qualification or performance claims here.
Root owns final four strict configurations, affected broad tests, publication
and performance measurement. Sources frozen at OWNED_SOURCES.sha256.
