# Slice209h — member and shared-group discovery preparation

Starting revision: 520b7eca2ad08adcb9c8969760521aad7808e464.

NativeMemberStartup and NativeMultiStartup expose the existing native prepared
parts for host discovery composition. Member startup retains Recover-only
validation; multi-group startup retains one shared WAL and the exact original
group/application inventory. Existing open/rotation paths still use configured
addresses. Explicit discovery preparation enables discovered QUIC Dial addresses
without changing peer pins or Accept routes. Final Node construction and cleanup
ownership remain with the host.

Six added tests cover member recovery over TCP/QUIC, three shared groups with
distinct data and reused operation IDs across verified checkpoint recovery,
invalid member Create and missing applications. Existing preparation rejection,
abandonment and worker/source cleanup checks remain active.

Evidence:

- clippy-initial.log records an intermediate test edit before the Member and
  checkpoint cases were added: the then-unused variant/method were diagnosed.
  clippy-final.log passes after the tests were completed.
- discovery-all.log: all11 discovery startup tests pass.
- startup-all.log: all50 member and43 startup tests pass before the fixture fix.
- startup-default.log:29 default startup tests pass, one reports AddrInUse.
  The discovery fixture now retains each unused TCP/UDP reservation until that
  node's preparation instead of releasing all ports before any store opens.
- startup-default-fixed.log: all30 default startup tests pass.
- startup-final.log:49 member tests pass; the existing QUIC restarted-learner
  readiness case times out at term3/config10. The fixture had stopped healthy
  voter polling while draining the learner and reopened it at time zero.
  It now drives healthy voters through learner shutdown, asserts leader/term
  continuity and reopens at current host time. Original stale-readiness and
  committed joint/final assertions remain unchanged.
- member-restart-fixed.log: both TCP/QUIC restarted-learner tests pass.
- startup-fixed-final.log: all50 member and43 startup tests pass after both fixes.
- default-final.log: all27 default member and30 default startup tests pass.
- checks-fixed-final.log: formatting plus default/all-feature/core-only/native-only
  strict Clippy pass with zero diagnostics.
- inventory.log:108 contract inventory paths pass; metadata.log:13 obligation
  metadata checks pass. No new provider or reviewed obligation family is claimed.
- source.sha256 and source-check.log record the changed source/evidence hashes.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test startup discovery:: -- --nocapture
cargo +stable test --locked --offline --all-features --test startup --test native_member_startup
cargo +stable test --locked --offline --all-features --test native_member_startup native_promotion_refuses_restarted_learner_readiness -- --nocapture
cargo +stable test --locked --offline --test startup --test native_member_startup
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

Runtime feature configurations ran sequentially with local socket permission.
These are Linux process/reopen histories, not macOS/separate-host or physical
power-loss validation. Automatic executable source bootstrap, combined recursive
movement and the full P0–P7 acceptance obligations remain open. No durable
discovery cache, trust distribution, consensus or persistent-format change.
