# Slice208b1 — shared receive-credit fairness

Starting revision85bbf12. PeerDriver previously left its cursor after the last
scanned peer, including peers refused by a full IngressRouter. A full scan
returned to the same starting peer; when dispatch freed only one frame's
capacity, that peer could claim it again indefinitely.

Two new host-provider tests reproduce this with background-frame and
data-message limits. Each makes both peers continuously ready, uses full peer
scans and interleaves zero-visit polls. The original driver admitted eight frames
from one peer and zero from the other. After the fix, each gets four. The scan
resumes after the last successful receive; rejected frames remain transport-owned.
No resource ceilings, reserves, public contracts or persistent state change.

A third test holds background/recovery input in ingress, refuses its next frame
without extracting it, admits another peer's control frame using the reserve,
completes its original outbound send, then dispatches held input and admits the
previously refused frame. Usage stays bounded and close/drain finishes. These
host cases exercise the real PeerDriver/IngressRouter/EffectOwner admission path,
not application of their synthetic snapshots. Commit index stays zero. Native
replicated progress under the combined pressure schedule remains208b2.

Evidence:

- before.log: both original regressions fail, with receive counts [8, 0].
- after.log: both pass after the cursor change.
- focused.log: all3 receive-fairness/held-recovery cases pass, core-only.
- core.log: all208 core-only tests pass: effect_owner169, peers18, runtime21.
- all.log: all249 all-feature tests pass: effect_owner175, peers18, runtime28,
  transport28. This includes existing hundred-group, WAL/snapshot recovery,
  shared-buffer, native quota-pressure and reconnect histories.
- pre-push.log: formatting plus strict Clippy for default, all-feature, core-only
  and native-only builds pass with zero diagnostics.
- inventory.log / metadata.log:106 inventory paths and13 obligation metadata
  checks pass. Metadata checks are not provider certification.
- source.sha256 / source-check.log: source and evidence fingerprints.

Commands:

```sh
cargo +stable test --locked --offline --no-default-features --test effect_owner saturated_
cargo +stable test --locked --offline --no-default-features --test effect_owner receive_fairness
cargo +stable test --locked --offline --no-default-features --test effect_owner --test peers --test runtime
cargo +stable test --locked --offline --all-features --test effect_owner --test peers --test runtime --test transport
sh .githooks/pre-push
node validation/check-inventory.mjs
node validation/check-provider-conformance.test.mjs
```

The first command intentionally failed before the fix. Configurations ran
sequentially; native socket regressions ran with loopback access. This advances
P2/P7 admission fairness; it is not full platform/fault or performance acceptance.
