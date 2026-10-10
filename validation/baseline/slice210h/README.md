# Slice210h — preserve original drain preparation through role changes

The completed a3822c4 Ubuntu counter run passes156/fails2. Both retirement
histories fail during shared membership_drain::prepare, before their retirement
cut: a sampled leader's move-leader19750 returns the exact LeadershipChanged
unknown. The raw job is retained in `ci-ubuntu.log`.

Preparation now resamples authority while preserving operation19750,
configuration1 and the originally selected target node/store/incarnation. If
that required source already leads, it preserves the existing setup success
condition rather than creating a self-target handoff. Otherwise exact known
role-loss replies permit a fixture-owned caller retry. Positive records must
contain all exact original target/operation/configuration fields. Unrelated
errors remain terminal and the existing15s preparation timeout check is retained.
The CLI, production uncertain-write policy and downstream drain/retirement plans
are unchanged. Source leadership is not inferred from a successful handoff reply.

Executed evidence:

- `drain-all.log`: all30 native drain histories pass, including both recorded
  TCP/QUIC retirement cases, shared preparation consumers and partial-recovery
  single/shared-group runner histories.
- `counter-all.log`: the complete affected all-feature counter target passes158.
- `drain-default.log`: all22 default TCP drain histories pass, sequentially after
  the all-feature executable run becomes terminal.
- `checks.log`: formatting and all four strict Clippy profiles pass with zero
  diagnostics. The pre-push hook remains enabled.

The recorded current macOS job is still live at `ci-final-observation.json`.
`ci-post-local.json` subsequently records its terminal failure, retained in
`ci-macos.log`:143 passes/15 failures, mostly QUIC group liveness and command
interruption. Those are actual unresolved failures, not passing acceptance.
Neither the preceding CI failure nor local selected passes establish supported-
platform acceptance. Combined membership/split coverage is in slice211; the full
P0–P7 goal remains active.

Reproduce sequentially with local socket access:

```sh
cargo +stable test --locked --offline --all-features --test counter_service drain_
cargo +stable test --locked --offline --all-features --test counter_service
cargo +stable test --locked --offline --test counter_service drain_
sh .githooks/pre-push
```
