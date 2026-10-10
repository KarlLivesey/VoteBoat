# Slice209j — executable peer discovery

The counter service can consume an independently pinned authenticated endpoint
source with `--peer-discovery FILE`. Native static, member and shared-group
startup use existing recovery and final Node assembly, including optional
record-checked peer credentials. Source progress and worker shutdown belong to
the driven connector. Idle source sessions detach without losing cache floors;
new sessions preserve the source/local binding and advance generation.

Schema and scope are recorded in `docs/IMPLEMENTATION.md`. Source discovery does
not create membership, install pins or persist cache floors. The source's saved
advertisement and independent client credentials remain host inputs. This is
not combined recursive-movement, macOS or full-roadmap acceptance.

## Executed evidence

- `counter-initial.log`: first three native TCP/QUIC executable histories pass.
- `counter-profiles.log`: all eight executable discovery tests pass, including
  member/shared-group key rotation and malformed/unavailable sources.
- `counter-checkpoint.log`: eight pass with strengthened QUIC assertions against
  actual persisted snapshot references covering each group's committed writes.
- `remote.log`: all28 remote-discovery tests pass, including the added host idle
  detach/cache/session-binding/stale-generation case.
- `regression-all.log`: final all-feature affected targets pass: counter152,
  member startup50, remote discovery28, startup43 (273 total). This includes the
  bounded upgrade I/O changes and the unchanged command/source regression cases.
- `counter-default.log`: all five applicable executable discovery cases pass
  with default features, after the all-feature processes finish.
- `native-only.log`: all25 applicable remote-discovery cases pass without TLS
  or QUIC, including the host cache/session test.
- `rustdoc.log`: `RUSTDOCFLAGS='-D warnings' cargo +stable doc --locked --offline
  --all-features --no-deps` passes.
- `pre-push.log`: formatting and all four strict Clippy profiles pass with zero
  diagnostics. `inventory.log` validates108 entries; `metadata.log` passes the
  thirteen provider-ledger metadata checks. These are not provider certification.

Socket tests use the approved local runtime permissions. Feature configurations
that build executable fixtures run sequentially. Initial compile failures are
retained: a re-export visibility mistake and methods unused in the directory
binary, followed by two function-size findings. Shared field access and focused
mode/open helpers corrected the causes; no lint thresholds were weakened.
