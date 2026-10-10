# Local checks

Run from the repository root. Formatting and strict Clippy must report zero
diagnostics; the enabled `.githooks/pre-push` checks default, all-feature,
no-default-feature and native-only builds.

```sh
cargo +stable fmt --all -- --check
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
```

For the full functional suite, run feature configurations sequentially. Some
integration tests start executables from the shared `target/debug` directory.

```sh
(
  set -e
  if [ "$(uname -s)" = Darwin ]; then ulimit -n 4096; fi
  cargo +stable test --locked --offline --all-features
  cargo +stable test --locked --offline --no-default-features
  cargo +stable test --locked --offline --no-default-features --features native
)
```

The limit applies only to that subshell and its children. macOS SSH shells can
inherit a256-file soft limit. A three-node/100-group snapshot fixture holds300
exclusive store-lock descriptors, plus WAL and transport handles; it cannot run
within256 even alone. The4096 limit leaves room for concurrent fixtures without
removing locks or reducing group counts. If the host hard limit prevents this
command, report that resource constraint rather than claiming a passing suite.
Normal small deployments need fewer handles; budget one snapshot-store lock per
local group plus provider/transport resources. No system-wide limit is changed.

The effect-owner suite admits one large disk fixture at a time. Each fixture
still runs three concurrent nodes with100 groups and real workers/network IO;
the harness continues running other tests concurrently. This avoids mixing six
independent snapshot initialization workloads into a single functional deadline.
Those deadlines and assertions stay intact. It does not validate performance at
that combined disk load; performance measurement uses separate declared workloads.
