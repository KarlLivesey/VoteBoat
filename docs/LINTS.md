# Rust checks

The enabled pre-push hook and background CI check formatting, then strict Clippy
for four configurations: default (native TCP/TLS), all features (including QUIC),
no default features (core only), and native without TLS. These cover all distinct
feature sets in the current dependency graph. Each uses all targets and denies
warnings.

```sh
cargo +stable fmt --all -- --check
cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
```

These compile different conditional paths. Slice184's default check caught an
unguarded QUIC enum reference that both other profiles missed. Tests are run
separately; the push hook runs formatting and all four lint builds above. The
native-only build caught a TLS-only startup helper compiled without its caller;
that helper now has the same feature gate as its caller. Unsafe code is forbidden.
CI is background feedback, with no required branch check blocking implementation.

`clippy.toml` pins the usual limits: 7 arguments, type complexity 250, cognitive
complexity 25, and 100 lines per function. The first two belong to the standard
Clippy checks. Cognitive complexity and function length are explicitly denied
in Cargo.toml, so they also fail plain `cargo clippy`. All four are errors under
the same local/CI `-D warnings` commands;
there is no warning-only audit or cap-lints override. Existing findings therefore
fail this check until corrected. CI remains background feedback rather than a
required merge check; that does not turn a failed lint run into a pass.

Lint cannot validate module boundaries, failure recovery or composable contracts.
Those still need the schema plan, review and conformance tests required by AGENTS.md.
