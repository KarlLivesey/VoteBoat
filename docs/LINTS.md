# Rust checks

Run `cargo +stable fmt --all -- --check` and
`cargo +stable clippy --locked --offline --all-targets --all-features -- -D warnings`.
Repeat Clippy with `--no-default-features` for core-only code. Unsafe code is
forbidden. CI runs these checks independently of the Linux/macOS test matrix;
no required branch checks gate ongoing implementation.

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
