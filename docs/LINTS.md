# Rust checks

Run `cargo +stable fmt --all -- --check` and
`cargo +stable clippy --locked --offline --all-targets --all-features -- -D warnings`.
Repeat Clippy with `--no-default-features` for core-only code. Unsafe code is
forbidden. CI runs these checks independently of the Linux/macOS test matrix;
no required branch checks gate ongoing implementation.

`clippy.toml` pins the usual limits: 7 arguments, type complexity 250, cognitive
complexity 25, and 100 lines per function. The first two belong to the standard
Clippy checks. Cognitive complexity and function length are opt-in and currently
reported in a separate CI audit: existing violations are not silently exempted,
but that audit is not yet an enforced zero-warning gate. This is explicit debt,
not a claim that every function already meets those limits.

Lint cannot validate module boundaries, failure recovery or composable contracts.
Those still need the schema plan, review and conformance tests required by AGENTS.md.
