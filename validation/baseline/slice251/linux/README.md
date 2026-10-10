# Slice251 — terminal Linux full suite

The source-preserved Linux run at ba67c8c finished successfully:

```sh
cargo +stable test --locked --offline --all-features --no-fail-fast
```

All1826 tests passed, zero failed or ignored. There are90 successful test-result
summaries including zero-test targets and documentation; this is not a count of
90 nonempty test targets. The routed target's171 cases and later targets finish.
Raw stdout/stderr and terminal all=0 are retained. Formatting and four strict
all-target Clippy profiles also finish zero. Installed Linux stable Rust1.98.1
and the existing environment/hook condition are recorded, without host changes.

All647 original build inputs verified before the run and again after completion.
build-source.sha256 is the accepted slice250 manifest. No original build input
changed while child executables ran. The attached implementation checkout stayed
separate. Production src/Cargo.toml/Cargo.lock are identical through the published
17ba335; later application-conformance and QUIC test fixture changes are not full-
suite-verified by this older source result.

The original Mac run remains live on its source-preserved checkout, with reported
QUIC/routing failures. Its pending full target cannot inherit this Linux result.
Current254 routing experiments are unaccepted and uncommitted; their failed and
controlled negative checks remain task-local, with zero lint diagnostics. This
artifact proves the named finite Linux suite, not Mac acceptance, all feature
configurations, arbitrary-fault/provider certification or P7 performance gates.
Full P0–P7 stays active; features/functional Mac/Linux work precedes tuning/security.

Published logs normalize the primary checkout path and trailing whitespace.
raw.sha256 records exact original unnormalized local files. production-diff.txt
is empty and refers to the Git sourcebase/published source comparison, not the
uncommitted fixture candidate. Source hashes bind actual build inputs separately.
