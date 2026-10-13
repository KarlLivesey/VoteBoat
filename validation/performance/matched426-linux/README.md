# Fresh Linux426 framework artifacts

Exact published source799f2cf300a50e004e74d07b6a9237f59003892b is exported into
an independent1418-input directory. `SOURCE.json`/`source.sha256` identify all
selected committed inputs, including five newly introduced test modules, both
locks, hook and Clippy configuration. There are22 changed426 inputs; the selected
scope differs from older inventories and is recorded explicitly.

Original build session3865 is terminal0 with STATUS0/STAGE DONE. A genuinely
fresh target compiles root library, executable, OpenRaft and bench_minimal with
Cargo `fresh:false`, opt3/debug2/debug_assertions=true/overflow_checks=true,
thin-LTO/codegen-units1; flags/wrappers are unset. Actual CargoJSON and verbose
rustc invocations, compiler hashes, source before/after and original/copied artifact
proofs are retained. Rust1.98.1/LLVM22.1.8 and Linux protoc3.9.0 match the425
Linux inputs. No dependency update or native-profile substitution occurred.

Candidate executable SHA256 is
e36086ec99160d70010dc2f4b46f04161fd0de410fc2460ffb9b708bea1d9b9c.
`ARTIFACTS.json` identifies every actual original/copy. Binary/build trees are not
archived. Functional qualification is separate in slice426/main and isolated;
this build alone makes no throughput or durability claim.
