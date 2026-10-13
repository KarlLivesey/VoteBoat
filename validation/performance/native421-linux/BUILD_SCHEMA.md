# Linux native421 build identity

Purpose: fresh default-native profile matching the immutable Mac421 source/profile
for same-artifact q16/32/64 measurements; root owns campaign schema and timing grants.
No performance run or repeated correctness qualification is part of this job.

Inputs are1396 Git blobs from1091610, exact Macqualification SOURCE inventory plus
37 ready-slice source hashes. No source from live422/423. Before/after guards require
exact paths/content, clippy config/locks and actual compiler-record source paths.
Native release uses opt3/assertions/overflow, defaultLTO/codegen; framework overrides,
RUSTFLAGS/wrappers are explicitly unset. Fresh source-specific target never reused.

Actual root library/example must be fresh:false and share required profile/features,
source/manifest/package identity. Original rlib/executable hashes and identical
read-only copies are retained locally; only proof metadata/logs/maps are published.
Failures preserve raw commands/status and original job handles; no unchanged restart.
Final Linux Rust1.98.1/bundledprotoc3.9.0 differ from MacRust1.98.0/protoc3.21.12; each host's future
quantum comparisons use its same immutable artifact. Compiler/host/versions recorded.

Initial runner75825 failed101 when explicitly selecting systemprotoc36.1; the
pinned test-oracle protobuf-build rejects major versions other than3. Existing
reviewed dependency-bundled protoc3.9.0 (prior automaticLinuxfallback) is verified,
selected explicitly and recorded. No install/source change; original failed target
and raw logs remain, while qualified target is fullyfresh and not reused.
