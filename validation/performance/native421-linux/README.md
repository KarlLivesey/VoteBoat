# Linux native421 artifact

Fresh asserted native release built from exact published1091610, using the Mac421
1396 Git-blob inventory plus37 ready-slice source hashes. Before/after whole-map and
exact compiler-source guards pass. Root library and native_benchmark records are
fresh:false, opt3/debuginfo0/debug_assertions=true/overflow_checks=true. Actual root
rustc lines preserve default native LTO/codegen; framework overrides were unset.

Final original runner7535 is terminal0. Immutable executable copy:
`/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native421-linux/artifacts/native/native_benchmark`
(SHA256 b6df24e8240a3d7a9bf394f2deb49829f1ff8e9c35a91e77f5a07c1a6e4d289d).
Root rlib hash bb1a5dc1c6cdcd59e62292f50a6fe13ffe8fe852591437a19970ef4552fd0f8c.
BUILD.json/ARTIFACTS.json retain original/copy paths and actual compiler records;
SOURCE.json/source.sha256 retain the exact frozen sourceRoot and full input map.
No source checkout, binary, rlib or target tree is published.

Initial runner75825 failed101 because explicitly selected systemprotoc36.1 is rejected
by the reference test dependency's pinned protobuf-build major-version check. Original
failed target and raw logs remain locally and failure proof is published. Its existing
reviewed bundledprotoc3.9.0, used by prior automatic Linux fallback, was verified and
selected explicitly for a whollyfresh qualified target. No install/source change or
unchanged restart occurred. LinuxRust1.98.1/bundledprotoc3.9.0 differ from MacRust1.98.0/
protoc3.21.12; each future quantum comparison uses the same artifact on its own host.

This job runs no tests or timings. Existing root398, real services8/flag controls8,
full hook and Mac421 qualification cover correctness. No performance claim follows
from artifact qualification. Full compiler JSONL remains at the hashed local paths
in BUILD.json; this small publication retains the exact root compiler records,
verbose rustc commands, source/tool/profile guards and original error logs.
