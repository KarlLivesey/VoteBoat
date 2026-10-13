# Mac candidate qualification and provenance

The final test-only overlay on immutable 306b26b production passed eight matched
tests, formatting and strict all-target Clippy. Four CLI controls used one client
worker and 16 server workers, verifying 16,384 originals, 4,096 requests and all
three frontiers at 17,409. Maximum original width was 4,096 in the primary profile
and 64 in Common64; histograms include 17,408 originals with warmup.

Unmodified native 306b26b passed 335 all-feature library tests, 266 core-only tests
and the exact ModelIO large-batch publication/recovery test. Root formatting and
strict Clippy for default, all, core and native configurations exited zero.
The export omitted clippy.toml: Linux's native 1,359-input manifest includes it;
all 1,358 shared Rust, manifest, lock, fixture and license inputs match exactly.
Strict Cargo lint levels still applied; the original checks used Clippy defaults.
The missing file declares 25/100/7/250 maintainability thresholds. A separate
explicit-config rerun subsequently passed all four root profiles plus matched
all-targets, using CLIPPY_CONF_DIR without changing guarded compiled source. Its
configuration is byte-identical to 306b26b's git blob, SHA256
`6609f9c70e8f11b54a29637838fef0ad5cf28cd31e9085245996a36dc1edb521`.
Environment, commands, zero exits and unchanged source guards are preserved under
explicit-mac-lint. The original omission remains recorded; future source exports
must include clippy.toml.

The matched harness uses optimization 3, assertions and overflow checks enabled,
thin LTO and one codegen unit. The accepted native release uses optimization 3,
assertions and overflow checks enabled, with the baseline's default LTO/codegen
tuning. Both have fresh:false executable and production-library compiler records.
Source manifests passed before and after. The harness uses the cfg(test) overlay;
the native build uses unmodified 306b26b.
No release behavior was changed by the fixture correction. The initial and
pause-ack-only failed attempts remain separately archived.

Harness SHA256:
`df6f1fc5c122a30034ec5d6ba0fd42ca1099121f1558392700698072ba9ee9fd`

The first native release incorrectly inherited harness thin LTO/codegen 1; its
SHA256 `1db48936a587ebc2857b80cd4ac36a35fed81e7da0561fd1d4965f64a858ba23`
is retained as invalid for paired native-profile comparison. No native timing
used it. A genuine fresh native-only rebuild unset both overrides, matching the
native baseline's default tuning while retaining optimization 3 and enabled
assertions/overflow checks.
Captured rustc commands show no LTO/codegen override. Source was unchanged and
all 1,358 before/after guards passed; source tests were unchanged.

Matching-profile native benchmark SHA256:
`e7e248b25db86f3d990f6e5f7edbfd64fbfb659ccf4d6e1a02e2aa929f15e4bc`

Matching-profile native production-library SHA256:
`8f018f39f83e2adff68e9f9ad28e68cf0c1aa583d2b1205aa27b97a186f7ddf1`

Baseline native production-library SHA256:
`0364c0f2db30d5c0733afa17af2ed9dc0a674cc9ad14dacb1bbfc718c3d6c03c`

Original compiler paths, toolchain, locks, profiles, hashes, source guards, raw
tests/Clippy and verbose builds are under artifacts-mac. No executable or checkout is
included. Matching native proofs are under native-mac-profilematched. Mac short
matched timing passed all 18 exact cases and guards, archived in mac-paired131072;
sustained/margin qualification remains separate.
