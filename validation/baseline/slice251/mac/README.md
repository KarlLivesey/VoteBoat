# Slice251 — terminal original Mac full suite

The original source-preserved Mac full all-feature run is now terminal, all=101:

```sh
cargo +stable test --locked --offline --all-features --no-fail-fast
```

Across90 result summaries it reports1779 passed,46 failed and zero ignored.
Four targets fail: quic13/2, routed135/36, transfer_service27/6 and the native
benchmark example32/2. [Exact target/test names](terminal-summary.json) and
[full failure bodies](all.log) are retained, with a [46-panic index](failure-index.json). These are original-source failures,
not results for current main or the corrected269/273 images.

All647 original inputs verify before and after execution and match the
[original Linux251 manifest](../linux/build-source.sha256). The environment
records the Mac checkout's older Git HEAD separately; the build-input manifest
is the authoritative code image. Installed stable Rust1.98.0, macOS27/arm64
and the4096 descriptor limit remain unchanged. The original formatting/four
strict profiles finish zero; that does not make the failed suite acceptable.

Later retained-credit/root-collision/metadata-read corrections have their own
source-bound targeted evidence. The corrected full69-history Mac269 image is
still live with independently reported failures; current273 checks exact
replica-bound application recovery. Neither selected fixes nor Linux251's1826/0
result erase these original failures. Broader Mac functional acceptance and
P0–P7 remain open.

Published artifacts normalize only the original checkout path/trailing spaces.
Raw and normalized hashes are separate. Failed original stores remain retained
on the Mac; this collection does not alter them.
