# Mac401 diagnostic-clock qualification

Exact committeda399346 source,1306 guarded inputs, selected public injection1/0
and native signed-epoch/shared-monotonic controls2/0. Build and every selection,
source, artifact, protoc and cache-clone guard exit0; terminal failures=0.
Actual Cargo records show normal debug profile, opt0, assertions/overflow and
all features. Original runtime/lib-test binaries and library remain unchanged
across both selections. This is a targeted new-clock qualification, not another
full Mac matrix or any performance claim.

Source402 changes only the unrelated nested-test watchdog; all three401 clock
Rust files remain identical. Source/body comparisons are separate from execution.
