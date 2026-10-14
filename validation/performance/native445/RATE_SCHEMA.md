# Native445 printed-rate verification

The first Linux readiness workload succeeds; its checker fails because the
six-decimal elapsed104657 microseconds and three-decimal rate1223.047 represent
rounded values.128/0.104657 differs by0.0041, despite compatible underlying values.
Preserve the original checker failure and validate the same output after fixing
the arithmetic. No workload/build repeat or ignored checker bypass.

Add one pure checker helper for the existing printed fields. Parse the specified
decimal precision into integer microseconds/milli-originals per second. Require
positive values and exact overlap of their half-unit rounding intervals with
count/time, using BigInt cross multiplication. This has no arbitrary tolerance;
long runs also retain the rate's half-milli-unit bound. Malformed fields or
incompatible intervals fail. All receipt/window/latency/recovery/storage checks
remain. No benchmark, timer, artifact, source profile or completion changes.

Ownership is local immutable strings/count; no asynchronous state, persistence,
cleanup, restart or provider seam. Focused tests cover the actual failure, an
exact rate, incompatible rates/times and malformed/nonpositive fields. Run the
corrected full checker on the two existing Linux readiness outputs. Copy the
checked validator source separately from frozen artifact inputs on Mac; validate
its original outputs once. Publish the correction through the full enabled hook.
