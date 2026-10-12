# Mac native-only100-group closure

The exact unchanged effect_owner native100-group test passes1/0 in90.03s on
current402 source, normal unoptimized test profile, with the original per-drain
10-second guard. It includes initial100-group snapshot installation, proposals,
checkpoints, reads, original close, same-store reopen/new bindings, retry/dedup
and further writes/checkpoints/cleanup. Whole test file matches original315b.

The original315b179/1 native result remains failed. This selected execution
closes its named recovery gap; it does not claim the whole current native matrix.
1306 source checks pass before and after the case. The native executable itself
has no before/after fingerprint record, and no stronger custody claim is made.

After this pass, a wrapper quoting error stopped before both cold cases. The
original wrapper/error is retained. The separately syntax-checked continuation
verified this pass and source/release artifacts, then started only pending cold
TCP/QUIC. No successful test/build/lint was repeated.
