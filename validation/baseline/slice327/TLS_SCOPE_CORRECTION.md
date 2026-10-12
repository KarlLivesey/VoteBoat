Frozen initial matrix: 24 invocations, 64 behavioral tests; two native-only cfg-disabled targets executed 0 and are not TLS acceptance.
Correction: only the two previously unrun --no-default-features --features tls targets run separately with source/runner/terminal guards. Combined 26 invocations expect 66 actual behavioral tests.
The initial failed audit and original native-only 0 logs are preserved; no successful test or strict profile repeated.
Audit parser also accepts Node actual dot-summary ℹ or TAP # lines for the 33 negative controls; original frozen audit and runner hashes remain preserved.
