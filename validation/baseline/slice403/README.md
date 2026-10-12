# Requirement audit and closure updates403

The archived71-row audit pins149 cited inputs at9d3e863. Root verified all149
hashes before making the status updates below. It is a scoped evidence review,
not a complete P8 certificate or universal correctness proof.

Subsequent root-inspected evidence:

- Mac398 is terminal: repeated TCP1/0 and QUIC1/0 pass with original15-owner
  cleanup. Both original cold cases fail final publication under15s. All raw
  records and guards are in ../slice398/mac.
- Mac401 exact source passes public clock injection1/0 and native signed/shared
  epoch controls2/0 with source/artifact guards. Q-CLOCK-MAC is now closed; raw
  evidence is in ../slice401/mac. The initial audit retains its earlier status.
- Q-100-MAC stays open: original Mac315b native179/1 failure is preserved here.
  The entire effect_owner test file is unchanged. A current exact native-only
  check is prepared as part of the Mac402 sequence; no old cause is inferred.
- Q-DEEP-MAC and final source reconciliation remain pending. No P9/P10 work.

Later update: Q-100-MAC is closed by the exact unchanged native-only case,1/0
in90.03s with the original per-drain watchdog. See [native100](native100/README.md),
including the preserved runner error and bounded continuation details. Only
Q-DEEP-MAC and final source reconciliation remain.
