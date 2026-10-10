# Isolated cross-authority history

The unchanged TCP/WAL cross-authority insertion history passes1/0 on Linux
(61.25s) and macOS arm64 (358.68s). This is an explicit non-reproduction of
the failure reported by the separate live65-history Mac run, not a correction
or a broad pass. Original91576fa/653 build inputs verify before and after on
both hosts. No retry, predicate, quota, deadline or fault cut changes here.

The Mac uses its own checkout/cache, child-shell4096 descriptors and task-owned
TMPDIR. Successful histories keep their existing explicit worker/store cleanup;
failed stores from the original runs remain. Raw.sha256 hashes the input bytes;
published.sha256 hashes the path-normalized text with trailing whitespace removed.
