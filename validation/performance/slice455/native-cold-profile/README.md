# Native cold recovery debug profile

Existing padded CommandBytes debug QUIC two-level cold-recovery test exited101 after598.831s (user556.520s/system5.327s, maxRSS126512KiB). This is a failed functional diagnostic, not throughput acceptance. The first launch exited127 because `/usr/bin/time` was absent; its separate records remain unchanged.

User-only cycles at99Hz, DWARF8192 and mmap128 recorded58240 samples; no system policy change. Saved flat report ranks range forward24.18%, range next21.12%, `format_crc::crc32c_extend`14.72%, iterator next7.78%, slice next2.49%. Inclusive text has the same leading values; overlapping entries are not summed as CPU time. No allocator or protocol failure cause follows from these ranks. Capture includes the whole debug test and cleanup; unresolved native addresses and raw warnings remain.

`RAW_REFERENCES.json` preserves SHA256 and byte sizes for raw perf data and full reports retained in ignored target (491108420-byte primary capture); no large capture, binary/build tree or padded full report is committed. `SOURCE_BEFORE_CRC.json` is the existing selected pre-change source inventory; `IDENTITY.json` records its subset and timing limits alongside the actual before-artifact hash. This profile precedes the CRC candidate and is not a new-CRC performance measurement.

The exact pre-CRC profiled ELF is preserved separately in ignored target from perf's build-ID cache, with SHA256 verified against the pre-capture artifact. Its checkout path now contains the post-CRC test binary; use the recorded old artifact for replay. No binary is committed or transferred.
