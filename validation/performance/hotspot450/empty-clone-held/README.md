# Empty command clone-call hypothesis held

The exact450 Linux executable SHA70ae8c2751b3c12e697bc93ee9c83bef5f833fe2b00f22cbb3623e31c9c76945 already skips both allocation and memcpy when command byte length is zero. Its append_for→to_vec→LogEntry::clone caller path reaches that guard. The separately inlined Vec<LogEntry>::clone also guards its payload allocation/copy with a zero test. This hypothesis was rejected before any manual clone edit, build or measurement.

RECEIPT.json records exact artifact-relative addresses, observed commands and instruction facts. It contains copied excerpts from completed tool output, not an invented full raw disassembly. Initial objdump emitted missing-DWO warnings; --dwarf=no-follow-links allowed clean instruction inspection of the same artifact. The full initial warning stream was not saved locally. No symbols or artifact bytes were changed, no old address transferred to a new binary and no recapture/build/test was performed.

These static facts do not attribute broad libc-copy samples to payload cloning, prove an allocator cause or predict speed. Mac instructions were not inspected. Source remains derived Clone with Vec command bytes; immutable payload work remains held. SHA256SUMS covers these two regular files only.
