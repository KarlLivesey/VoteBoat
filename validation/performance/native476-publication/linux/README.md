#476 fresh Linux native artifact and release controls

Published source `7e9068296ce00dde5bf3e03bf4d852ed2833d185`; source export97792, fresh build26237 and actual release controls30285 all terminate0. Collection verifies local HEAD/origin-main equal that published commit and all1491 qualified inputs within1492 exported files. File modes and326 observed directory modes match the source freeze. Original/copy root-library and executable hashes match. Cargo JSON/verbose rustc prove root library, example and root build-script fresh:false.

Linux stable rustc1.98.1 (LLVM22.1.8), x86_64-unknown-linux-gnu, protoc3.9.0; compiler/tool hashes match the475 within-host reference. Native opt3/debug0/assertions+overflow/defaultLTO/defaultcodegen is separate from the framework profile. The release checkpoint debug oracle is absent.

Executable SHA256: `0bf40f6b4547c446f9db2a86a4bffe6d9ab223640d470154ec266a0d7cc2d064`

Root library SHA256: `a72d441edb01d4b080f51ff12b57558556cf08a7fe92a437543d50ea53130f96`

ELF build ID: `8dff134c977ac4292c24183a6bd4404bdfcb4727`

The same executable runs both explicit atomic/FileLogIo and witness/WitnessLogIo arms with identical native diagnostics. Each TCP/TLS and QUIC case has one group/three voters/8B Counter/64 warmups/512 offers/4096 window/128 offers per second/ready16/maintenance2s.

| Transport | Publication | Applied originals | Recovered incl. warmup | Observed max inflight | Checkpoints/reclaims |
| --- | --- | ---: | ---: | ---: | --- |
| tcp | atomic | 512 | 576 | 144 | 1/3 |
| tcp | witness | 512 | 576 | 116 | 1/3 |
| quic | atomic | 512 | 576 | 91 | 1/3 |
| quic | witness | 512 | 576 | 95 | 1/3 |

All2048 offered originals apply; all refusal classes, unknown and pending are zero. Every case verifies retries and joins workers. All four native/storage and four publication timing validators pass. Each case records six actual capacity and six publication readbacks across three Create and three Recover assemblies. Twelve post-join physical records check selected generation, selector/metadata CRC and exact WAL boundary; witness arms check both fixed128B records and their matching metadata. These are functional checks, not proof of4096-wide batching or a capacity/gain comparison.

The initial restricted-sandbox control failed before network setup with `no local TCP/UDP endpoint` (exit1,0 cases completed). Its actual run.txt/offers.csv, stdout/stderr/command/exit, before/after inventories, terminal and original setup failure are retained under controls-sandbox-denied and case-results; no nonexistent summary or successful outcome is invented. Only functional controls were retried with authorized localhost networking, using the same source/artifact. No source export or build repeated. The earlier unpublished-sentinel refusal also remains raw.

All original setup/guard/build commands, Cargo JSON/vv compiler diagnostics, symbol/build-ID records, source inventories, publication/qualification receipts, both successful and failed control outputs and validators, maintenance/bases/storage/publication/offer tables and source modes are retained. Large originals use lossless gzip with original/container hashes and lengths in PACKING; SELECTION records original paths/bytes/hashes/modes. SHA256SUMS seals the exact recursive payload set. Generated WAL states, executables/libraries, source archive/full source and build trees remain local under target and are excluded.

No timed performance campaign has started. This does not establish a throughput gain, sustained20M framework floors, Btrfs/APFS/pending real ext4/XFS acceptance, independent physical placement, fair/skewed load, bounded long-run memory/debt or P8 composition performance. FileLogIo remains the default; witness publication stays explicitly selected. Physical file inspection and finite crash controls do not establish real device power-loss behavior.
