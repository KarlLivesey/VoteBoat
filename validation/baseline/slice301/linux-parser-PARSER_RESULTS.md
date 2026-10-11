# Slice299 parser campaign — final local evidence

Owned source is frozen for parent integration: tests/parser_campaign.rs and parser_campaign/{generator,routing,snapshot,wal,wire}.rs. No production, Cargo/dependency, shared ledger or design-pack edits. Schema/proposal preserved separately.

Actual all-feature Linux campaign:8 tests passed /0 failed in0.12s. Eight seeds [1,7,19,43,101,257,1021,65537],192 cases/seed/corpus,11 corpora. Totals: {"cases":16896,"accepted":4829,"refused":12067}. Each corpus executes192 unmodified controls plus seven generated classes; accepted mutants are allowed when public shape/limits/roundtrip hold.

| Corpus | Cases | Accepted | Refused |
|---|---:|---:|---:|
| routing-publication | 1536 | 479 | 1057 |
| wal-create-batch | 1536 | 434 | 1102 |
| wal-acknowledged-recovery | 1536 | 312 | 1224 |
| snapshot-published-manifest | 1536 | 403 | 1133 |
| wal-manifest-recovery | 1536 | 312 | 1224 |
| wal-update-batch | 1536 | 431 | 1105 |
| wal-interrupted-suffix | 1536 | 1230 | 306 |
| snapshot-image | 1536 | 343 | 1193 |
| rpc-v1-multiplex | 1536 | 301 | 1235 |
| rpc-v8-multiplex | 1536 | 274 | 1262 |
| rpc-v8-snapshot | 1536 | 310 | 1226 |

Seven inner codec/manifest corpora repair CRC for384 overwrite/length cases each (2,688 attempts): WAL Create/Update, RPC v1/v8 multiplex and v8 snapshot, snapshot image and published manifest. Raw acknowledged WAL/manifest and interrupted suffix recovery do not reseal corrupted acknowledged bytes. The common log structured_overwrites count denotes generated classes, not CRC repair for routing/raw recovery.

Other actual checks: core-only2 tests passed /0 failed (routing1,536 generated cases + independent standard CRC vector); strict all-target/all-feature Clippy final exit0 with zero diagnostics; fmt --all -- --check exit0; git diff --check exit0. Isolated CARGO_TARGET_DIR=target/divide-conformance/parser-build. Final all-feature repeat was justified by expanding the existing WAL Update seed to include its actual suffix/command payload before final qualification; no new corpus afterward.

Failed preflight retained in linux-parser-initial.log:5/7 passed; both snapshot tests failed during fixture creation because the independent SnapshotIo returned Ok(empty) for an absent manifest, correctly interpreted by NativeSnapshotStore as already existing. Focused fixture fix returns NotFound for absence. No production failure observed. One apply_patch context mismatch was corrected after reading the actual formatted lines; it made no source change.

Functional scope: public native batch codec + actual native WAL/manifest reopening and interrupted tails; selected NativeWireCodec1/8 Vote, ReadProbe, Append, ReadAck and Snapshot frames; NativeSnapshotCodec static metadata and actual published two-slot manifest recovery; DirectoryCommand partitioned-manifest/bucket boundary decoding. Accepted output uses public shape/budget/roundtrip assertions; rejected complete CRC damage/truncation yields no partial decoded batch/image. Fresh independent devices per recovery case; exact acknowledged GroupLog retained on accepted raw recovery.

Limits: bounded deterministic seeded mutation, not exhaustive or coverage-guided fuzzing. No custom unsafe allocator instrumentation/RSS claim; input sizes/output budgets/bounded snapshot reads are checked. Not all RPC variants/versions, native reclaimed-log checkpoint formats, membership-bearing snapshot codecs, every routing/lifecycle decoder, real disk publication, arbitrary allocator/device faults or security certification. Parent owns frozen Linux/macOS four-profile qualification and authoritative acceptance ledger.
