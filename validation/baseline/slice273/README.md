# Slice 273: exact application recovery expectation

The imported-partial Mac269 checkpoint comparison reported 7400-byte images
that differ only at the applied index:46 versus44. The log captured under
`observed/` was emitted by a still-live older-source run; it is not a terminal
suite result. Its662-input source manifest/guard identifies178844a. Two binary
images reconstructed from those exact assertion arrays and their byte-level
comparison are retained separately.

The fixture previously compared pre-abort checkpoints positionally with reopened
nodes. Its ready-leader selection can reorder nodes, and accepted native worker
updates can finish while abort returns stores. Restart now matches cloned
applications and returned logs by NodeId, applies only each actual contiguous
committed tail after the captured application boundary and requires exact
reopened log/checkpoint equality. No checkpoint field is ignored. The original
unread-result cut, operation IDs, lifecycle phases and cleanup remain.

Two actual-core/native-WAL regressions preserve a campaign no-op and a later
Counter command after capture, then recover the exact application and original
retry. These are one-voter model-journal cases; the original four three-voter
TCP/QUIC WAL/checkpoint histories separately compose real stores/workers/owners.
No production, public provider, wire format or persistence format changes.

Linux and Mac each finish the six-case family with zero failures, plus two
all-feature/two default-feature regression checks:10 executions per host.
Formatting/four strict profiles are zero and667 inputs verify before regression,
after regression and after the family. Native/core profiles do not compile
this TLS-only private fixture and have strict compile/lint checks.

`published.sha256` hashes normalized artifact bytes; `raw.sha256` records
original bytes. Frozen269 and older262/251 Mac jobs remain separate obligations.
The endpoint duplicate assertion and imported-retirement timeout are separate
next deliverables. Full P0–P7 and broader platform/fault/provider gates stay open.
