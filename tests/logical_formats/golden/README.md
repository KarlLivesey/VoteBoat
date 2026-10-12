# Existing format compatibility fixtures

Captured from the untouched native codec implementations at baseline `a68e699`
before their core-module relocation. The public records are constructed in
`../fixture.rs`. The capture completed through the original codec APIs; final
tests never regenerate or write these files.

The log batch is original sequence2 after a separate bootstrap creation. Its
checkpoint includes the resulting canonical GroupLog (term/vote, committed
command prefix, revision and generation). Snapshot1 and snapshot2 retain exact
static and membership metadata/application bytes. Wire1 through wire8 encode
the same original scoped Append with its command bytes through each explicit
existing format version. No fixture byte or local encoding proves durability.

Relocation also preserves the full historical-ballot checkpoint implementation
and native storage tests; these fixtures specifically cover VBLCPT01, VBSNAP01,
VBSNAP02 and VBWIRE01 versions1–8, not every possible record workload.
