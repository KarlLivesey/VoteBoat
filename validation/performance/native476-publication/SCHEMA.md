#476: two-sync native publication with fixed independent witnesses

Before production edits.475's complete54-case/host sweeps show backlog capacity
helps batching but does not remove publication waiting. At512 offered/s and4096
window Linux TCP/QUIC medians200.436/205.054 completed originals/s have p99
18.965/20.052s; Mac364.009/363.740 have p99 6.499/6.535s. Mean leader publication
is27.835/27.916ms Linux and16.351/16.227ms Mac. These are nested elapsed scopes,
not CPU. A portable reduction in publication work advances P9.0/P9.1/P9.3.
No filesystem tuning, new timeout, outcome removal or default promotion.

## Public seam and ownership

Add JournalIo::publish_barrier(manifest), defaulting to the current ordered
sync_log then publish_manifest. NativeLogStore creation/recovery/barrier call
this operation and preserve their existing validation/promotion/error fencing.
A separate public native WitnessLogIo implements the same JournalIo interface;
FileLogIo remains the default with unchanged durability/format/primitive order.
Transparent generic wrappers must forward the combined operation. A legacy
primitive observer can remain deliberately bound to FileLogIo's default path;
it must not accidentally wrap WitnessLogIo and add a third sync.

The provider exclusively owns one WAL, one independent acknowledgement file,
selected physical generation, lock and bounded witness metadata. No new worker,
shared singleton, log-store stacking or second application WAL. Accepted work,
LogTicket/DurableLog, configuration/term/generation/snapshot checks and per-original
completion contracts remain unchanged. Every failure after attempted writes is
uncertain; NativeLogStore fences before any dependent durability escapes.

The normal publication is: complete appends; write witness A in a fixed WAL
header; synchronize WAL/header; write witness B into a precreated independent
file; synchronize B; return success. Only then does NativeLogStore promote and
return original exact tickets. Keep sync_all initially to isolate the publication
change. There is no ordinary create/truncate/rename/directory sync. Two syncs,
not the unsafe single-sync historical-marker proposal; no assumed gain.

## Physical representation

Each witness is128 bytes, fixed little endian: distinct magic8; versionu32@8;
roleu32@12 (A=1/B=2); physicalgenerationu64@16; publicationcounteru64@24;
StoreIdu128@32; incarnationu64@48; sessionu64@56; logicalWALlengthu64@64;
52zero reserved bytes@72..124; CRC32C@124 over0..124. Both witnesses describe the
same publication after success, ignoring role/CRC. Counter starts1 and increments
checked max(valid counters)+1; no wrapping or historical slot fallback.

The physical WAL starts with A; unchanged native codec frames start at128.
Logical read/append/truncate/replacement and max_wal_bytes consistently exclude
this fixed header; physical lengths use checked addition. B's file is exactly128
bytes, never truncated in ordinary publication. Recovery reads are bounded.
Checksum-valid unknown format/role, zero IDs/session/counter, nonzero reserved,
wrong generation/identity or contradictory pair metadata are errors, not invalid
fallback candidates. Invalid CRC permits the other fixed witness to protect the
acknowledged boundary; invalid/missing physical selection never permits fallback.

Use a distinct20-byte CRC-protected CURRENT selector (magic8,generationu64,CRC4).
Generation0 is legal. Keep bounded generation parity files and exclusive LOCK.
Use existing recognizable log.wal/MANIFEST and parity names so the default
provider's create refuses a witness store; the128-byte format also makes legacy NativeLogStore recovery
reject. Witness open requires its distinct selector and rejects legacy
selection; never infer an old format from a damaged witness or selector.

## Recovery and partial progress

Read both fixed witnesses. Both invalid rejects. Equal counters require identical
metadata. With both valid and A ahead, B is the mandatory acknowledged boundary;
A is merely a candidate, because a crash in the first sync can persist A before
all data. B ahead rejects. A's candidate length cannot be less than B and its
session must equal B's or checked B.session+1. Candidate counter gaps are allowed
across repeated interrupted recovery. Same identity/physicalgeneration is mandatory.
With only A valid, A is mandatory; with only B valid, B is mandatory. This never
selects an older historical marker after final-authority damage.

Native recovery still rejects truncation/corruption inside the mandatory prefix,
validates complete frames/transitions/identities, recovers complete valid unacked
suffixes and discards only the incomplete final tail. read_manifest returns the
existing canonical52-byte VBLSTR02 representation. Do not bypass the ordinary
recovery decoder or accept a maximum index as a prefix.

Before publication overwrites a sole surviving witness, restore and synchronize
its counterpart from that mandatory witness. This is cold-path repair; otherwise
a second ordinary crash could tear the sole survivor. Repairs and new publication
follow only after ordinary WAL validation; interruption preserves a mandatory old
or new prefix. Preserve original failures and retry the same operation IDs through
existing recovery rules. Publication counters and sessions are checked separately.

## Initialization, reclamation and compatibility

Only a fresh create handle has an explicit volatile Initializing state that may
report missing manifest/empty logical WAL. Create refuses every old/new/striped
store marker and precreates the fixed regions. Initial compound publication syncs
both witnesses before atomic selector publication and required directory/parent
syncs return. An open never converts missing/partial initialization to an empty
voter. Failures retain their files/lock until owner cleanup; no automatic deletion
of potentially acknowledged data.

Reclamation writes complete checkpoint+A+B into the inactive parity generation,
synchronizes data and acknowledgement plus namespace creation, then atomically
publishes/synchronizes CURRENT before switching handles and deleting old data.
All primitive interruption points recover selected entire old or new pair. Never
choose an older generation because selected files are missing/corrupt. Preserve
codec verification, drained-transition gate, generation/session identities and
old/new cleanup, including parity reuse. Format is provider-specific v1; native
codec version2 and wire bytes remain unchanged.

First slice supports explicitly selected fresh stores and their own reopen,
repair and reclamation only. No implicit FileLogIo conversion or legacy default
change. Existing stores stay on FileLogIo. Backend migration remains explicit
quiesce/export/import/verify/publish with new binding and old completion rejection;
that path is not claimed implemented by this candidate. This scope gives a usable
Rust provider and real candidate service without relabeling unsupported migration.

## Acceptance

Keep the independent finite ordinal model and its negative controls (maximum
witness, historical-marker fallback, sole-survivor overwrite) alongside its actual
commands/exits. Model checks do not establish byte/device correctness. Add byte-
level torn overwrite/sync/error/restart controls, repeated repair crashes, every
acknowledged WAL truncation and CRC+truncation compositions, invalid identity/
role/reserved/session/counter/physicalgeneration, missing selected files/selector,
init/reclaim/parity cuts, unsupported legacy mixing and single-lock lifetime.
Use real NativeLogStore tickets/outcomes/recovery/optional reclaim controls and
public downstream interface injection. Existing unchanged provider scopes are
reused only by exact source hashes; run affected checks and enabled full strict
hook without weakening lint/format settings.

Actual three-voter TCP/TLS and QUIC offered histories must preserve all original
applied/rejected/unknown outcomes, snapshots/maintenance, retry/reopen and worker
joins. A small explicit benchmark selector must record actual provider in Create
and Recover; baseline/candidate must use identical diagnostics/profile/admission/
workload/timer boundaries. Fresh published-source native builds on both hosts
precede paired measurements. Gain must pass Btrfs, APFS and the user's real
ext4/XFS scratch; third environment remains pending. Keep default FileLogIo until
all acceptance is met. Do not turn this provider slice into full P9 completion:
seven framework cells/both hosts/sustained bounded memory/debt, real headroom,
fairness/skew, placement/shared barriers and P8 composition all remain open.
