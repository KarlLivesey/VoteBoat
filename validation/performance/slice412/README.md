# Opaque owned log validation

`log::OwnedLogState` lets host stores reuse validated delta transitions without
copying unchanged history. Its fixed limits, canonical states and versioned
summaries stay private. Whole-batch planning and correlation precede installation;
invalid later units leave every original group intact. Configuration, snapshot
and replacement keep the original full-validator fallback.

The framework's modeled volatile store uses this helper. Original exact tickets,
pending visibility and barriers remain; bounded range fetch now borrows completed
state instead of cloning the full prefix. This is not a native durability gain.
No provider, I/O runtime, durable token or disk migration is introduced.

[Schema](SCHEMA.md) and retained [checks](checks/) describe the source and scope.
Public5/internal2/framework9 controls pass with all features; public5/internal2
pass without defaults; both targeted strict configurations are zero. Public
oracle coverage includes192 finite rows, late invalid cross-group atomicity,
ballots/configuration/snapshot/replacement, limits and prior payload allocations.
Internal controls cover private stale-cache and revision-exhaustion atomicity.

Final shared source additionally passes the [411 affected broad qualification](../slice411/README.md):
all716/core547 and formatting/four strict profiles0. Performance measurement must
use the same frozen driver/workload and independently compiled reference/candidate
artifacts on Linux and M2 Max. No OpenRaft floor is claimed from these checks.
