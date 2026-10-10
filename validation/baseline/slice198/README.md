# Slice198 — authenticated local assignment pages

Base revision: `6b2bf1c`. Local Linux execution; full P0–P7 remains active.

`list-assigned-groups CURSOR LIMIT` reports the fixed executable's actual local
assignments using the existing owner iterator. Each row gives exact group and
incarnation, latest locally accepted configuration, stable configuration and
optional joint target. The output is local accepted state, not committed
membership, global placement, owner activation or read authority.

The command snapshots at most256 rows, emits at most eight per page, and binds
cursors to the complete configuration view and exact node/store/incarnation/
session. It retains no cursor resources. Every local group requires Inspect
before any page is emitted, including an empty page. Group-prefixed inventory
commands are rejected; single-group trusted loopback operation remains available.
No new provider, consensus/storage format or dependency is introduced.

- `focused.log`: all three process tests pass (7.72s): TCP/QUIC paging, bounded
  and malformed inputs, group1-only refusal, all-group reader permission,
  data-write cursor stability, configuration/reopen invalidation and preserved
  original duplicate write receipts. Single-group inspection works without
  an elected leader or remote quorum.
- `unit-focused.log`: both unit tests pass, checking identity/session/inventory/
  configuration changes, malformed and missing-group cursors, empty end pages,
  maximum IDs,256-row/eight-row bounds and the4096-byte client reply ceiling.
- `service.log`: final counter regression passes122 service tests (56.28s) and
  16 command tests, with no failures or ignored tests. It includes existing
  drain, leadership, membership and credential authorization behavior.
- `service-before.log`: the earlier regression passed121 and failed one QUIC
  assignment test on an explicit UNKNOWN after source restart.
  `interrupted-write-source.log` records the exact operation42 wait being
  cancelled at its deadline. The fixture now retries that original acknowledged
  operation/payload for the observed transport/leadership uncertainty, then
  requires duplicate=true. No client retry semantics or deadlines were changed.
- Final formatting, all four strict Clippy profiles, warnings-denied API docs
  and the105-contract inventory check pass. Inventory metadata checking does
  not establish behavioral conformance by itself.

Commands and source hashes are retained separately. These selected Linux
histories do not establish current macOS, separate-machine or full-roadmap
acceptance. Platform validation, broader lifecycle/fault and provider evidence,
and the original unmet performance gate remain open.
