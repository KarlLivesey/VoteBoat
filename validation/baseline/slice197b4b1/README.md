# Slice197b4b1 — public native multi-group startup

Base revision: `e66d4ff`. Local Linux execution; full P0–P7 goal remains active.

`NativeMultiStartup` opens an explicit complete group set with one authoritative
native WAL, peer endpoint, scheduler and snapshot worker. Original bootstraps,
fresh applications and provisioned identities are checked before resource
creation. The existing WAL barrier authorizes bootstrap durability; existing
snapshot/log verification authorizes application recovery. No consensus or
ownership protocol changes are included.

`focused.log` records all five new downstream tests passing. `regression.log`
records the final source passing all25 startup and32 native benchmark tests.
The TCP/QUIC histories each use three groups and three nodes. They checkpoint,
close/join, reject omitted and changed original group declarations, recover all
groups and preserve original operation receipts while accepting fresh writes.
Changed-bootstrap recovery returns a map containing an already-restored first
application. Separate invalid-input and late-construction tests check absent
preflight side effects, full application return, joined workers, released sockets
and recoverability of already-created files.

`service.log` records all96 existing service tests and nine command unit tests
passing against the refactored common native assembly. `fmt.log`, the three
`clippy-*.log` files and `doc.log` record successful formatting, strict all-target
Clippy for all/default/no-default features and warnings-denied API docs.
`inventory.log` validates105 contract metadata records and referenced paths;
this metadata check is not behavioral conformance.

The API retains single-group snapshot paths; additional paths explicitly bind
group ID and incarnation. It offers no automatic migration, partial store
activation, group creation in an existing store, executable multi-group command
surface or new performance claim. These are finite local histories, not arbitrary
power-loss proof, macOS execution or separate-host deployment evidence. The next
planned slice connects group-addressed authenticated commands to this production
startup and the existing multi-group drain coordinator.
