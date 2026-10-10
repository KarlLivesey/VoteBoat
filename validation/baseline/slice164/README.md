# Slice164 — independent Counter history checking

Base `6019b83089b505b7b19037177af8d6e1ada159ed` plus this commit. No production
Rust, dependency, wire or persistent format changes.

## Commands

```sh
cargo +stable test --locked --offline --no-default-features --test history
cargo +stable test --locked --offline --all-features --test counter_service history:: -- --nocapture
cargo +stable test --locked --offline --all-features --test counter_service
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
cargo +stable fmt --all -- --check
node validation/check-inventory.mjs
```

Five checker tests pass. Two service tests execute four histories, TCP/TLS or
QUIC replication × WAL or checkpoint reopen, and pass in3.19s. The existing
command client interface is TCP. Complete service-suite output is retained in
service-all.log. Both strict Clippy configurations and formatting pass; inventory
metadata/path validation passes89 records. No protocol proof is inferred from
those metadata checks.

## Independent model and limits

The checker uses no Raft internals or production Counter implementation. It
models signed checked addition, idempotent operation IDs, original results,
conflicting payload rejection and cached overflow. Each call has a unique
invocation/completion ordinal, action and observed outcome. Successful operations
must fit within their recorded intervals. Proven non-admission is a no-op.
An unknown response is not a rejection or the latest possible effect point:
its write may be omitted or placed after uncertainty is reported, up to the
history cut. Other known responses retain their real-time constraints.

Search returns an explicit sequence of original call indices, NotLinearizable,
InvalidInput or SearchExhausted. Exhaustion never counts as success or as proof
of an invalid history. Default bounds are32 calls/100,000 states; service traces
allow48 calls. This is a bounded counter specification, not a universal history
solver or a proof of Raft. Negative fixtures cover impossible reads, real-time
inversion, double application, conflicting retry results, unknown omission/late
effects, overflow, failed admission, invalid intervals and exhausted search.

## Actual service histories

Each history starts three native-file processes, records overlapping write
invocations and their responses, retries original IDs, and obtains quorum reads.
A separate command's reply is intentionally unread; a recorded quorum read
observes its new value before the leader is killed. The replacement leader
returns the original result on retry. Next, both followers are killed, another
write is sent without reading a reply, and the remaining process is killed.
All original stores reopen; that unknown write may be absent or present, but
an exact retry must result in one addition. Final restarts preserve earlier
operation results and the final read. Checkpoint variants inspect a nonzero
native durable base after drain, before reopening.

service-histories.log retains exact raw replies, call ordinals, operation IDs,
fault order and a witness for each history. Unread/disconnected commands are
labeled as observations, not fabricated server replies. The final read of each
trace is also changed to an impossible negative value; the checker must reject
that mutated trace. Failed tests preserve their history.txt and native logs in
the printed temporary directory; successful traces are captured in the log.

## Observed correction and scope

The first rerun and the first complete suite found a recorder omission: the
service can return ERR NotRead(ReadNotReady) after election but before read
readiness. The parser initially rejected it. The exact response is now recorded
as a failed read and retried explicitly. The failed logs and incomplete recorded
histories are retained; no production behavior or arbitrary-error retry changed.

These are selected Linux process-crash schedules, not physical power loss,
arbitrary generated schedules, minimization, cross-group ordering, membership
model checking or proof for other application semantics. Duplicate marker text
is retained, but correctness is checked through original operation results and
counter state, not that marker alone. The previous commit's Linux/macOS CI was
still active when inspected; previous-ci.json is an observation, not a passed
platform result. Full P0–P7 remains active.

Final corrected complete service target:39 passed,0 failed in41.36s. The
five independent checker tests also pass. The earlier full run was38 passed,
1 failed on the known ReadNotReady parser omission; it is retained separately.
