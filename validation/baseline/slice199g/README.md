# Slice199g — bounded automatic read failover

Starting revision42ded2d. A real CLI regression demonstrates the original gap:
a silent first replica consumes the whole ten-second deadline, and the client
never tries the available next replica. Automatic reads now have two-second
attempts within the same overall deadline. Every attempt sends the original
scope and asks its server for a fresh quorum read. Explicit-node calls and
uncertain writes are not given automatic replay.

The existing transient-observation policy is shared between client routing and
the drain runner. Authentication/request/reply deadlines and empty disconnects
allow another read; malformed/partial replies, authentication failures and
other outcomes remain terminal. Partial reply bytes now have a distinct error
from a connection closed with no reply bytes. This changes neither consensus
nor persistence.

## Evidence

- `read-failover-before.log`: original source fails the new silent-replica test
  with `ERR reply deadline expired`.
- `read-failover-initial.log`: first revised run passes three checks and exposes
  two fixture reservation errors. The helper's placement also produces two
  unrelated-binary unused warnings. Both are corrected in final source.
- `read-failover.log`: all five new checks pass, covering plain/grouped commands,
  empty/partial/invalid replies, total deadline exhaustion and a stalled first
  handshake followed by real authenticated TCP/QUIC service reads.
- `counter-unit.log`: all27 command/drain runner tests pass, including partial-reply
  refusal.
- `operators-port-pool-failure.log`: the first full run passes121 counter tests
  and fails eight when the old157-block fixture pool is exhausted, before those
  fixtures can start services. The pool now has305 candidates, still checks all
  endpoints by binding and never recycles an issued block within a test process.
- `operators.log`: all129 counter,13 directory and14 transfer service tests pass.
- `read-failover-default.log`: all4 default-feature checks pass, executed after
  all-feature processes have finished.
- `clippy-all.log` and `pre-push.log`: strict all-target lint and the complete
  formatting/four-profile hook pass with zero diagnostics on final code.
- `source.sha256` / `source-check.log`: relevant final source hashes.

Commands:

```sh
cargo +stable test --locked --offline --all-features --test counter_service read_failover::
cargo +stable test --locked --offline --all-features --bin voteboat-counter
cargo +stable test --locked --offline --all-features --test counter_service --test directory_service --test transfer_service
cargo +stable test --locked --offline --test counter_service read_failover::
sh .githooks/pre-push
```

## Platform boundary

`prior-54a73ad-macos.log` belongs to completed operator run38035784987, job
114165907119. It reports117 passing and five failing counter histories: assignment
retry, a final group read, grouped drain write, historical leadership/current
leader mismatch, and an interrupted configuration exchange. That run's Ubuntu
operator job114165906949 passed. It is older-source platform evidence, not a
current macOS result. The failed download of job114165760812 is explicitly
recorded as unavailable in `prior-ccc5bb7-macos.log`; that file is not an execution
log. Remaining platform issues, combined lifecycle/revocation and P7 are open.
