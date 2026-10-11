# Documented Rust embedding handoff

Manual Linux check on11 October2026, against the unchanged709 compilation/test
inputs recorded in [305](../slice305/README.md). Root verification with
`sha256sum -c target/slice305/results/source.sha256` succeeds for every input.
This check covers the documented single-voter Rust embedding; it is separate
from networked three-voter acceptance and the still-running Mac302 matrix.

The installed `stable` compiler reports `rustc1.98.1`, matching the repository's
pinned version. Commands use explicit `+stable` because the separately named
`1.98.1` rustup toolchain is not installed in this environment. The first command
without that override fails before execution when rustup cannot write its
restricted temporary directory. A sandboxed create attempt then reaches native
startup but refuses listener access with PermissionDenied. Neither is counted
as a successful example run. The successful sequence uses authorized local
listener access and a fresh directory.

```sh
cargo +stable run --locked --offline --example embedded_counter -- create /tmp/voteboat-feature-handoff-MAP5yq/qualified tests/fixtures/tls 1 7
cargo +stable run --locked --offline --example embedded_counter -- recover /tmp/voteboat-feature-handoff-MAP5yq/qualified tests/fixtures/tls 1 7
cargo +stable run --locked --offline --example embedded_counter -- recover /tmp/voteboat-feature-handoff-MAP5yq/qualified tests/fixtures/tls 2 3
```

All three exit0. Actual reported results:

| Invocation | Applied receipt | Quorum-backed read | Owned cleanup |
| --- | --- | --- | --- |
| Create, operation1 adds7 | Value(7), duplicate=false | linearizable_value=7 | workers_joined=true |
| Recover, retry original operation1 | Value(7), duplicate=true | linearizable_value=7 | workers_joined=true |
| Recover, fresh operation2 adds3 | Value(10), duplicate=false | linearizable_value=10 | workers_joined=true |

The demo uses public fixture credentials, an owned temporary directory and the
existing native startup, application, client/read and shutdown paths. It changes
no production behavior. Successful worker joins permit cleanup of that directory.
This is a manual usability check, not a full-platform, performance or fault-proof
claim. Formatting and strict all-target/all-feature Clippy have zero diagnostics.
The enabled pre-push hook also checks all four feature profiles.

Service instructions now link the actual mixed-host static deployment256 and
Linux/Mac complete counter305 evidence rather than claim these paths have never
run. Separate-host discovery/membership administration and overall Mac matrix
acceptance remain distinct. The README remains concise.
