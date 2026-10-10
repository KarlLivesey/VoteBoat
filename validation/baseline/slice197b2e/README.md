# Slice197b2e — explicit final retirement after source drain

Base revision: `acc005c`. Linux local process execution. The full P0–P7 goal
remains active.

The existing authenticated membership executor already supports final removal
of the drained learner. This slice documents that composed operator workflow
and verifies it with actual processes; it introduces no production protocol,
public API, storage format or background worker.

The prepared administrative operation19780 expects evacuation configuration3,
preserves its voters/policy and removes the source learner in configuration4.
Retirement is explicit and separate from the bounded drain runner, which still
reports stop acceptance with a retained learner. No source files are deleted.

## Executed histories

The two new process tests use TCP/TLS and QUIC peers; all command connections
use authenticated TCP/TLS. Both:

1. Reject premature retirement and prove local absence; then complete the
   source drain and wait for actual worker joining.
2. Reject a reader's retirement request; kill one surviving voter; persist the
   removal on its leader without a quorum. The accepted configuration is4
   while the committed configuration remains3.
3. Lose the command observation and kill that leader. Reopen both surviving
   stores; recover and complete the original retirement operation.
4. Commit a fresh data write and preserve the pre-drain retry; reopen from WAL
   (TCP) or a pinned final-membership checkpoint (QUIC). Verify no learners, two
   remaining voters, no departed voter and both configuration operation IDs.
5. Retry the completed retirement and data operations, write again, then reopen
   the source's stale image. Its original durable gate remains active and
   rejects a new write. Surviving quorum reads remain correct.

`focused.log` records both selected histories (9.03 seconds). `regression.log`
records all 96 service tests and 8 command unit tests passing (service tests:
50.53 seconds). Formatting and all three strict Clippy profiles pass with zero
diagnostics; 103 contract metadata/path checks pass. These checks are recorded with commands and source
hashes. These finite histories do not establish arbitrary power-loss recovery,
multi-group orchestration, untrusted state transfer, automatic data deletion,
separate-machine execution or macOS validation.
