# Slice194b — authenticated native split commands

Base revision: b0c2e2e2377569ee2e07b8091c4fadf8379cb689. Source hashes identify
the changed production and test files. RPL-1.5; no dependency, consensus protocol
or persistent format change. The bounded observation wire format is new.
See [usage and limits](../../../docs/TRANSFER_SERVICE.md).

`voteboat-transfer` composes native metadata/source/target replicas and drives the
public TransferOperation through authenticated completed reads and ordinary
authorized Node admission. It retains original intent and operation identities;
there is no separately persisted client phase counter.

## Evidence

- `service.log`: all3 tests pass in20.34s. Two histories run12 real role processes
  each over TCP/TLS or QUIC peers. The TCP client starts and completes a split.
  QUIC kills all role processes after the first import, reopens original stores
  and resumes. Both disconnect a client after a read is accepted without quorum,
  check pending reads return to zero, and reject a reader mutation/wrong-group
  principal. Both verify source fencing and original data/retry identity on
  independent children after metadata/source shutdown, including read-back of
  newly written values. Normal shutdown checks
  worker joins. Six malformed-profile cases fail before replica files open.
- `operator-wire.log`: all8 core-only operator tests pass. New tests roundtrip
  observations, reject every truncated prefix plus trailing/magic/budget errors,
  and preserve next-action decisions. The existing activated-history test also
  roundtrips its observations. Synthetic application values remain distinguished
  from native read-completion evidence in `service.log`.
- `core.log`: all31 core transfer/operator/publication/source/target tests pass.
- `counter-auth.log`: both existing TCP/QUIC authenticated principal/recovery
  histories pass after the shared authorization/client helper change.
- `directory-service.log`: all13 shared-client directory regressions pass in20.84s
  after the fixture port correction.
- `fmt.log`, `clippy-{all,default,minimal}.log`: final format and strict Clippy
  pass with zero diagnostics for all targets and three configurations.
- `rustdoc.log`: warning-denied all-feature documentation passes.
- `inventory.log`:98 records pass shape/path checks only, not conformance.

## Corrections retained

`service-initial.log` records test-fixture parent directories missing before
native startup; the fixture now creates those parents. `service-first-pass.log`
is the earlier passing run. `service-refusal-expectation.log` records two test
failures after changing refused data reads to nonzero stderr errors; assertions
were corrected to match the intended interface. The final `service.log` includes
the real profile generator, readable data results and corrected refusals.

`directory-service-initial.log` records12 passing histories and one startup
failure with AddressAlreadyInUse. Its fixture range began at32000 and extended
into this Linux host's32768–60999 ephemeral range. The test allocator now begins
at20000 to avoid client-port overlap during multi-replica startup. No production
network behavior or assertion was weakened; the log proves the bind failure,
while ephemeral reuse is the identified likely collision mechanism.

## Limits

Only explicit whole-responsibility native counter profiles with one source and
2–16 targets are exposed by this executable. The administrator remains a trusted
publisher of lifecycle evidence; the wire decoder does not authenticate or prove
freshness by itself. Tests kill processes after committed phase boundaries,
not at arbitrary writes/fsyncs. Recursive/retained/merge executable profiles,
broader combined membership/fault schedules, retirement, macOS execution,
separate-host deployment and the original P7 performance gate remain open.
The full P0–P7 goal remains active. No whole-repository test pass or macOS result
is inferred from these focused checks.
