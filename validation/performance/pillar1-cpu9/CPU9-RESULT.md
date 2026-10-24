# CPU9: rejected at the Apple work-removal gate

24 October 2026. [PR32](https://github.com/KarlLivesey/VoteBoat/pull/32) is rejected
without merging candidate source. Exact head `52f4571e18f0ec5407b8dd4942c5a26d93f0528a`,
tree `542426e051eaeaae1b34c26521aded21e06a671c`; original production baseline
`0bf9271ecb5a34c4089c6bb10381303232624170`. Production remains unchanged.

The required Apple mechanism gate failed on the normal candidate binary. The
ordinary volatile branch still stages an Effect at `sp+0x140`, joins the durable
arm and transfers272 bytes into the actual ring slot. At0x100091290 the source
is `sp+0x140`, at0x100091294 the length is272, and0x100091298 calls0x100211084,
identified as `_memcpy` by the Mach-O indirect-symbol table. The full branch,
Pending installation, growth path, destination and imports were inspected;
this is not an isolated-copy grep or a size-only inference. Baseline retains the
same kind of272-byte staging transfer. Branch-local source construction did not
remove the required common machine work.

The mandatory gate required removal of both staging and extraction movement.
The staging counterexample is sufficient to fail it; complete downstream
extraction/removal remains unqualified. No execution frequency, CPU fraction,
regression cause, rate, p99 or gain is inferred. **Primary0/9 pairs; canonical0/64
children; CPU gain UNKNOWN.** Full5 and integration were not released.

Both final independent Sol source reviews approved this head after the snapshot
input-ring lifetime P2 was fixed. Linux focused tests, valid old-work and semantic
controls, strict7 and the actual enabled push hook were recorded separately.
They do not substitute for the compiled-work gate. On Apple, exact2607-input
and mode/stream/provenance checks and normal build passed; four queue tests
passed. Other Apple focused tests, VALID/semantic controls, diagnostic/identity/
Rust-packet controls and all timing were not launched after the counterexample.

The normal Apple binary is4112000 bytes, SHA256
`9ef043d7ff0daf49265f15ca2b0928d4885995bbf89bc1c07174b1c7eeb0f91d`.
Frozen input manifest562841 bytes/SHA256
`98adcc1f6a03ca550889847ea30380ad928b541671747ad4a30abc69bafe3456`.
Full source patch234195 bytes/SHA256
`9b66dcb48c836ab783a2baddef61ca12b771114a952e0fa12d01d89751988c24`.
The complete source/controls/binaries/raw evidence remain in ignored
`target/claude/pillar1-20261022/cpu9-{builder-r4,snapshot-p2-fix,perf}/`.
[Compact disposition and hashes](evidence/parent-disposition.json),
[actual machine gate](evidence/WORK-GATE.json),
[runner report and limits](evidence/runner-report.md) preserve the audit trail.

Both owned Mac roots and their sole target were removed after verified evidence
collection. Known children joined, exact recorded identities were checked, and
the final unchanged stock guard reached the actual Mac and passed. Initial,
sampling, observer/receipt and historical HOLD limitations remain disclosed;
later clean snapshots do not erase them. No production fix, alternative timing,
profile, full suite, detached job or successor was launched by the runner.

P1 remains active. The next design must use the retained machine transfer as
evidence and identify a representation or ownership change that can remove real
work while preserving every implemented VoteBoat feature. No accepted CPU gain
or complete optimisation is claimed.
