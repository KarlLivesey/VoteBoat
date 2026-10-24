# CPU10: stopped at the first public byte-budget gate

No production change was published or merged. Parent input is
`ff21f12ad293fd5e5cefb7b203e493531fa5e2b0`; production remains
`0bf9271ecb5a34c4089c6bb10381303232624170`. There was no CPU10 PR.

The design separated small volatile tokens/markers from genuine large payloads
in two retained rings behind one optional header Box. The bounded release
required compiled layout/capacity and independent per-domain byte feasibility
before migrating production or consumers. It did not authorize spending
native-only reservation credit in a different embedding.

The Linux test-only prototype reported Effect272, PayloadEffect272,
OrderItem24, old header32, boxed pair of headers64 and pointer8 bytes.
Twenty-four finite capacity/correspondence histories passed. The independent
public boundary assertion failed (Cargo101):

| Actual retained owner | Old queue | Split queue |
| --- | ---: | ---: |
| Public input envelope | 256 | 256 |
| Queue header/pointer | 32 | 8 |
| Boxed pair of collection headers | 0 | 64 |
| Order backing at capacity38,994 | 0 | 935,856 |
| Large effect/payload backing at capacity38,994 | 10,606,368 | 10,606,368 |
| Total | 10,606,656 | 11,542,552 |

The actual public `effect_reservation(256)` is10,606,720 bytes. The old
retained queue fits with64 bytes left; the split storage exceeds it by935,832.
Both queues are empty while retaining their actual allocations. The original
public SetCampaigning call succeeds without output, respecting the documented
drained-output restriction. No payload backing objects, conversion source,
growth buffers or external leases are live in this witness. Spare retained
capacity is real ownership. Parent independently read the complete prototype
and original failing log and checked the arithmetic.

This is a legal explicitly reserved public reusable capacity, not evidence that
the ordinary matched Actor organically reaches it. Both totals fit its declared
4GiB ceiling; a complete matched bound remains unqualified. Native EffectOwner
stays on legacy owned collections, with its separate4x slot accounting and
existing source/continuation/lease charges. That credit cannot erase this
public counterexample. General growth/conversion/lease feasibility was not
qualified after the stable public case failed.

No production migration, Counter/Actor history, semantic feature matrix,
VALID restoration or semantic mutants, seven-check publication/hook, review,
Apple codegen, CPU pair, canonical child, full suite or integration ran.
CPU gain and Apple work removal are UNKNOWN. The first prototype yielded one
pass and one failed assertion. An unrelated fixture maintainability lint
(107-line function) was corrected by moving its exhaustive helper to module
scope; the boundary function hash remained identical. Final formatting and
strict all-target/all-feature Clippy passed. The final helper-moved fixture was
compiled/linted, not functionally rerun; no strict7 or functional-green claim.

Both tested and final complete patches and2,606-row build-input manifests are
preserved. The tested manifest was captured after execution from unchanged
formatted source, not before-build frozen qualification. The final patch
contains only docs and a test-only prototype; it is evidence, not applied code.
Compact [original evidence and hashes](evidence/SHA256SUMS) are retained here;
full raw records remain under
`target/claude/pillar1-20261022/cpu10-builder/`.

Own target, checkout, registration and unpublished seed were removed. Final
unchanged stock guard passed with actual Mac observation;495 recorded PID/start
identities had no remaining live owned process. Parent read cleanup and terminal
closure receipts. Finite sampling does not clear unobserved infrastructure or
historical HOLDs. Initial/read/setup and other visibility limitations remain in
the original report. No limits, admission, lints or safety rules were weakened.
