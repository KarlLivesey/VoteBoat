# Fresh Linux428 framework artifacts

Publisheda46da2b0a4a13166974601ef0752341aecf9c1c6 (production736eb9a) is exported
as1,424 exact committed inputs with Git modes preserved. The426 export selected
1,418 inputs; the scope is identical and428 adds exactly six files: root build.rs,
log/comparison.rs, its tests, owned/checkpoint tests, Native accepted/checkpoint
tests and Raft checkpoint tests. Thirty paths change. Both root/matched locks,
Cargo profiles, Clippy configuration and compiler remain identical within host.
No dependency update or native-profile substitution occurred.

[PROOF.json](PROOF.json), SOURCE.json/source.sha256 and ARTIFACTS.json retain the
actual source, root library/executable and pinned OpenRaft/bench_minimal compiler
records. All four plus the root build script compiled fresh:false in a genuinely
new target, opt3/debug2/debug_assertions=true/overflow_checks=true/thin-LTO/
codegen-units1/split-debuginfo=packed; flags/wrappers were unset. Compiler/Cargo/
protoc metadata matches426 byte-for-byte: Rust1.98.1/LLVM22.1.8, Linux protoc3.9.0.
Original/copy artifact hashes remain equal. No full test suite was repeated.

[RELEASE_ORACLE.json](RELEASE_ORACLE.json) proves actual Cargo PROFILE=release,
the fresh root build-script compilation, absence of the redundant reference cfg
from both its emitted cfgs and actual root rustc arguments, and presence of the
check-cfg declaration. Ordinary assertions and overflow remain enabled. Checked
nonrelease/test full-Eq oracle proof is separate in slice428/isolated. This
profile gate changes only redundant reference validation; exact release
comparison rests on sealed core/provider ownership and mutation provenance.
Raw matches_state remains full Eq in every profile.

Original build93265 compiled successfully in1m14s, then terminated1 because the
guard searched for a build-script check-cfg string in stderr rather than its
actual root rustc invocation. attempt1 preserves the failure, initial guard and
four artifact records. A focused guard correction validates the exact actual
root --check-cfg argument and existing original/copy hashes without rebuilding;
RESUME_STATUS=0/STAGE=DONE. STATUS=1 remains the original result, not a final
aggregate status. All source hashes pass after building and CLI controls.

The copied executable SHA256 is
85f997d4a969f762c636933dea62aa56fe1864c330c6d21562bfd575dbc9b730.
SYMBOLS.json guards its adjacent packed DWP (34,870,776 bytes,
SHA5a2c79a9bba8b9ce27e4a3da9a4de79c377ff153d567875e6c9c910a64fcf3fd).
ELF notes/sections and symbol-table output preserve profiling provenance.
Binaries, DWP contents, source/build trees and generated states are excluded
from this regular-file evidence archive.

Three small off/on/OpenRaft CLI controls pass16,384 originals/4,096 requests,
frontiers[17409;3]. ON verifies29 phases/four scopes/15 methods and three complete
gap observations per replica, distinct raw-Eq/clone/protected phases, logical
retained-entry units and protected-verification gap boundary. Small control
rates are1,384,800/1,416,816/1,790,503 completed PUTS/s and p998.968/7.826/4.660ms;
these are functional controls, not paired capacity or sustained acceptance.
CLI.json/raw outputs preserve every outcome and actual completion arithmetic.

paired-preparation contains a guarded24-case426/428 singleton/bulk runner and
CONFIG_PAIR.linux.json, prepared without timing. Actual428 outputs plus ten
observer/workload negative controls and six artifact configuration negative
controls pass. Its initial string-adaptation SyntaxError and focused repair are
retained. Baseline OFF supplies no observer; its historical28-phase shape is
not relabeled as candidate29. Candidate ON logical item counts cannot measure
actual compared/skipped work. No campaign has run. Mac qualification, paired
measurements and every full P9 sustained/durable acceptance family remain open.
