# Host-specific input schema

CONFIG.json has mode asserted_release (actual diagnostic) or
functional_debug_smoke (only16384 originals/one trial/no process timing).
source_commit is exact published41ee217 SHA; source_root and source_sha256 map
freeze the full relevant source/manifests/locks. executable is the single shared
binary used by all three variants. artifacts is a list of target/path/sha256;
asserted mode additionally requires original and actual Cargo compiler_record
for voteboat and voteboat-matched-framework. Actual records must be fresh:false,
opt3/debug2/assertions/overflow true and source-root correlated.

compiler_evidence maps rustc, rustc_version and invocations to path/sha256. The
last file is captured actual verbose Cargo rustc lines, including bin thinLTO/
codegen1 flags, not a hand-written intended profile. All originals/copies and
compiler evidence are hashed again. driver_sha256 binds absolute paths for this
three-script driver/checker/guard set. No compiler is invoked by the timed driver.

Smoke identity instead discloses qualification exactly as "debug functional smoke
only; no release/timing claim" and uses the already built debug41 executable.
Release checks cannot be skipped by using smoke: mode also fixes operations/trials
and report scope. Each outputdirectory owns a copied CONFIG, commands/raw exits,
before/afterguard logs and every stdout/stderr. Failed cases remain invalid rather
than becoming throughput observations. No new dependency, installer or package.
