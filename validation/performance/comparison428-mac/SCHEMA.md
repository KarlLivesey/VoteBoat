# Mac428 paired comparison schema

This separate driver copies the frozen Linux428 workload/checker contract without
changing those files. Compare immutable Mac426 (799f2cf) against published Mac428
(a46da2b), three rotating singleton1/batch1 and bulk4096/batch4 trials each,1048576
measured originals plus1024 warmups: baseline off/candidate off/candidate on/upstream.
Same Mac compiler1.98.0, opt3/debug2/assertions/overflow/thinLTO/codegen1/packed
profile; source/build.rs, locks/config/compiler and four actual fresh:false library
and executable records are guarded. Actual Mac packed output is a Cargo-recorded
dSYM bundle, not Linux's DWP file. Guard every original/copied dSYM file, complete
bundle inventories and executable correlation. Baseline's existing original bundle
remains read-only; candidate qualification has an actual matching copied bundle.
Normalize /tmp and /private/tmp paths using canonical filesystem identity.

Candidate PROFILErelease proof is derived from qualified raw Cargo JSON and actual
verbose compiler invocations; build-script compilation fresh:false, cfg emission
empty and ordinary assertions/overflow still enabled. Debug/test full Eq remains
separate. Runtime observes29 phases/four scopes/15 methods only on candidate ON;
logical equal-entry units are never physical traversal counts. Other completion,
frontier/capacity/timer/maintenance and all original outcome checks remain frozen.

Prepare and guard outside timing; no builds/checks/copies/archive work on the same
host during timing. No timing until root coordination. These24 short diagnostic
cases do not qualify any sustained20M minimum, real durability or P8 acceptance.
