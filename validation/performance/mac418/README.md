# Frozen418 qualification on M2 Max

Exact published 32217aebe6d425f8658fbe8b0a111bfaf4aaca09 passed the approved scoped Mac qualification on its original runner (SSH 99226, remote PID 64863, exit 0). All 1384 compiled input hashes match before/after, including exact clippy.toml, both pinned locks and 417 standalone attribution code. Later 419 source, ignored packs, checkout binaries and validation history were excluded. [Schema](SCHEMA.md) records scope and ownership.

| Actual scope | All features | No default features |
| --- | ---: | ---: |
| Log/owned/private transition controls | 22 | 22 |
| Native accepted-overlay controls | 11 | Not enabled |
| CoreRaft controls | 110 | 91 |
| LogStore ModelIO/publication/compaction controls | 15 | 0, feature-filtered |
| Raft integrations | 79 | 29 |
| Snapshot image/proof/restore integrations | 25 | 10 |
| Real TCP/TLS and QUIC retries, replacement and native-file recovery | 2 | Not enabled |
| Maintenance/checkpoint/reclaim/restart and invalid schedules | 6 | Not enabled |
| Standalone matched controls, including optional attribution | 11 | Standalone |

Root formatting and all four strict feature profiles, plus standalone formatting/strict all-target checks, passed with zero diagnostics. Four functional CLI controls each validate 16384 original outcomes/4096 requests, all three frontiers 17409, and explicit primary 4096/4096/1024 or common64 envelopes. Attribution is disabled in these primary CLI controls. These are correctness results; no timed benchmark or full unrelated Mac-suite claim is made.

Distinct fresh release targets compile both root libraries and executables with actual fresh:false compiler records. Matched uses opt3/assertions/overflow/thin-LTO/codegen1; native explicitly unsets framework tuning and uses default native LTO/codegen with opt3/assertions/overflow. Actual compiler source paths resolve into the 1384-input source map. Immutable copied artifacts match original hashes: matched executable 15e1c8fc, native 6365206c; full library/executable hashes, compiler profiles and rustc lines are in [qualification](qualification/). Compiler/protoc/platform identity is recorded without hardware serials.

All 22 commands completed on their original invocation, with no source corrections, collector corrections, restarts or repeats. The reused collector already includes Rust crate-name normalization and compiler-source-map assertions. The original 96 proof files and their manifest verify exactly; binaries/build directories remain outside this public archive. The scopes include native ModelIO crash/session/publication controls and actual process/file recovery; they do not represent physical device-power-loss testing. The unrelated 40-example and whole-file-library histories from the preceding qualification were not repeated.

Mac timing remains held for root's coordination. This archive qualifies frozen 418 only; current 419 work is separate.
