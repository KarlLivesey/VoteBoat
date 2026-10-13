Frozen Mac 419 qualification completed once on `7511e5ec627bbe910b8ba901e36daad090e9ce6d`: original runner 72073 / PID 92910, exit 0. All 1,390 compiled-input hashes passed before and after, including `incoming_codec.rs` and the exact Clippy configuration. The earlier incomplete 1,389-input export and live 421 source were excluded.

| Actual scoped controls | All features | Core only |
| --- | ---: | ---: |
| Incoming allocation/identity/atomicity/correlation/codec-first | 12 | 9 |
| Log transition/owned library | 29 | 29 |
| Native accepted overlays | 14 | feature excluded |
| Core Raft library | 111 | 92 |
| log_store / raft / snapshot integrations | 15 / 79 / 25 | 0 (feature filtered) / 29 / 10 |

Actual TCP/TLS and QUIC original retry/leader replacement/native recovery passed 2 controls, and maintenance/checkpoint/reclaim/restart/invalid schedules passed 6. Standalone matched tests passed 11; four small CLI controls verified every 16,384 original, 4,096 request outcomes and all three applied frontiers 17,409 across both engines and envelope policies. Formatting and all four root strict Clippy profiles passed, as did standalone formatting and strict lint. These are scoped results, not a whole unrelated Mac suite claim.

Independent fresh asserted release targets produced actual root-library and executable `fresh:false` compiler records with opt3, debug assertions and overflow checks enabled. Matched uses thin LTO / one codegen unit; native explicitly removes those framework overrides and uses native defaults. Original/copy library and executable hashes plus compiler-source records are in `qualification/ARTIFACTS.json`. Immutable remote executables are `/tmp/voteboat-mac419-7511e5e/artifacts/matched/voteboat-matched-framework` and `/tmp/voteboat-mac419-7511e5e/artifacts/native/native_benchmark`. No executable, rlib, checkout or build tree is published here.

All 101 original proof records and their hash manifest are retained in `qualification/`; the publication hash index is `EVIDENCE.json`. No failure, restart, source correction or timed run occurred in this qualification. Throughput, latency and published-floor attainment remain unmeasured for this Mac slice.
