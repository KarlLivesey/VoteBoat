# Mac425 same-artifact diagnostic

Original SSH 66027 exited0. Nine rotating cases completed 1048576 measured originals/262144 requests each, plus 1024 warmup originals, with all applied frontiers 1049601. VoteBoat off/on and pinned OpenRaft used one same qualified ccee8eca asserted executable, c4096/b4, primary equal-original envelopes, 16 server workers, zero payload and unchanged default maintenance. Every process exit, command, raw JSON/stderr, bash wall/user/system time, before/after source/artifact/compiler/driver guards and checker output is retained. No build/fetch/copy overlapped timing.

| Mode | Median originals/s | Median request p99 |
| --- | ---: | ---: |
| VoteBoat off | 2668504 | 6.650ms |
| VoteBoat on | 2719963 | 6.702ms |
| OpenRaft | 2165089 | 9.416ms |

Rates varied across trials: off 3.398M/2.669M/2.583M, on 3.389M/2.720M/2.718M, upstream 2.274M/2.165M/2.003M. This is a short diagnostic, not sustained capacity, a healthy-margin goal qualification or an optimization gain. Observer impact is mixed, so no benefit is assigned to instrumentation. Published floors and sustained matched whole curves remain outstanding.

ON leaders each performed 204 checkpoints. Inclusive compaction was 31.823/40.363/42.263ms; state-to-pin 13.258/16.763/17.760ms (41.5–42.0%); state construction 6.397/8.169/8.704ms; pin-to-append 1.046/1.289/1.273ms; barrier-to-reconcile 5.828/7.429/7.553ms. The source intervals cover complete successful helper observations across actor lifetime, including warmup/drain. State-to-pin includes exact full Eq, temporary destruction and subsequent guards; construction is a separate provider interval. Pin-to-append includes nested pinned loading/verification/restore/planning. Barrier-to-reconcile includes completion/install/collector retirement/broadcast/return overhead. None is pure Eq, Core CPU or a critical path; nested intervals and replicas must not be added as independent costs.

Linux's approximately 93% state-to-pin fraction differs from Mac's approximately 42%. This is observed elapsed attribution, not evidence of allocator/syscall/TLB cause. A borrowed exact comparison could remove clone construction/destruction while retaining full content equality and all existing binding/visibility/error proofs, but this observer does not establish how much of the full gap would disappear. Any next change requires checked paired evidence.

The driver/checker/guard bytes are identical to root's Linux comparison425 scripts; only CONFIG paths/tool/artifact identities and the PATH-only SSH wrapper differ. All1409 input guards include the two separately recorded post-build supplemental documents; compiler guard scope remains explicit. No product or public-helper/provider contract was modified.
