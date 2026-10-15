# Native Counter CPU profiles 479

Original Linux28000 and Mac11837 recordings actually exited0 before either host
began validation, conversion or collection. Both frozen validators and exact
source/artifact/tool/device guards pass. Same published476 native debug0 artifacts;
no recapture, rebuild, browser, SVG, installation or kernel-policy change.

[Linux](linux/README.md): one-group TCP ready64/Witness,8192 offers at1024/s,
window4096. All8192 apply, no refusals, peak pending65,1022.769 completed/s and
p9945.110ms. This profiled diagnostic is offer-capped and cannot explain478's
thousands-pending regime. Saved-data Samply conversion recovers useful ancestry
where the initial perf text unwinder lost most callers:2452 native samples,
5,017,802,056 period weight, only seven single-frame stacks. Reconciliation is
14.306% of native period weight; Counter validation2.937%. Pending B-tree11.863%
overlaps reconciliation11.138%. Unknown and synthetic fun_hex leaves remain
separate; neither names an allocator cause. Original perf results/parser fixes
and the better saved-data conversion are both retained.

[Mac](mac/README.md): TCP ready64/Witness, two groups31:1, same8192/1024/window4096
and2048/group fixture ceiling.6587 apply and1605 hot offers are explicitly refused;
horizon backlog is2024 hot/six cold. All256 cold originals apply without refusal;
hot/cold p993565.098/862.228ms.733.223 completed/s includes drain and is not paired
capacity. The captured backlog supplies the relevant admission context.

Each saved threadCPUDelta contributes once, never multiplied by coalesced wall
sample weights. The4,238,938µs observed interval sum contains exact Counter
validation frames in41.807% of weight: ready26.410%, submission15.397%. Independent
raw traversal agrees. Reserved-set insertion and applied-outcome lookup dominate
resolved admission stacks. These are sampled interval attributions, not CPU
timers:26.464% is assigned to a semaphore wait frame by interval/current-stack
bias. Unknown ancestry27.914%, unknown leaves1.137%, whole-launch/inline/unmarked
unwind limits remain explicit. Mac interval shares are not Linux perf-period
shares or directly comparable workload/CPU measurements.

Root verifies all170 Linux/144 Mac archive members and every losslessly packed
original in [ROOT_ARCHIVE_REVIEW.json](ROOT_ARCHIVE_REVIEW.json). Linux rawperf
stays at its hashed ignored location; saved imported JSON, Mac raw JSON, complete
symbol requests/responses, folded/numerical outputs, failed analysis attempts,
original commands and actual exits remain archived. Owned symbol servers joined.

The [480 pre-edit schema](../native480-admission/SCHEMA.md) targets the supported
ready-stage repetition with one temporary exact reservation context, retaining
the original validator as oracle. Submission and Linux reconciliation remain
separate measured work. No speedup follows from these profiles or implementation;
fresh published artifacts and paired useful-rate/tail/refusal checks follow.
Defaults, pending ext4/XFS acceptance and the full P9 objective remain unchanged.
