# 420 bounded peer lookahead: not promoted

The isolated host change passes20 semantic controls, formatting, strict Clippy and
all source guards on frozen Core418. The patch remains unapplied to the main
comparison: its paired concurrent Linux performance regresses, so it does not
satisfy this deliverable's performance purpose. Existing original-result and
individual-oneshot handling remain unchanged in both variants.

Original9-case exec62212 exits0. Every case validates1,048,576 measured originals,
262,144 batch-four requests,1024 warmups, three frontiers1,049,601, weighted original
histograms, primary4096/4096/1024 envelopes and204 leader checkpoints. All phase
failure counters and before/after source/library/executable guards pass. No local
build/copy overlapped timing, and no slower case was dropped or rerun.

| Variant | Median originals/s | Individual trials | Median request p99 |
| --- | ---: | --- | ---: |
| Frozen418 baseline | 961,644 | 961,644 / 908,462 / 967,795 | 19.829 ms |
| Frozen418 plus420, attribution off | 923,582 | 950,003 / 840,653 / 923,582 | 21.280 ms |
| Frozen418 plus420, attribution on | 934,083 | 893,239 / 934,083 / 947,027 | 20.480 ms |

Off regresses in all three pairs; median within-trial ratio0.9543 (−4.57%). Its
absolute median drops3.96%. Leader proposal batches fall from2335/2277/2290 to
2176/2198/2244, while persistence units shift4620/4537/4584 to4494/4452/4544.
This modest batching difference does not establish a capacity improvement. Candidate
off actor turns are8673/8925/8582; collection decisions/vetoes2176/1,2198/5,2244/2.
Staged peers remain at most64 with bounded owned bytes and full original accounting.
No observer overhead bound follows from the overlapping on/off ranges.

Candidate-on elapsed leader lifetime median1146.58ms includes startup, warmup,
measured work, follower drain and shutdown. Inclusive maintenance409.51ms and
persistence helper309.43ms remain substantial; admission109.72ms, results90.01ms,
nested store append108.61ms and host wait196.09ms overlap broader scopes. These
are not CPU/critical-path totals or paired phase attribution; do not sum them.
The next action is to isolate maintenance helper work before another redesign.

SOURCE/BASE_SOURCE/OVERLAY maps identify exactly1387 inputs: published32217ae
Core plus six changed/new matched inputs. All production inputs match418 exactly.
Actual root library and harness records are fresh:false opt3/assertions/overflow,
thin LTO/codegen1; compiler/independent lock match the baseline. The source-only
patch and reviewed schema specify original FIFO, protected/uninspected-tail
singleton fallback,64-item/1MiB routine lookahead, and limited ready-timer courtesy.
Continuous uninspected streams retain the baseline timer limitation. No production
runtime/provider/security/disk contract is changed by this isolated adapter.

qualification/ preserves the actual418+420 controls and source guards; release-logs/
retains genuine compiler proof. linux-cases/ preserves every original raw result,
command, exit, process timer and guard, plus all derived widths and attribution.
No executable or source checkout is included. This is a short volatile in-process
no-op framework result, not key/value PUTs, physical durability, sustained capacity
or a published-floor pass. No Mac timing is requested for this rejected variant;
both-host qualification remains required before promoting a measured improvement.
