
### `whole` — E3, whole binary

| side | run | exit | wall s | passed/failed | target test | non-poison roots (test → file:line:col → error) | poison cascades | log |
|---|---|---|---|---|---|---|---|---|
| parent | 1 | 101 | 46.8 | 24/7 | ok | `real_parent_intent_replicates_learners_but_cannot_promote_absent_native_tcp` → `tests/logical_activation_driver/learners.rs:95:5` → original bridge usage LogicalBridgeUsage { actions: 0, command_bytes: 0, child_a | 6 | `whole-01-parent.log` |
| pr | 1 | 0 | 25.8 | 31/0 | ok | — | 0 | `whole-01-pr.log` |
| parent | 2 | 101 | 40.9 | 24/7 | ok | `real_parent_intent_replicates_learners_but_cannot_promote_absent_native_tcp` → `tests/logical_activation_driver/learners.rs:95:5` → original bridge usage LogicalBridgeUsage { actions: 0, command_bytes: 0, child_a | 6 | `whole-02-parent.log` |
| pr | 2 | 101 | 23.8 | 24/7 | ok | `real_parent_intent_replicates_learners_but_cannot_promote_absent_native_tcp` → `tests/logical_activation_driver/learners.rs:95:5` → original bridge usage LogicalBridgeUsage { actions: 0, command_bytes: 0, child_a | 6 | `whole-02-pr.log` |
| parent | 3 | 101 | 6.7 | 10/21 | ok | `three_logical_parents_ten_native_owners_replace_distinct_child_quic` → `tests/logical_activation_driver/cohort/members.rs:56:70` → Os { code: 98, kind: AddrInUse, message: "Address already in use" } | 20 | `whole-03-parent.log` |
| pr | 3 | 0 | 25.9 | 31/0 | ok | — | 0 | `whole-03-pr.log` |
| parent | 4 | 101 | 17.9 | 13/18 | ok | `full_child_replacement_retains_original_ordinary_client_and_read_count_pressure_quic` → `tests/logical_activation_driver/cohort/members.rs:56:70` → Os { code: 98, kind: AddrInUse, message: "Address already in use" } | 17 | `whole-04-parent.log` |
| pr | 4 | 0 | 28.7 | 31/0 | ok | — | 0 | `whole-04-pr.log` |
| parent | 5 | 101 | 10.7 | 11/20 | ok | `three_logical_parents_ten_native_owners_replace_distinct_child_tcp` → `tests/logical_activation_driver/cohort/members.rs:56:70` → Os { code: 98, kind: AddrInUse, message: "Address already in use" } | 19 | `whole-05-parent.log` |
| pr | 5 | 0 | 43.2 | 31/0 | ok | — | 0 | `whole-05-pr.log` |
| parent | 6 | 101 | 18.2 | 13/18 | ok | `full_child_replacement_retains_original_ordinary_client_and_read_count_pressure_quic` → `tests/logical_activation_driver/cohort/members.rs:56:70` → Os { code: 98, kind: AddrInUse, message: "Address already in use" } | 17 | `whole-06-parent.log` |
| pr | 6 | 0 | 26.5 | 31/0 | ok | — | 0 | `whole-06-pr.log` |
| parent | 7 | 101 | 0.2 | 3/28 | FAILED (poison) | `full_child_replacement_retains_original_client_byte_and_read_count_pressure_quic` → `tests/logical_activation_driver/cohort/members.rs:56:70` → Os { code: 98, kind: AddrInUse, message: "Address already in use" } | 27 | `whole-07-parent.log` |
| pr | 7 | 0 | 27.5 | 31/0 | ok | — | 0 | `whole-07-pr.log` |
| parent | 8 | 0 | 24.7 | 31/0 | ok | — | 0 | `whole-08-parent.log` |
| pr | 8 | 101 | 6.5 | 7/24 | FAILED (poison) | `three_logical_parents_ten_native_owners_replace_distinct_child_quic` → `tests/logical_activation_driver/cohort/members.rs:56:70` → Os { code: 98, kind: AddrInUse, message: "Address already in use" } | 23 | `whole-08-pr.log` |
| parent | 9 | 0 | 42.7 | 31/0 | ok | — | 0 | `whole-09-parent.log` |
| pr | 9 | 0 | 25.1 | 31/0 | ok | — | 0 | `whole-09-pr.log` |
| parent | 10 | 0 | 41.7 | 31/0 | ok | — | 0 | `whole-10-parent.log` |
| pr | 10 | 0 | 43.5 | 31/0 | ok | — | 0 | `whole-10-pr.log` |

- **parent**: close root in 0/10 runs; target test ran to a real result (not poisoned/not run) in 9/10
- **pr**: close root in 0/10 runs; target test ran to a real result (not poisoned/not run) in 9/10
- sampled absolute test nice: [-4, 15] (counts [(-4, 2), (15, 17)]); affinity ['0-3']; no sample (exited <0.2 s): 1; burner nice ['[-4, -4, -4]']
- load before first run: `04:39:03 up  6:13,  1 user,  load average: 5.78, 5.47, 4.93`; after last run: `04:48:00 up  6:22,  1 user,  load average: 6.95, 6.55, 5.74`

### `exact` — E3, `--exact` target test

| side | run | exit | wall s | passed/failed | target test | non-poison roots (test → file:line:col → error) | poison cascades | log |
|---|---|---|---|---|---|---|---|---|
| parent | 1 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-01-parent.log` |
| pr | 1 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-01-pr.log` |
| parent | 2 | 0 | 0.3 | 1/0 | ok | — | 0 | `exact-02-parent.log` |
| pr | 2 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-02-pr.log` |
| parent | 3 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-03-parent.log` |
| pr | 3 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-03-pr.log` |
| parent | 4 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-04-parent.log` |
| pr | 4 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-04-pr.log` |
| parent | 5 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-05-parent.log` |
| pr | 5 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-05-pr.log` |
| parent | 6 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-06-parent.log` |
| pr | 6 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-06-pr.log` |
| parent | 7 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-07-parent.log` |
| pr | 7 | 0 | 0.3 | 1/0 | ok | — | 0 | `exact-07-pr.log` |
| parent | 8 | 0 | 0.3 | 1/0 | ok | — | 0 | `exact-08-parent.log` |
| pr | 8 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-08-pr.log` |
| parent | 9 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-09-parent.log` |
| pr | 9 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-09-pr.log` |
| parent | 10 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-10-parent.log` |
| pr | 10 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-10-pr.log` |
| parent | 11 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-11-parent.log` |
| pr | 11 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-11-pr.log` |
| parent | 12 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-12-parent.log` |
| pr | 12 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-12-pr.log` |
| parent | 13 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-13-parent.log` |
| pr | 13 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-13-pr.log` |
| parent | 14 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-14-parent.log` |
| pr | 14 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-14-pr.log` |
| parent | 15 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-15-parent.log` |
| pr | 15 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-15-pr.log` |
| parent | 16 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-16-parent.log` |
| pr | 16 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-16-pr.log` |
| parent | 17 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-17-parent.log` |
| pr | 17 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-17-pr.log` |
| parent | 18 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-18-parent.log` |
| pr | 18 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-18-pr.log` |
| parent | 19 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-19-parent.log` |
| pr | 19 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-19-pr.log` |
| parent | 20 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-20-parent.log` |
| pr | 20 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-20-pr.log` |
| parent | 21 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-21-parent.log` |
| pr | 21 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-21-pr.log` |
| parent | 22 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-22-parent.log` |
| pr | 22 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-22-pr.log` |
| parent | 23 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-23-parent.log` |
| pr | 23 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-23-pr.log` |
| parent | 24 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-24-parent.log` |
| pr | 24 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-24-pr.log` |
| parent | 25 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-25-parent.log` |
| pr | 25 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-25-pr.log` |
| parent | 26 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-26-parent.log` |
| pr | 26 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-26-pr.log` |
| parent | 27 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-27-parent.log` |
| pr | 27 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-27-pr.log` |
| parent | 28 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-28-parent.log` |
| pr | 28 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-28-pr.log` |
| parent | 29 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-29-parent.log` |
| pr | 29 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-29-pr.log` |
| parent | 30 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-30-parent.log` |
| pr | 30 | 0 | 0.2 | 1/0 | ok | — | 0 | `exact-30-pr.log` |

- **parent**: close root in 0/30 runs; target test ran to a real result (not poisoned/not run) in 30/30
- **pr**: close root in 0/30 runs; target test ran to a real result (not poisoned/not run) in 30/30
- sampled absolute test nice: [-4, 15] (counts [(-4, 6), (15, 44)]); affinity ['0-3']; no sample (exited <0.2 s): 10; burner nice ['[-4, -4, -4]']
- load before first run: `04:48:45 up  6:23,  1 user,  load average: 5.37, 6.19, 5.66`; after last run: `04:49:28 up  6:23,  1 user,  load average: 5.30, 6.06, 5.64`

### `squeeze-cal-n10` — squeeze calibration: test `taskset -c 1 nice -n 10`, 1 burner on cpu 1

| side | run | exit | wall s | passed/failed | target test | non-poison roots (test → file:line:col → error) | poison cascades | log |
|---|---|---|---|---|---|---|---|---|
| parent | 1 | 0 | 0.6 | 1/0 | ok | — | 0 | `squeeze-cal-n10-01-parent.log` |
| pr | 1 | 0 | 0.6 | 1/0 | ok | — | 0 | `squeeze-cal-n10-01-pr.log` |
| parent | 2 | 0 | 0.6 | 1/0 | ok | — | 0 | `squeeze-cal-n10-02-parent.log` |
| pr | 2 | 0 | 0.6 | 1/0 | ok | — | 0 | `squeeze-cal-n10-02-pr.log` |

- **parent**: close root in 0/2 runs; target test ran to a real result (not poisoned/not run) in 2/2
- **pr**: close root in 0/2 runs; target test ran to a real result (not poisoned/not run) in 2/2
- sampled absolute test nice: [6] (counts [(6, 4)]); affinity ['1']; no sample (exited <0.2 s): 0; burner nice ['[-4]']
- load before first run: `04:49:56 up  6:24,  1 user,  load average: 5.07, 5.94, 5.61`; after last run: `04:50:00 up  6:24,  1 user,  load average: 4.98, 5.91, 5.60`

### `squeeze-cal-n14` — squeeze calibration: `taskset -c 1 nice -n 14`, 1 burner on cpu 1

| side | run | exit | wall s | passed/failed | target test | non-poison roots (test → file:line:col → error) | poison cascades | log |
|---|---|---|---|---|---|---|---|---|
| parent | 1 | 0 | 1.4 | 1/0 | ok | — | 0 | `squeeze-cal-n14-01-parent.log` |
| pr | 1 | 0 | 1.4 | 1/0 | ok | — | 0 | `squeeze-cal-n14-01-pr.log` |
| parent | 2 | 0 | 1.4 | 1/0 | ok | — | 0 | `squeeze-cal-n14-02-parent.log` |
| pr | 2 | 0 | 1.4 | 1/0 | ok | — | 0 | `squeeze-cal-n14-02-pr.log` |

- **parent**: close root in 0/2 runs; target test ran to a real result (not poisoned/not run) in 2/2
- **pr**: close root in 0/2 runs; target test ran to a real result (not poisoned/not run) in 2/2
- sampled absolute test nice: [10] (counts [(10, 4)]); affinity ['1']; no sample (exited <0.2 s): 0; burner nice ['[-4]']
- load before first run: `04:50:01 up  6:24,  1 user,  load average: 4.98, 5.91, 5.60`; after last run: `04:50:08 up  6:24,  1 user,  load average: 5.06, 5.89, 5.60`

### `squeeze-cal-n19` — squeeze calibration: `taskset -c 1 nice -n 19`, 1 burner on cpu 1

| side | run | exit | wall s | passed/failed | target test | non-poison roots (test → file:line:col → error) | poison cascades | log |
|---|---|---|---|---|---|---|---|---|
| parent | 1 | 0 | 5.6 | 1/0 | ok | — | 0 | `squeeze-cal-n19-01-parent.log` |
| pr | 1 | 0 | 5.6 | 1/0 | ok | — | 0 | `squeeze-cal-n19-01-pr.log` |
| parent | 2 | 0 | 5.8 | 1/0 | ok | — | 0 | `squeeze-cal-n19-02-parent.log` |
| pr | 2 | 0 | 4.6 | 1/0 | ok | — | 0 | `squeeze-cal-n19-02-pr.log` |

- **parent**: close root in 0/2 runs; target test ran to a real result (not poisoned/not run) in 2/2
- **pr**: close root in 0/2 runs; target test ran to a real result (not poisoned/not run) in 2/2
- sampled absolute test nice: [15] (counts [(15, 4)]); affinity ['1']; no sample (exited <0.2 s): 0; burner nice ['[-4]']
- load before first run: `04:50:08 up  6:24,  1 user,  load average: 5.06, 5.89, 5.60`; after last run: `04:50:31 up  6:24,  1 user,  load average: 5.26, 5.88, 5.60`

### `squeeze-cal-b11` — squeeze calibration: `taskset -c 1 nice -n 19`, 2 burners on cpu 1

| side | run | exit | wall s | passed/failed | target test | non-poison roots (test → file:line:col → error) | poison cascades | log |
|---|---|---|---|---|---|---|---|---|
| parent | 1 | 101 | 26.5 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-cal-b11-01-parent.log` |
| pr | 1 | 0 | 16.8 | 1/0 | ok | — | 0 | `squeeze-cal-b11-01-pr.log` |
| parent | 2 | 0 | 17.3 | 1/0 | ok | — | 0 | `squeeze-cal-b11-02-parent.log` |
| pr | 2 | 0 | 13.4 | 1/0 | ok | — | 0 | `squeeze-cal-b11-02-pr.log` |

- **parent**: close root in 1/2 runs; target test ran to a real result (not poisoned/not run) in 2/2
- **pr**: close root in 0/2 runs; target test ran to a real result (not poisoned/not run) in 2/2
- sampled absolute test nice: [15] (counts [(15, 4)]); affinity ['1']; no sample (exited <0.2 s): 0; burner nice ['[-4, -4]']
- load before first run: `04:50:46 up  6:25,  1 user,  load average: 6.01, 6.01, 5.65`; after last run: `04:52:01 up  6:26,  1 user,  load average: 6.92, 6.31, 5.79`

### `squeeze-cal-b111` — squeeze calibration: `taskset -c 1 nice -n 19`, 3 burners on cpu 1

| side | run | exit | wall s | passed/failed | target test | non-poison roots (test → file:line:col → error) | poison cascades | log |
|---|---|---|---|---|---|---|---|---|
| parent | 1 | 101 | 43.4 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-cal-b111-01-parent.log` |
| pr | 1 | 101 | 34.2 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-cal-b111-01-pr.log` |
| parent | 2 | 101 | 44.6 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-cal-b111-02-parent.log` |
| pr | 2 | 101 | 48.5 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-cal-b111-02-pr.log` |

- **parent**: close root in 2/2 runs; target test ran to a real result (not poisoned/not run) in 2/2
- **pr**: close root in 2/2 runs; target test ran to a real result (not poisoned/not run) in 2/2
- sampled absolute test nice: [15] (counts [(15, 4)]); affinity ['1']; no sample (exited <0.2 s): 0; burner nice ['[-4, -4, -4]']
- load before first run: `04:52:02 up  6:26,  1 user,  load average: 6.92, 6.31, 5.79`; after last run: `04:54:54 up  6:29,  1 user,  load average: 8.07, 7.16, 6.22`

### `squeeze-b11` — squeeze series: `taskset -c 1 nice -n 19`, 2 burners on cpu 1

| side | run | exit | wall s | passed/failed | target test | non-poison roots (test → file:line:col → error) | poison cascades | log |
|---|---|---|---|---|---|---|---|---|
| parent | 1 | 0 | 13.1 | 1/0 | ok | — | 0 | `squeeze-b11-01-parent.log` |
| pr | 1 | 0 | 11.7 | 1/0 | ok | — | 0 | `squeeze-b11-01-pr.log` |
| parent | 2 | 0 | 12.5 | 1/0 | ok | — | 0 | `squeeze-b11-02-parent.log` |
| pr | 2 | 0 | 16.8 | 1/0 | ok | — | 0 | `squeeze-b11-02-pr.log` |
| parent | 3 | 0 | 12.7 | 1/0 | ok | — | 0 | `squeeze-b11-03-parent.log` |
| pr | 3 | 0 | 14.4 | 1/0 | ok | — | 0 | `squeeze-b11-03-pr.log` |
| parent | 4 | 101 | 20.1 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b11-04-parent.log` |
| pr | 4 | 101 | 23.4 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b11-04-pr.log` |
| parent | 5 | 101 | 24.8 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b11-05-parent.log` |
| pr | 5 | 0 | 16.9 | 1/0 | ok | — | 0 | `squeeze-b11-05-pr.log` |
| parent | 6 | 101 | 16.2 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b11-06-parent.log` |
| pr | 6 | 0 | 13.3 | 1/0 | ok | — | 0 | `squeeze-b11-06-pr.log` |
| parent | 7 | 0 | 12.9 | 1/0 | ok | — | 0 | `squeeze-b11-07-parent.log` |
| pr | 7 | 0 | 12.0 | 1/0 | ok | — | 0 | `squeeze-b11-07-pr.log` |
| parent | 8 | 0 | 13.2 | 1/0 | ok | — | 0 | `squeeze-b11-08-parent.log` |
| pr | 8 | 0 | 12.7 | 1/0 | ok | — | 0 | `squeeze-b11-08-pr.log` |
| parent | 9 | 0 | 12.5 | 1/0 | ok | — | 0 | `squeeze-b11-09-parent.log` |
| pr | 9 | 0 | 12.4 | 1/0 | ok | — | 0 | `squeeze-b11-09-pr.log` |
| parent | 10 | 0 | 13.1 | 1/0 | ok | — | 0 | `squeeze-b11-10-parent.log` |
| pr | 10 | 0 | 12.4 | 1/0 | ok | — | 0 | `squeeze-b11-10-pr.log` |

- **parent**: close root in 3/10 runs; target test ran to a real result (not poisoned/not run) in 10/10
- **pr**: close root in 1/10 runs; target test ran to a real result (not poisoned/not run) in 10/10
- sampled absolute test nice: [15] (counts [(15, 20)]); affinity ['1']; no sample (exited <0.2 s): 0; burner nice ['[-4, -4]']
- load before first run: `04:55:04 up  6:29,  1 user,  load average: 7.13, 6.99, 6.17`; after last run: `05:00:10 up  6:34,  1 user,  load average: 6.62, 6.89, 6.36`

### `squeeze-b111` — squeeze series: `taskset -c 1 nice -n 19`, 3 burners on cpu 1

| side | run | exit | wall s | passed/failed | target test | non-poison roots (test → file:line:col → error) | poison cascades | log |
|---|---|---|---|---|---|---|---|---|
| parent | 1 | 101 | 31.8 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-01-parent.log` |
| pr | 1 | 101 | 33.4 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-01-pr.log` |
| parent | 2 | 101 | 28.5 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-02-parent.log` |
| pr | 2 | 101 | 28.4 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-02-pr.log` |
| parent | 3 | 101 | 33.0 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-03-parent.log` |
| pr | 3 | 101 | 29.6 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-03-pr.log` |
| parent | 4 | 101 | 29.8 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-04-parent.log` |
| pr | 4 | 101 | 54.0 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-04-pr.log` |
| parent | 5 | 101 | 32.6 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-05-parent.log` |
| pr | 5 | 101 | 28.7 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-05-pr.log` |
| parent | 6 | 101 | 44.0 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-06-parent.log` |
| pr | 6 | 101 | 33.0 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-06-pr.log` |
| parent | 7 | 101 | 32.0 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-07-parent.log` |
| pr | 7 | 101 | 27.4 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-07-pr.log` |
| parent | 8 | 101 | 31.2 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-08-parent.log` |
| pr | 8 | 101 | 31.4 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-08-pr.log` |
| parent | 9 | 101 | 32.7 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-09-parent.log` |
| pr | 9 | 101 | 35.7 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-09-pr.log` |
| parent | 10 | 101 | 31.6 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-10-parent.log` |
| pr | 10 | 101 | 36.8 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-b111-10-pr.log` |

- **parent**: close root in 10/10 runs; target test ran to a real result (not poisoned/not run) in 10/10
- **pr**: close root in 10/10 runs; target test ran to a real result (not poisoned/not run) in 10/10
- sampled absolute test nice: [15] (counts [(15, 20)]); affinity ['1']; no sample (exited <0.2 s): 0; burner nice ['[-4, -4, -4]']
- load before first run: `05:00:11 up  6:34,  1 user,  load average: 6.62, 6.89, 6.36`; after last run: `05:11:26 up  6:45,  1 user,  load average: 7.63, 7.32, 6.91`

### `squeeze-bt` — squeeze + `RUST_BACKTRACE=1`: `taskset -c 1 nice -n 19`, 3 burners on cpu 1

| side | run | exit | wall s | passed/failed | target test | non-poison roots (test → file:line:col → error) | poison cascades | log |
|---|---|---|---|---|---|---|---|---|
| parent | 1 | 101 | 36.0 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-bt-01-parent.log` |
| pr | 1 | 101 | 37.5 | 0/1 | ok | `shutdown_rejection_retains_original_cancellation_before_later_provider_fault` → `tests/logical_activation_driver/cohort/close/progress.rs:126:51` → Network(Session(Closed)) | 0 | `squeeze-bt-01-pr.log` |

- **parent**: close root in 1/1 runs; target test ran to a real result (not poisoned/not run) in 1/1
- **pr**: close root in 1/1 runs; target test ran to a real result (not poisoned/not run) in 1/1
- sampled absolute test nice: [15] (counts [(15, 2)]); affinity ['1']; no sample (exited <0.2 s): 0; burner nice ['[-4, -4, -4]']
- load before first run: `05:11:43 up  6:46,  1 user,  load average: 6.24, 7.02, 6.82`; after last run: `05:12:57 up  6:47,  1 user,  load average: 6.53, 6.97, 6.81`
