// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

#[test]
fn workload_option_preserves_other_arguments_and_rejects_incomplete_duplicates() {
    let mut input = args(&[
        "root",
        "tcp",
        "50",
        "32",
        "3",
        "--group-weights",
        "2,1,1",
        "--offered",
        "10",
    ]);
    assert_eq!(extract(&mut input).unwrap().as_deref(), Some("2,1,1"));
    assert_eq!(
        input,
        args(&["root", "tcp", "50", "32", "3", "--offered", "10"])
    );
    assert_eq!(extract(&mut input).unwrap(), None);
    for values in [
        vec!["--group-weights"],
        vec!["--group-weights", "--offered"],
        vec!["--group-weights", "1", "--group-weights", "1"],
    ] {
        let mut input = args(&values);
        let before = input.clone();
        assert!(extract(&mut input).is_err());
        assert_eq!(input, before);
    }
}

#[test]
fn weights_are_bounded_positive_workload_inputs_only() {
    for raw in [
        "",
        "0,1",
        "1,0",
        "-1,1",
        "+1,1",
        "1.5,1",
        "1,,1",
        "1",
        "1,1,1",
        "256,1",
        "184467440737095516160,1",
    ] {
        assert!(GroupSchedule::parse(raw, 2).is_err(), "{raw}");
    }
    assert!(GroupSchedule::parse("1", 0).is_err());
    assert!(GroupSchedule::parse("1", 33).is_err());
    assert!(GroupSchedule::from_cli(Some("1"), 1, false).is_err());
    assert!(GroupSchedule::from_cli(None, 1, false).unwrap().is_none());
    let schedule = GroupSchedule::parse("0255,01", 2).unwrap();
    assert_eq!(schedule.slots.len(), 256);
    assert_eq!(
        schedule.metadata(),
        " workload_group_weights=255,1 workload_group_phase=absolute_operation_minus_one"
    );
}

#[test]
fn all_one_weights_preserve_uniform_absolute_phase_including_warmup_offset() {
    for groups in 1..=32 {
        let raw = std::iter::repeat_n("1", groups)
            .collect::<Vec<_>>()
            .join(",");
        let schedule = GroupSchedule::parse(&raw, groups).unwrap();
        for operation in 1..=WARMUP + 500 {
            assert_eq!(
                schedule.group(operation),
                operation_group(operation, groups)
            );
        }
    }
    let skew = GroupSchedule::parse("2,1", 2).unwrap();
    assert_eq!(
        (65..=70)
            .map(|op| skew.group(op).id.get())
            .collect::<Vec<_>>(),
        vec![1, 2, 1, 1, 2, 1]
    );
}

#[test]
fn exact_weighted_counts_and_log_capacity_include_uniform_warmup_and_control_reserve() {
    for raw in ["1,1,1", "7,1,2", "1,255"] {
        let groups = raw.split(',').count();
        let schedule = GroupSchedule::parse(raw, groups).unwrap();
        for count in [1, 2, 3, 255, 256, 257, 15000, 50000] {
            let mut literal = vec![0usize; groups];
            for operation in WARMUP + 1..=WARMUP + count {
                literal[schedule.group(operation).id.get() as usize - 1] += 1;
            }
            assert_eq!(schedule.offer_counts(count), literal);
        }
    }
    let uniform = GroupSchedule::parse("1,1,1", 3).unwrap();
    for count in [1usize, 15000, 48000] {
        let bound = (count + WARMUP).div_ceil(3) + 64;
        for schedule in [None, Some(&uniform)] {
            assert!(validate_capacity(schedule, count, 3, bound).is_ok());
            assert!(validate_capacity(schedule, count, 3, bound - 1).is_err());
        }
    }
    let skew = GroupSchedule::parse("255,1", 2).unwrap();
    assert!((20000usize + WARMUP).div_ceil(2) + 64 < 16384);
    assert!(validate_capacity(None, 20000, 2, 16384).is_ok());
    assert!(validate_capacity(Some(&skew), 20000, 2, 16384).is_err());
    let offers = skew.offer_counts(15000);
    let exact = offers.into_iter().max().unwrap() + WARMUP / 2 + 64;
    assert!(skew.validate_capacity(15000, exact).is_ok());
    assert!(skew.validate_capacity(15000, exact - 1).is_err());
}
