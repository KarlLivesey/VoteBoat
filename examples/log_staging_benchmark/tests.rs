// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{arguments, journal::*, workload, History, Workload};
use std::{cell::RefCell, rc::Rc};
use voteboat::native::log_store::JournalIo;

#[test]
fn bounded_workload_rejects_zero_and_oversized_inputs() {
    for (unrelated, history, iterations) in
        [(33, 2, 1), (0, 1, 1), (0, 4097, 1), (0, 2, 0), (0, 2, 2001)]
    {
        assert!(Workload {
            unrelated,
            history,
            iterations,
            append: false,
            commit: false,
            kind: History::Plain,
        }
        .validate()
        .is_err());
    }
    assert!(arguments(&[]).is_err());
    let args = ["output.csv", "8", "4", "3"].map(str::to_owned);
    let (_, config) = arguments(&args).unwrap();
    assert_eq!(config.unrelated, 8);
    assert_eq!(config.history, 4);
    assert_eq!(config.iterations, 3);
}

#[test]
fn fixed_histories_and_all_unrelated_groups_survive_exact_native_recovery() {
    let mut previous = None;
    for unrelated in [0, 2, 8] {
        let samples = workload::run(Workload {
            unrelated,
            history: 4,
            iterations: 3,
            append: false,
            commit: false,
            kind: History::Plain,
        })
        .unwrap();
        assert_eq!(samples.len(), 3);
        let first = samples[0].io;
        assert_eq!(first.appends, 1);
        assert_eq!(first.syncs, 1);
        assert_eq!(first.publications, 1);
        assert_eq!(first.manifest_bytes, 52);
        if let Some(expected) = previous {
            assert_eq!(first, expected);
        }
        previous = Some(first);
        for (index, sample) in samples.iter().enumerate() {
            assert_eq!(sample.iteration, index);
            assert_eq!(sample.io, first);
        }
    }
}

#[test]
fn volatile_journal_retains_only_simulated_synchronized_prefix() {
    let device = Rc::new(RefCell::new(Device::new(16, 16)));
    let mut io = VolatileIo(device.clone());
    io.append(&[1, 2, 3]).unwrap();
    io.sync_log().unwrap();
    io.append(&[4, 5]).unwrap();
    assert!(io.read_log(4).is_err());
    device.borrow_mut().power_loss();
    assert_eq!(io.read_log(3).unwrap(), [1, 2, 3]);
    assert!(io.append(&[0; 14]).is_err());
    assert!(io.truncate_log(4).is_err());
    io.truncate_log(2).unwrap();
    assert_eq!(io.read_log(2).unwrap(), [1, 2]);
}

#[test]
fn append_histories_preserve_exact_tickets_and_restart_with_unchanged_neighbors() {
    let args = ["output.csv", "2", "4", "3", "append"].map(str::to_owned);
    let (_, config) = arguments(&args).unwrap();
    assert!(config.append);
    let samples = workload::run(config).unwrap();
    assert_eq!(samples.len(), 3);
    for (index, sample) in samples.iter().enumerate() {
        assert_eq!(sample.iteration, index);
        assert_eq!(sample.io.appends, 1);
        assert_eq!(sample.io.syncs, 1);
        assert_eq!(sample.io.publications, 1);
    }
    let bad = ["output.csv", "2", "4", "3", "unbounded"].map(str::to_owned);
    assert!(arguments(&bad).is_err());
}

#[test]
fn configured_and_checkpoint_updates_match_full_validator_through_exact_recovery() {
    for kind in ["plain", "stable", "joint", "checkpoint"] {
        for mode in ["append", "append-commit"] {
            let args = ["output.csv", "2", "4", "3", mode, kind].map(str::to_owned);
            let (_, config) = arguments(&args).unwrap();
            assert_eq!(config.kind.label(), kind);
            assert_eq!(config.commit, mode == "append-commit");
            let samples = workload::run(config).unwrap();
            assert_eq!(samples.len(), 3);
            for sample in samples {
                assert_eq!(sample.io.appends, 1);
                assert_eq!(sample.io.syncs, 1);
                assert_eq!(sample.io.publications, 1);
                assert_eq!(sample.io.manifest_bytes, 52);
            }
        }
    }
    for args in [
        ["output.csv", "2", "2", "3", "append", "joint"],
        ["output.csv", "2", "4", "3", "replace", "stable"],
        ["output.csv", "2", "4", "3", "append", "unknown"],
    ] {
        assert!(arguments(&args.map(str::to_owned)).is_err());
    }
}
