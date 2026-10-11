// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[derive(Debug)]
struct Reduction {
    trace: Trace,
    attempts: usize,
    one_removal_minimal: bool,
}
fn minimize(trace: &Trace, target: &LostPrefix, budget: usize) -> Reduction {
    assert!(trace.actions.len() <= MAX_ACTIONS);
    assert!(budget <= 1024);
    assert_eq!(replay(trace).unwrap_err(), *target);
    let mut reduced = trace.clone();
    let mut attempts = 0;
    let mut cursor = 0;
    while cursor < reduced.actions.len() && attempts < budget {
        let mut candidate = reduced.clone();
        candidate.actions.remove(cursor);
        attempts += 1;
        if replay(&candidate).err().as_ref() == Some(target) {
            reduced = candidate;
            cursor = 0;
        } else {
            cursor += 1;
        }
    }
    Reduction {
        one_removal_minimal: cursor == reduced.actions.len(),
        trace: reduced,
        attempts,
    }
}
fn faulty_trace() -> Trace {
    Trace {
        seed: 42,
        actions: vec![
            Action::Heal,
            Action::Pump,
            Action::Campaign(1),
            Action::Pump,
            Action::Isolate(3),
            Action::BadSync(1),
            Action::BadSync(2),
            Action::TryWrite(1, 7001, 7),
            Action::Pump,
            Action::Heartbeat(1),
            Action::Pump,
            Action::PowerLoss(2),
            Action::Heal,
        ],
    }
}
#[test]
fn actual_false_sync_prefix_loss_is_replayed_and_minimized() {
    let original = faulty_trace();
    let target = LostPrefix {
        node: 2,
        operation: Some(OperationId::new(7001).unwrap()),
        stage: FailureStage::DurableLogMissing,
    };
    assert_eq!(replay(&original), Err(target.clone()));
    let reduced = minimize(&original, &target, 512);
    assert!(reduced.trace.actions.len() < original.actions.len());
    assert!(reduced.attempts <= 512);
    assert!(reduced.one_removal_minimal);
    let saved = reduced.trace.encode();
    let decoded = Trace::decode(&saved).unwrap();
    assert_eq!(replay(&decoded), Err(target.clone()), "{saved}");
    for i in 0..decoded.actions.len() {
        let mut smaller = decoded.clone();
        smaller.actions.remove(i);
        assert_ne!(replay(&smaller).err().as_ref(), Some(&target));
    }
    let good = Trace {
        seed: original.seed,
        actions: original
            .actions
            .iter()
            .filter(|a| !matches!(a, Action::BadSync(_)))
            .cloned()
            .collect(),
    };
    assert!(replay(&good).is_ok());
    eprintln!("minimized actual prefix-loss trace:\n{saved}");
}
#[test]
fn exhausted_reduction_budget_preserves_the_same_failure_without_claiming_minimality() {
    let trace = faulty_trace();
    let target = replay(&trace).unwrap_err();
    let zero = minimize(&trace, &target, 0);
    assert_eq!(zero.attempts, 0);
    assert_eq!(zero.trace, trace);
    assert!(!zero.one_removal_minimal);
    let one = minimize(&trace, &target, 1);
    assert_eq!(one.attempts, 1);
    assert!(!one.one_removal_minimal);
    assert_eq!(replay(&one.trace), Err(target));
}
