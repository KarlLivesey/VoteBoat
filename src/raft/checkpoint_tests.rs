// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Existing-owner mutation-domain controls with modeled scoped completions.
use super::*;
use membership_tests::{core, durable};
use std::collections::VecDeque;

#[test]
fn same_core_suffix_replacement_and_replayed_higher_term_replace_lineage() {
    let mut core = core();
    let mut comparison = CheckpointComparison::default();
    assert!(comparison.compare(core.state(), &core.checkpoint_log()));
    let first = core.checkpoint_log().lineage().clone();
    let effects = core
        .propose_batch(vec![
            Proposal {
                operation: OperationId::new(u128::MAX).unwrap(),
                bytes: Vec::new(),
            },
            Proposal {
                operation: OperationId::new(7).unwrap(),
                bytes: vec![7],
            },
        ])
        .unwrap();
    durable(&mut core, effects);
    comparison.preserve(core.state());
    assert_eq!(first, *core.checkpoint_log().lineage());
    assert!(comparison.compare(core.state(), &core.checkpoint_log()));
    assert_eq!(comparison.work().compared, 2);
    assert_eq!(comparison.work().reused, 1);
    let sender = StoreBinding {
        identity: core.state().bootstrap.voter_stores[&NodeId::new(2).unwrap()],
        session: StoreSession::new(1).unwrap(),
    };
    let message = Message {
        group: core.state().bootstrap.group,
        configuration: core.membership().id(),
        from: NodeId::new(2).unwrap(),
        sender,
        to: NodeId::new(1).unwrap(),
        term: 2,
        context: RequestContext {
            origin: sender,
            sequence: 1,
        },
        rpc: Rpc::Append {
            previous_index: 1,
            previous_term: 1,
            entries: vec![
                LogEntry {
                    index: 2,
                    term: 2,
                    payload: EntryPayload::Command {
                        operation: OperationId::new((1 << 127) | 7).unwrap(),
                        bytes: vec![9],
                    },
                },
                LogEntry {
                    index: 3,
                    term: 2,
                    payload: EntryPayload::Noop,
                },
            ],
            leader_commit: 1,
        },
    };
    let mut effects = VecDeque::from(core.step(Event::Receive(message)).unwrap());
    let mut full_installs = 0;
    while let Some(effect) = effects.pop_front() {
        if let Effect::Persist(update) = effect {
            let old = core.checkpoint_log().lineage().clone();
            effects.extend(durable(&mut core, vec![Effect::Persist(update)]));
            assert_ne!(old, *core.checkpoint_log().lineage());
            full_installs += 1;
        }
    }
    assert_eq!(
        full_installs, 1,
        "term and replacement share one atomic durable installation"
    );
    assert_eq!(core.state().hard_state.term, 2);
    assert_eq!(core.state().entries[1].term, 2);
    assert_ne!(first, *core.checkpoint_log().lineage());
    assert!(comparison.compare(core.state(), &core.checkpoint_log()));
    assert!(comparison.work().full);
    assert_eq!(comparison.work().compared, 3);
    assert_eq!(comparison.work().reused, 0);
}

#[test]
fn heartbeat_keeps_domain_but_recovery_and_failed_owner_do_not() {
    let mut core = core();
    let before = core.checkpoint_log().lineage().clone();
    core.step(Event::Heartbeat).unwrap();
    assert_eq!(before, *core.checkpoint_log().lineage());
    let recovered =
        Raft::recover(core.node, core.binding, core.state().clone(), core.limits).unwrap();
    assert_eq!(core.state(), recovered.state());
    assert_ne!(before, *recovered.checkpoint_log().lineage());
    core.storage_failed();
    assert_ne!(before, *core.checkpoint_log().lineage());
}
