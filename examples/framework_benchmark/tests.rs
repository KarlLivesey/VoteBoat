// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

#[test]
fn singleton_counts_only_original_applied_results() {
    let settings = Settings {
        clients: 1,
        operations: 16,
        warmup: 2,
        ..Settings::default()
    };
    let report = workload::run(settings).unwrap();
    assert_eq!(report.completed, 16);
    assert_eq!(report.peak_pending, 1);
    assert_eq!(report.frontiers, [19; 3]);
    assert!(report
        .json(settings)
        .contains("\"comparison_gate\":\"unmatched\""));
}

#[test]
fn multiple_clients_remain_outstanding_until_original_application() {
    let settings = Settings {
        clients: 4,
        operations: 32,
        warmup: 0,
        ..Settings::default()
    };
    let report = workload::run(settings).unwrap();
    assert_eq!(report.completed, 32);
    assert!(report.peak_pending > 1);
    assert!(report.peak_pending <= 4);
    assert_eq!(report.frontiers, [33; 3]);
}

#[test]
fn invalid_batch_and_hidden_history_reset_are_refused() {
    assert!(Settings {
        batch: 65,
        ..Settings::default()
    }
    .validate()
    .is_err());
    assert!(Settings {
        retained_entries: MAX_RETAINED_ENTRIES + 1,
        ..Settings::default()
    }
    .validate()
    .is_err());
    assert!(Settings {
        retained_entries: 1024,
        ..Settings::default()
    }
    .validate()
    .is_err());
    assert!(Settings {
        clients: 0,
        ..Settings::default()
    }
    .validate()
    .is_err());
    assert!(Settings {
        clients: 3,
        operations: 16,
        ..Settings::default()
    }
    .validate()
    .is_err());
}

#[test]
fn four_entry_batches_return_every_original_result_from_three_voters() {
    let settings = Settings {
        clients: 4,
        operations: 32,
        warmup: 2,
        batch: 4,
        ..Settings::default()
    };
    let report = workload::run(settings).unwrap();
    assert_eq!(report.completed, 32);
    assert_eq!(report.completed_batches, 8);
    assert!(report.peak_pending > 4);
    assert!(report.peak_pending <= 16);
    assert_eq!(report.frontiers, [35; 3]);
    let json = report.json(settings);
    assert!(json.contains("\"comparison_gate\":\"unmatched\""));
    assert!(json.contains("\"batch\":4"));
    assert!(!json.contains("batch_many_unsupported"));
}

#[test]
fn volatile_completion_requires_the_exact_accepted_ticket() {
    use std::collections::BTreeMap;
    use voteboat::{identity::*, log::*, quorum::*};

    let identity = StoreIdentity {
        id: StoreId::new(1).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    };
    let binding = StoreBinding {
        identity,
        session: StoreSession::new(1).unwrap(),
    };
    let node = NodeId::new(1).unwrap();
    let mut bootstrap = Bootstrap {
        group: GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
        voter_stores: BTreeMap::from([(node, identity)]),
    };
    let mut store =
        store::MemoryStore::new(binding, LogLimits::default(), bootstrap.clone()).unwrap();
    bootstrap.group.id = GroupId::new(2).unwrap();
    let group = bootstrap.group;
    let tickets = store
        .append_batch(vec![LogMutation::Create(bootstrap)])
        .unwrap();
    assert!(store.state(group).is_err());
    let mut foreign = tickets.clone();
    foreign[0].batch += 1;
    assert!(store.barrier(&foreign).is_err());
    assert_eq!(store.barrier(&tickets).unwrap().tickets, tickets);
    assert!(store.state(group).is_ok());
    assert!(store.barrier(&tickets).is_err());
}
#[path = "tests/owned_store.rs"]
mod owned_store;
