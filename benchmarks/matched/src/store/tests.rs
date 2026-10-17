// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::attribution::Phase;
use std::collections::VecDeque;
use voteboat::{
    quorum::{Limits, Policy, Tree},
    raft::*,
};

#[test]
fn volatile_store_round_trip_keeps_fifo_and_attribution() {
    let node = NodeId::new(1).unwrap();
    let binding = StoreBinding {
        identity: StoreIdentity {
            id: StoreId::new(1).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
        session: StoreSession::new(1).unwrap(),
    };
    let bootstrap = Bootstrap {
        group: GroupIdentity {
            id: GroupId::new(1).unwrap(),
            incarnation: GroupIncarnation::new(1).unwrap(),
        },
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node), Limits::default()).unwrap(),
        voter_stores: [(node, binding.identity)].into(),
    };
    let store = VolatileStore::<true>::new(binding, LogLimits::default()).unwrap();
    let state = store.log.bootstrap(&bootstrap).unwrap();
    let mut core = Raft::recover_volatile(node, &store.log, state).unwrap();
    // Drive every token through the same FIFO rule as the actor: borrowed
    // completion only when nothing is queued, otherwise owned and appended.
    let mut queue = VecDeque::from(core.step(Event::Campaign).unwrap());
    let mut committed = Vec::new();
    let mut tokens = 0;
    let mut proposed = false;
    loop {
        let Some(effect) = queue.pop_front() else {
            if proposed {
                break;
            }
            proposed = true;
            queue.extend(
                core.propose_batch(
                    (1..=3)
                        .map(|i| Proposal {
                            operation: OperationId::new(i).unwrap(),
                            bytes: vec![i as u8],
                        })
                        .collect(),
                )
                .unwrap(),
            );
            continue;
        };
        match effect {
            Effect::PersistVolatile(persist) => {
                tokens += 1;
                let result = persist_volatile(&mut core, &store.log, persist).unwrap();
                if queue.is_empty() {
                    let (entries, following) = result.into_parts();
                    committed.extend(entries.into_iter().flatten().map(|e| e.index));
                    queue.extend(following);
                } else {
                    queue.extend(result.into_owned());
                }
            }
            Effect::Committed(entries) => committed.extend(entries.iter().map(|e| e.index)),
            other => panic!("single voter effect {other:?}"),
        }
    }
    assert_eq!(committed, [1, 2, 3, 4]);
    assert_eq!(core.state().commit_index, 4);
    // Campaign, its leader no-op and commit, then the batch and its commit.
    assert!(tokens >= 4);
    let report = serde_json::to_value(
        store
            .attribution
            .report(
                0,
                &store.attribution,
                &crate::attribution::Recorder::default(),
            )
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let phases = report["phases"].as_array().unwrap();
    assert_eq!(phases.len(), 29);
    for phase in [
        Phase::StoreState,
        Phase::StoreMatchesState,
        Phase::StoreCheckpointState,
        Phase::StoreAppend,
        Phase::StoreBarrier,
    ] {
        assert_eq!(phases[phase as usize]["calls"], 0);
    }
    assert_eq!(
        report["checkpoint_compact_gaps"].as_array().unwrap().len(),
        3
    );
}
