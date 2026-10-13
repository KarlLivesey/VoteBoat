// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

fn configuration(shard: &Shard<Ready>) -> Event {
    use crate::membership::{Configuration, ConfigurationChange, ConfigurationRecord};
    let state = shard.core(group()).unwrap().state();
    let next = Configuration::new(
        ConfigurationId::new(2).unwrap(),
        state.bootstrap.policy.clone(),
        state.bootstrap.voter_stores.clone(),
        BTreeMap::new(),
    )
    .unwrap();
    Event::Configure(Box::new(ConfigurationProposal {
        record: ConfigurationRecord {
            operation: OperationId::new(1000).unwrap(),
            expected: state.bootstrap.configuration,
            change: ConfigurationChange::Learners(next),
        },
        readiness: Vec::new(),
        requirements: crate::application::Counter::new(4)
            .unwrap()
            .readiness_requirements(),
    }))
}

#[test]
fn protected_event_behind_routine_head_or_background_still_prevents_coalescing() {
    for boundary in 0..7 {
        let mut shard = shard(ShardLimits::default(), LogLimits::default());
        let term = shard.core(group()).unwrap().state().hard_state.term;
        shard.admit(group(), Event::Heartbeat).unwrap();
        let protected = match boundary {
            0 => reply(&shard, term + 1),
            1 => reply(&shard, term - 1),
            2 => Event::Campaign,
            3 => Event::SetCampaigning { enabled: false },
            4 => Event::Checkpoint,
            5 => configuration(&shard),
            _ => {
                let Event::Receive(mut message) = reply(&shard, term) else {
                    unreachable!()
                };
                message.rpc = Rpc::Voted { granted: true };
                Event::Receive(message)
            }
        };
        shard.admit(group(), protected.clone()).unwrap();
        for id in 1..=3 {
            shard.admit_tracked(group(), command(id)).unwrap();
        }
        let visit = data_turn(&mut shard);
        let step = shard
            .step_ready_checked(visit, MonoTime(0), 3, |_, _, _| Ok(()))
            .unwrap()
            .unwrap();
        assert!(step.batch_members.is_empty());
        assert_eq!(step.operation, Some(OperationId::new(1).unwrap()));
        assert!(shard.groups[&group()]
            .queues
            .iter()
            .any(|queue| queue.iter().any(|q| q.event == protected)));
        assert_eq!(shard.groups[&group()].queues[1].len(), 2);
    }
}

#[test]
fn later_original_refusal_remains_queued_with_routine_control_untouched() {
    let mut shard = shard(ShardLimits::default(), LogLimits::default());
    shard.admit(group(), Event::Heartbeat).unwrap();
    for id in 1..=4 {
        shard.admit_tracked(group(), command(id)).unwrap();
    }
    let visit = data_turn(&mut shard);
    let mut calls = Vec::new();
    let step = shard
        .step_ready_checked(visit, MonoTime(0), 4, |_, event, prefix| {
            let Event::Propose { operation, .. } = event else {
                unreachable!()
            };
            calls.push((operation.get(), prefix.len()));
            if operation.get() == 3 {
                Err(RaftError::Busy)
            } else {
                Ok(())
            }
        })
        .unwrap()
        .unwrap();
    assert_eq!(calls, vec![(1, 0), (2, 1), (3, 2)]);
    assert_eq!(step.batch_members.len(), 1);
    assert_eq!(shard.groups[&group()].queues[0].len(), 1);
    let remaining = &shard.groups[&group()].queues[1];
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining.front().unwrap().event, command(3));
}
