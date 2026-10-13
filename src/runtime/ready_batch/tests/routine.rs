// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
mod boundaries;
mod budgets;
mod outcomes;

fn reply(shard: &Shard<Ready>, term: u64) -> Event {
    let core = shard.core(group()).unwrap();
    Event::Receive(Message {
        group: group(),
        configuration: core.membership().id(),
        from: NodeId::new(2).unwrap(),
        sender: StoreBinding {
            identity: StoreIdentity {
                id: StoreId::new(2).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            session: StoreSession::new(1).unwrap(),
        },
        to: NodeId::new(1).unwrap(),
        term,
        context: RequestContext {
            origin: core.storage_binding(),
            sequence: 1,
        },
        // Untrusted evidence must remain queued, not count towards commitment.
        rpc: Rpc::Appended {
            success: true,
            matching_index: u64::MAX,
        },
    })
}

fn data_turn(shard: &mut Shard<Ready>) -> VisitTicket {
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    shard.groups.get_mut(&group()).unwrap().turn = 2;
    visit
}

#[test]
fn selected_data_batch_defers_same_term_reply_without_consuming_or_trusting_it() {
    let mut shard = shard(ShardLimits::default(), LogLimits::default());
    let term = shard.core(group()).unwrap().state().hard_state.term;
    let ack = reply(&shard, term);
    shard.admit(group(), ack.clone()).unwrap();
    for id in 1..=4 {
        shard.admit_tracked(group(), command(id)).unwrap();
    }
    let before = shard.core(group()).unwrap().state().clone();
    let visit = data_turn(&mut shard);
    let step = shard
        .step_ready_checked(visit, MonoTime(0), 4, |_, _, _| Ok(()))
        .unwrap()
        .unwrap();
    assert_eq!(step.batch_members.len(), 3);
    assert_eq!(step.received_from, None);
    assert_eq!(shard.core(group()).unwrap().state(), &before);
    assert_eq!(shard.groups[&group()].queues[0].front().unwrap().event, ack);
    assert_eq!(
        shard.groups[&group()].visit.as_ref().unwrap().used[0].items,
        0
    );
    assert_eq!(shard.finish(visit), Err(RuntimeError::DependencyPending));
    shard
        .with_core(visit, |core| finish(core, step.result.unwrap()))
        .unwrap();
    shard.finish(visit).unwrap();
    let durable = shard.core(group()).unwrap().state().clone();
    let next = shard.poll(MonoTime(0)).unwrap().unwrap();
    let processed = shard.step_next(next, MonoTime(0)).unwrap().unwrap();
    assert_eq!(processed.received_from, Some(NodeId::new(2).unwrap()));
    assert_eq!(processed.result, Err(RaftError::WrongIdentity));
    assert_eq!(shard.core(group()).unwrap().state(), &durable);
    assert!(processed.batch_members.is_empty());
    assert!(shard.groups[&group()].queues[0].is_empty());
}

#[test]
fn two_control_turns_then_bounded_data_quantum_preserve_cursor_and_next_control() {
    let limits = ShardLimits {
        visit_items: 4,
        ..ShardLimits::default()
    };
    let mut shard = shard(limits, LogLimits::default());
    for _ in 0..3 {
        shard.admit(group(), Event::Heartbeat).unwrap();
    }
    for id in 1..=6 {
        shard.admit_tracked(group(), command(id)).unwrap();
    }
    let visit = shard.poll(MonoTime(0)).unwrap().unwrap();
    for expected_turn in [1, 2] {
        let step = shard
            .step_ready_checked(visit, MonoTime(0), 6, |_, _, _| Ok(()))
            .unwrap()
            .unwrap();
        assert!(step.operation.is_none());
        assert!(step.result.unwrap().is_empty());
        assert_eq!(shard.groups[&group()].turn, expected_turn);
    }
    let step = shard
        .step_ready_checked(visit, MonoTime(0), 6, |_, _, _| Ok(()))
        .unwrap()
        .unwrap();
    assert_eq!(step.batch_members.len(), 1);
    let state = &shard.groups[&group()];
    assert_eq!(state.turn, 3);
    assert_eq!(state.queues[0].len(), 1);
    assert_eq!(state.visit.as_ref().unwrap().used[0].items, 2);
    assert_eq!(state.visit.as_ref().unwrap().used[1].items, 2);
    shard
        .with_core(visit, |core| finish(core, step.result.unwrap()))
        .unwrap();
    shard.finish(visit).unwrap();
    let next = shard.poll(MonoTime(0)).unwrap().unwrap();
    let control = shard.step_next(next, MonoTime(0)).unwrap().unwrap();
    assert!(control.operation.is_none());
    assert!(control.result.unwrap().is_empty());
    assert_eq!(shard.groups[&group()].turn, 1);
    assert!(shard.groups[&group()].queues[0].is_empty());
    assert_eq!(shard.groups[&group()].queues[1].len(), 4);
}
