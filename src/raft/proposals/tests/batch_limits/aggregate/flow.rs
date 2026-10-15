// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::application::{Counter, CounterOutcome, StateMachine};
fn message(effects: &[Effect], peer: u64) -> Message {
    effects
        .iter()
        .find_map(|effect| match effect {
            Effect::Send(message) if message.to == node(peer) => Some(message.clone()),
            _ => None,
        })
        .unwrap()
}
fn ack(core: &Raft, request: &Message) -> Message {
    let Rpc::Append {
        previous_index,
        entries,
        ..
    } = &request.rpc
    else {
        panic!("append required")
    };
    Message {
        group: request.group,
        configuration: request.configuration,
        from: request.to,
        to: request.from,
        sender: StoreBinding {
            identity: core.membership().replica_store(request.to).unwrap(),
            session: StoreSession::new(1).unwrap(),
        },
        term: request.term,
        context: request.context,
        rpc: Rpc::Appended {
            success: true,
            matching_index: entries.last().map_or(*previous_index, |entry| entry.index),
        },
    }
}
fn complete(core: &mut Raft, effects: Vec<Effect>) -> Vec<Effect> {
    assert!(matches!(effects.as_slice(), [Effect::Persist(_)]));
    let exact = ticket(core);
    core.admitted(exact).unwrap();
    core.complete(&DurableLog {
        tickets: vec![exact],
    })
    .unwrap()
}
fn apply_chunk(core: &mut Raft, request: Message, app: &mut Counter, end: u64) -> Message {
    let Rpc::Append { entries, .. } = &request.rpc else {
        panic!("append required")
    };
    assert_eq!(entries.len(), 1024);
    assert_eq!(entries.last().unwrap().index, end);
    let response = ack(core, &request);
    let effects = core.step(Event::Receive(response.clone())).unwrap();
    assert_eq!(core.state().commit_index, end - 1024);
    let effects = complete(core, effects);
    assert!(!effects
        .iter()
        .any(|effect| matches!(effect,Effect::Send(message) if message.to==node(3))));
    let committed: Vec<_> = effects
        .iter()
        .flat_map(|effect| match effect {
            Effect::Committed(entries) => entries.clone(),
            _ => Vec::new(),
        })
        .collect();
    let receipts = app.apply_batch(&committed).unwrap();
    assert_eq!(receipts.len(), 1024);
    for receipt in receipts {
        assert_eq!(receipt.operation.get(), u128::from(receipt.index - 1));
        assert_eq!(
            receipt.outcome,
            CounterOutcome::Value((receipt.index - 1) as i64)
        );
        assert!(!receipt.duplicate);
    }
    assert!(core.step(Event::Receive(response)).unwrap().is_empty());
    message(&effects, 2)
}
#[test]
fn aggregate_partial1024_quorum_prefixes_keep_all_results_delayed_peer_and_finite_notifications() {
    let mut cores = aggregate_replicas();
    let mut core = cores.remove(&node(1)).unwrap();
    let mut app = Counter::new(16384).unwrap();
    app.apply_batch(&core.replay_committed()).unwrap();
    let effects = core
        .propose_batch(
            (1..=16384)
                .map(|id| Proposal {
                    operation: OperationId::new(id).unwrap(),
                    bytes: 1i64.to_le_bytes().to_vec(),
                })
                .collect(),
        )
        .unwrap();
    let effects = complete(&mut core, effects);
    let delayed = message(&effects, 3);
    let mut request = message(&effects, 2);
    let first_retry = message(&core.step(Event::Heartbeat).unwrap(), 3);
    assert_eq!(first_retry, delayed);
    for chunk in 1..=16 {
        request = apply_chunk(&mut core, request, &mut app, chunk * 1024 + 1);
        assert_eq!(core.requests[&node(3)].context, delayed.context);
    }
    let Rpc::Append {
        previous_index,
        entries,
        leader_commit,
        ..
    } = &request.rpc
    else {
        panic!()
    };
    assert_eq!(*previous_index, 16385);
    assert!(entries.is_empty());
    assert_eq!(*leader_commit, 16385);
    assert!(core
        .step(Event::Receive(ack(&core, &request)))
        .unwrap()
        .is_empty());
    let heartbeats = core.step(Event::Heartbeat).unwrap();
    let idle_peer_probe = message(&heartbeats, 2);
    let retry = message(&heartbeats, 3);
    assert_eq!(retry.context, delayed.context);
    let Rpc::Append {
        entries,
        leader_commit,
        ..
    } = &retry.rpc
    else {
        panic!()
    };
    assert_eq!(entries.len(), 1024);
    assert_eq!(*leader_commit, 16385);
    request = delayed;
    for chunk in 1..=16 {
        let Rpc::Append { entries, .. } = &request.rpc else {
            panic!()
        };
        assert_eq!(entries.last().unwrap().index, chunk * 1024 + 1);
        let effects = core.step(Event::Receive(ack(&core, &request))).unwrap();
        assert!(!effects
            .iter()
            .any(|effect| matches!(effect, Effect::Committed(_) | Effect::Persist(_))));
        if chunk == 16 {
            assert!(effects.is_empty());
        } else {
            request = message(&effects, 3);
        }
    }
    // The explicit heartbeat also starts a normal fresh empty probe for idle peer2.
    // Complete that exact probe; it must not create another owed notification.
    assert!(core
        .step(Event::Receive(ack(&core, &idle_peer_probe)))
        .unwrap()
        .is_empty());
    assert!(core.requests.is_empty());
    assert_eq!(app.read_applied(16385), Ok(16384));
}
