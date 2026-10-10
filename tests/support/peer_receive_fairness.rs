// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{
    log::{EntryPayload, LogEntry},
    snapshot::{Snapshot, SnapshotMetadata},
};

fn limits() -> IngressLimits {
    IngressLimits {
        batches: 3,
        messages: 5,
        control_batches: 1,
        control_messages: 2,
        background_batches: 1,
        background_messages: 2,
        batch_messages: 2,
        ..IngressLimits::default()
    }
}
fn frame(f: &Fixture, peer: u64, class: MessageClass) -> ReceivedBatch {
    let connection = f.transports.borrow()[&node(peer)].borrow().binding;
    let messages = (0..2)
        .map(|offset| {
            let mut message = message(peer, 1);
            message.context.sequence = offset + 1;
            message.rpc = match class {
                MessageClass::Data => Rpc::Append {
                    previous_index: 0,
                    previous_term: 0,
                    leader_commit: 0,
                    entries: vec![LogEntry {
                        index: 1,
                        term: 1,
                        payload: EntryPayload::Command {
                            operation: OperationId::new(u128::from(peer * 10 + offset)).unwrap(),
                            bytes: 1i64.to_le_bytes().to_vec(),
                        },
                    }],
                },
                MessageClass::Background => Rpc::Snapshot {
                    snapshot: Box::new(Snapshot {
                        metadata: SnapshotMetadata {
                            membership: None,
                            bootstrap: support::bootstrap(1, 3),
                            index: 1,
                            term: 1,
                            application_schema: 1,
                        },
                        application: vec![],
                    }),
                },
                MessageClass::Control => Rpc::ReadProbe,
            };
            message
        })
        .collect();
    let batch = ReceivedBatch {
        connection,
        messages,
    };
    assert_eq!(batch.info(limits().batch_bytes).unwrap().class, class);
    batch
}
fn compete(class: MessageClass) {
    let mut f = Fixture::with_ingress_limits(2, limits());
    f.attach();
    let mut received = [0; 2];
    for _ in 0..8 {
        for peer in 2..=3 {
            let control = f.transports.borrow()[&node(peer)].clone();
            if control.borrow().incoming.is_none() {
                control.borrow_mut().incoming = Some(frame(&f, peer, class));
            }
        }
        let idle = f
            .driver
            .as_mut()
            .unwrap()
            .poll(
                &mut f.owner,
                &mut f.outbound,
                MonoTime(0),
                PeerDriverBudget {
                    peer_visits: 0,
                    ingress: 0,
                    ..PeerDriverBudget::default()
                },
            )
            .unwrap();
        assert_eq!(idle.received, 0);
        let progress = f.poll(0).unwrap();
        assert_eq!(progress.received, 1);
        assert_eq!(progress.ingress_blocked, 1);
        assert_eq!(progress.ingress.admitted, 2);
        assert_eq!(progress.ingress.completed.len(), 1);
        let completion = progress.ingress.completed[0];
        assert_eq!(completion.admitted, 2);
        assert_eq!(completion.end, IngressEnd::Drained);
        received[(completion.connection.peer.node.get() - 2) as usize] += 1;
        assert_eq!(
            f.driver.as_ref().unwrap().ingress().usage(),
            IngressUsage::default()
        );
    }
    assert_eq!(
        received,
        [4, 4],
        "one ready peer monopolized receive credit"
    );
    assert_eq!(f.owner.core(group(1)).unwrap().state().commit_index, 0);
    f.close(0);
}
#[test]
fn saturated_background_receive_credit_rotates_between_ready_peers() {
    compete(MessageClass::Background);
}
#[test]
fn saturated_data_message_credit_rotates_between_ready_peers() {
    compete(MessageClass::Data);
}
fn inject(f: &Fixture, peer: u64, class: MessageClass) {
    let input = frame(f, peer, class);
    let control = f.transports.borrow()[&node(peer)].clone();
    assert!(control.borrow_mut().incoming.replace(input).is_none());
}
fn hold_dispatch(f: &mut Fixture) -> PeerDriverProgress {
    f.driver
        .as_mut()
        .unwrap()
        .poll(
            &mut f.owner,
            &mut f.outbound,
            MonoTime(0),
            PeerDriverBudget {
                ingress: 0,
                ..PeerDriverBudget::default()
            },
        )
        .unwrap()
}
#[test]
fn held_recovery_receive_preserves_control_reserve_and_outbound_completion() {
    let mut f = Fixture::with_ingress_limits(2, limits());
    f.attach();
    inject(&f, 2, MessageClass::Background);
    assert_eq!(hold_dispatch(&mut f).received, 1);
    let initial = f.driver.as_ref().unwrap().ingress().usage();
    assert_eq!(initial.batches, 1);
    assert_eq!(initial.messages, 2);
    inject(&f, 2, MessageClass::Background);
    inject(&f, 3, MessageClass::Control);
    let sent = f.enqueue(3);
    let progress = hold_dispatch(&mut f);
    assert_eq!(progress.received, 1);
    assert_eq!(progress.ingress_blocked, 1);
    assert_eq!(progress.sends, 1);
    let held = f.driver.as_ref().unwrap().ingress().usage();
    assert_eq!(held.batches, 2);
    assert_eq!(held.messages, 4);
    assert!(held.bytes <= limits().bytes);
    let progress = f.poll(0).unwrap();
    assert!(progress
        .completions
        .iter()
        .any(|c| { c.ticket == sent && c.result == LocalSendResult::Sent }));
    assert_eq!(progress.ingress.admitted, 4);
    assert_eq!(progress.ingress.completed.len(), 2);
    assert!(progress
        .ingress
        .completed
        .iter()
        .any(|c| { c.connection.peer.node == node(3) && c.admitted == 2 }));
    assert_eq!(
        f.driver.as_ref().unwrap().ingress().usage(),
        IngressUsage::default()
    );
    assert_eq!(f.poll(0).unwrap().ingress.admitted, 2);
    assert_eq!(f.owner.core(group(1)).unwrap().state().commit_index, 0);
    f.close(0);
}
