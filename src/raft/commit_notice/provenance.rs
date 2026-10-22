// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::raft::request_window::OriginalScope;

#[test]
fn exact_emission_is_one_way_and_forged_sequence_is_retired() {
    let mut c = Cluster::new(true, true);
    c.propose(1);
    let held = c.notices();
    for m in &held {
        assert!(matches!(
            c.cores[0].request_window(m),
            OriginalScope::OneWay
        ));
        let mut forged = m.clone();
        forged.context.sequence -= 1;
        assert!(matches!(
            c.cores[0].request_window(&forged),
            OriginalScope::Retired
        ));
        let mut ack = m.clone();
        ack.from = m.to;
        ack.to = m.from;
        ack.sender = c.cores[m.to.get() as usize - 1].binding;
        ack.rpc = Rpc::Appended {
            success: true,
            matching_index: u64::MAX,
        };
        let progress = c.cores[0].progress.clone();
        c.deliver(ack);
        assert_eq!(c.cores[0].progress, progress);
        assert!(c.cores[0].requests.is_empty());
    }
    let m = &held[0];
    let reservation = c.cores[0].effect_reservation(0).unwrap();
    assert!(reservation > c.cores[0].commit_notice_emissions.retained_bytes().unwrap());
    c.cores[0].storage_failed();
    assert!(c.cores[0].commit_notice_emissions.is_empty());
    assert!(matches!(
        c.cores[0].request_window(m),
        OriginalScope::Retired
    ));
}

#[test]
fn stamps_are_exact_bounded_superseded_and_survive_ordinary_request_retirement() {
    let mut c = Cluster::new(true, true);
    c.propose(1);
    let old = c.notices();
    let allocation = c.cores[0].commit_notice_emissions.retained_bytes().unwrap();
    for m in &old {
        for case in 0..5 {
            let mut forged = m.clone();
            match case {
                0 => forged.context.sequence += 1,
                1 => forged.to = if m.to == node(2) { node(3) } else { node(2) },
                2 => {
                    forged.rpc = Rpc::CommitNotice {
                        index: 2,
                        log_term: m.term,
                    }
                }
                3 => {
                    forged.rpc = Rpc::CommitNotice {
                        index: 3,
                        log_term: 1,
                    }
                }
                _ => forged.sender.session = StoreSession::new(2).unwrap(),
            }
            assert!(matches!(
                c.cores[0].request_window(&forged),
                OriginalScope::Retired
            ));
        }
    }
    for m in &old {
        c.deliver(m.clone());
    }
    c.act(0, Event::Heartbeat);
    c.drain();
    assert_eq!(c.cores[0].commit_notice_emissions.len(), 2);
    for op in 2..=1000 {
        c.propose(op);
        c.drain();
        assert_eq!(c.cores[0].commit_notice_emissions.keys().count(), 2);
        assert_eq!(
            c.cores[0].commit_notice_emissions.retained_bytes(),
            Some(allocation)
        );
    }
    for m in old {
        assert!(matches!(
            c.cores[0].request_window(&m),
            OriginalScope::Retired
        ));
    }
    c.assert_converged(1000);
}

#[test]
fn transfer_and_higher_term_invalidate_captured_emission_before_release() {
    for transfer in [true, false] {
        let mut c = Cluster::new(true, true);
        c.propose(1);
        let held = c.notices();
        if transfer {
            let request = LeadershipTransferRequest {
                operation: OperationId::new(99).unwrap(),
                target: PeerIdentity {
                    node: node(2),
                    store: c.cores[1].binding.identity,
                },
                configuration: c.cores[0].membership().id(),
            };
            let out = c.cores[0].step(Event::TransferLeadership(request)).unwrap();
            assert!(out.iter().any(|e| matches!(
                e,
                Effect::Send(Message {
                    rpc: Rpc::Append { .. },
                    ..
                })
            )));
        } else {
            let mut input = held[0].clone();
            input.from = node(2);
            input.to = node(1);
            input.sender = c.cores[1].binding;
            input.context.origin = input.sender;
            input.term += 1;
            c.cores[0].step(Event::Receive(input)).unwrap();
        }
        assert!(c.cores[0].commit_notice_emissions.is_empty());
        for m in held {
            assert!(matches!(
                c.cores[0].request_window(&m),
                OriginalScope::Retired
            ));
        }
    }
}

#[test]
fn notice_context_is_neither_read_evidence_nor_a_valid_new_request_reply() {
    let mut c = Cluster::new(true, true);
    c.propose(1);
    let notice = c.notices().remove(0);
    c.act(
        0,
        Event::Read {
            request: ReadRequestId::new(1).unwrap(),
        },
    );
    let mut fake = notice.clone();
    fake.from = notice.to;
    fake.to = notice.from;
    fake.sender = c.cores[1].binding;
    fake.rpc = Rpc::ReadAck;
    c.deliver(fake);
    assert!(c.cores[0].ready_read.is_none());
    assert!(c.cores[0].read.is_some());
    c.queue.clear();
    c.act(0, Event::Heartbeat);
    let request = c
        .queue
        .iter()
        .find(|m| matches!(m.rpc, Rpc::Append { .. }))
        .unwrap()
        .clone();
    let OriginalScope::Request(window) = c.cores[0].request_window(&request) else {
        panic!()
    };
    let mut fake = notice.clone();
    fake.from = notice.to;
    fake.to = notice.from;
    fake.sender = c.cores[1].binding;
    fake.rpc = Rpc::Appended {
        success: true,
        matching_index: 3,
    };
    assert!(!window.accepts_reply(
        &c.cores[0],
        crate::secure::LocalIdentity {
            node: fake.from,
            store: fake.sender
        },
        &fake
    ));
    let actual = c.cores[0].requests[&request.to];
    c.deliver(fake);
    assert_eq!(c.cores[0].requests[&request.to], actual);
}

#[test]
fn snapshot_staging_invalidates_captured_notice() {
    let mut c = Cluster::new(true, true);
    c.propose(1);
    let held = c.notices();
    let mut incoming = held[0].clone();
    incoming.from = node(2);
    incoming.to = node(1);
    incoming.sender = c.cores[1].binding;
    incoming.context.origin = incoming.sender;
    incoming.rpc = Rpc::Snapshot {
        snapshot: Box::new(Snapshot {
            metadata: crate::snapshot::SnapshotMetadata {
                bootstrap: c.cores[0].state().bootstrap.clone(),
                membership: None,
                index: 4,
                term: incoming.term,
                application_schema: 1,
            },
            application: vec![1],
        }),
    };
    let out = c.cores[0].step(Event::Receive(incoming)).unwrap();
    assert!(matches!(out.as_slice(), [Effect::StageSnapshot(_)]));
    assert!(c.cores[0].commit_notice_emissions.is_empty());
    assert!(matches!(
        c.cores[0].request_window(&held[0]),
        OriginalScope::Retired
    ));
}

#[test]
fn accepted_configuration_invalidates_stamps_before_install_and_all_sends_fall_back() {
    use crate::raft::membership_tests::{accept, configuration, durable};
    let mut c = Cluster::new(true, false);
    c.propose(1);
    let held = c.notices();
    let effects = accept(
        &mut c.cores[0],
        200,
        crate::membership::ConfigurationChange::Learners(configuration(2, &[1, 2, 3], &[4])),
    );
    for m in &held {
        assert!(matches!(
            c.cores[0].request_window(m),
            OriginalScope::Retired
        ));
    }
    let out = durable(&mut c.cores[0], effects);
    assert!(c.cores[0].commit_notice_emissions.is_empty());
    assert!(out.iter().all(|e| !matches!(
        e,
        Effect::Send(Message {
            rpc: Rpc::CommitNotice { .. },
            ..
        })
    )));
}
