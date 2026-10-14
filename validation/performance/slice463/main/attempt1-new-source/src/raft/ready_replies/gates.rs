// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Boundary refusals retain exact original receive scopes and canonical state.
use super::*;

fn rejected(core: &mut Raft, replies: &[Message], proposals: Vec<Proposal>) {
    let before = core.state().clone();
    let progress = core.progress.clone();
    let next = core.next_index.clone();
    let requests: Vec<_> = core
        .requests
        .iter()
        .map(|(peer, sent)| {
            (
                *peer,
                sent.context,
                sent.configuration,
                sent.start,
                sent.end,
                sent.commit_at_start,
            )
        })
        .collect();
    let cursors = (
        core.request_sequence,
        core.last_batch,
        core.batch_limits_locked,
        core.role,
    );
    assert!(core
        .propose_after_ready_replies(replies, proposals)
        .is_err());
    assert_eq!(core.state(), &before);
    assert_eq!(core.progress, progress);
    assert_eq!(core.next_index, next);
    assert_eq!(
        core.requests
            .iter()
            .map(|(peer, sent)| {
                (
                    *peer,
                    sent.context,
                    sent.configuration,
                    sent.start,
                    sent.end,
                    sent.commit_at_start,
                )
            })
            .collect::<Vec<_>>(),
        requests
    );
    assert_eq!(
        (
            core.request_sequence,
            core.last_batch,
            core.batch_limits_locked,
            core.role
        ),
        cursors
    );
    assert!(core.pending.is_none());
}

#[test]
fn complete_reply_scope_and_success_are_required_before_any_mutation() {
    for variant in 0..12 {
        let (mut core, mut replies) = ready(0);
        core.batch_limits_locked = false;
        let message = &mut replies[1];
        match variant {
            0 => message.to = node(2),
            1 => message.sender.identity.id = StoreId::new(99).unwrap(),
            2 => message.group.incarnation = GroupIncarnation::new(2).unwrap(),
            3 => message.configuration = ConfigurationId::new(2).unwrap(),
            4 => message.context.origin.session = StoreSession::new(2).unwrap(),
            5 => message.context.sequence += 1,
            6 => message.term += 1,
            7 => message.term = 0,
            8 => {
                message.rpc = Rpc::Appended {
                    success: false,
                    matching_index: 3,
                }
            }
            9 => {
                message.rpc = Rpc::Appended {
                    success: true,
                    matching_index: 2,
                }
            }
            10 => replies[1] = replies[0].clone(),
            _ => message.rpc = Rpc::ReadAck,
        }
        rejected(&mut core, &replies, vec![proposal(12)]);
        assert!(!core.batch_limits_locked);
    }
}

#[test]
fn count_quorum_legacy_counters_and_proposal_budgets_refuse_without_pending() {
    for variant in 0..10 {
        let (mut core, mut replies) = ready(if variant == 3 { 1 } else { 0 });
        let mut proposals = vec![proposal(12)];
        match variant {
            0 => core.batch_limits.max_ready_reply_inputs = 0,
            1 => replies.clear(),
            2 => core.batch_limits.max_ready_reply_inputs = 1,
            3 => {
                // Five voters: peer5 alone plus self is not enough. Removing
                // peer2's known match makes first selected reply insufficient.
                core.progress.insert(node(2), 0);
            }
            4 => {
                core.durable.revision = LogRevision::new(u64::MAX - 1).unwrap();
                core.log_summary = LogSummary::from_validated(core.state());
            }
            5 => core.request_sequence = u64::MAX - 1,
            6 => proposals.clear(),
            7 => proposals[0]
                .bytes
                .resize(core.limits.max_command_bytes + 1, 0),
            8 => core.limits.max_entries_per_group = core.state().entries.len(),
            _ => core.limits.max_batch_bytes = 1,
        }
        rejected(&mut core, &replies, proposals);
    }
    let (mut core, replies) = ready(0);
    core.batch_limits.max_ready_reply_inputs = 1;
    assert!(core
        .propose_after_ready_replies(&replies[..1], vec![proposal(12)])
        .is_ok());
}

#[test]
fn ready_bound_is_startup_only_zero_disables_and_sixty_four_is_valid() {
    for max_ready_reply_inputs in [0, 2, 64] {
        let mut core = core();
        let limits = BatchLimits {
            max_ready_reply_inputs,
            ..BatchLimits::default()
        };
        core.configure_batch_limits(limits).unwrap();
        assert_eq!(core.batch_limits(), limits);
        core.step(Event::Heartbeat).unwrap();
        assert_eq!(
            core.configure_batch_limits(BatchLimits::default()),
            Err(RaftError::Busy)
        );
    }
    let mut core = core();
    assert!(core
        .configure_batch_limits(BatchLimits {
            max_ready_reply_inputs: 65,
            ..BatchLimits::default()
        })
        .is_err());
    assert_eq!(core.batch_limits().max_ready_reply_inputs, 2);
}

#[test]
fn altered_persistence_and_wrong_ticket_cannot_gain_joined_authority() {
    let (mut core, replies) = ready(0);
    let effects = core
        .propose_after_ready_replies(&replies, vec![proposal(12)])
        .unwrap();
    let exact = update(effects);
    for variant in 0..4 {
        let mut altered = exact.clone();
        match variant {
            0 => altered.commit_index -= 1,
            1 => altered.hard_state.term += 1,
            2 => altered.suffix.as_mut().unwrap().entries[0].term += 1,
            _ => {
                let EntryPayload::Command { bytes, .. } =
                    &mut altered.suffix.as_mut().unwrap().entries[0].payload
                else {
                    panic!()
                };
                *bytes = vec![7; 8].into();
            }
        }
        assert_eq!(
            core.validate_persist_effect(&altered),
            Err(RaftError::WrongCompletion)
        );
        core.validate_persist_effect(&exact).unwrap();
    }
    let mut wrong = ticket(&core);
    wrong.last_index -= 1;
    assert_eq!(
        core.replicate_admitted(wrong),
        Err(RaftError::WrongCompletion)
    );
    assert!(core.pending.as_ref().unwrap().ticket.is_none());
    core.storage_failed();
    assert_eq!(
        core.propose_after_ready_replies(&replies, vec![proposal(13)]),
        Err(RaftError::Fenced)
    );
}

#[test]
fn actual_empty_collector_aliases_remain_distinct_from_primary_tip_replies() {
    let mut core = core();
    for peer in [node(2), node(3)] {
        core.progress.insert(peer, 1);
    }
    let collectors = sends(core.step(Event::Heartbeat).unwrap());
    core.propose_batch(vec![proposal(10), proposal(11)])
        .unwrap();
    let exact = ticket(&core);
    let early = sends(core.replicate_admitted(exact).unwrap());
    core.complete(&DurableLog {
        tickets: vec![exact],
    })
    .unwrap();
    let alias = ack(&core, &collectors[0]);
    let primaries: Vec<_> = early.iter().map(|m| ack(&core, m)).collect();
    assert!(core.is_empty_alias(&alias));
    rejected(
        &mut core,
        &[alias.clone(), primaries[1].clone()],
        vec![proposal(12)],
    );
    assert!(core.is_empty_alias(&alias));
    assert_eq!(
        core.requests[&primaries[0].from].context,
        primaries[0].context
    );
    for collector in &collectors {
        let response = ack(&core, collector);
        assert!(core.step(Event::Receive(response)).unwrap().is_empty());
    }
    assert!(core
        .propose_after_ready_replies(&primaries, vec![proposal(12)])
        .is_ok());
}

#[test]
fn read_transfer_checkpoint_and_existing_pending_dependencies_are_not_crossed() {
    for variant in 0..5 {
        let (mut core, replies) = ready(0);
        match variant {
            0 | 1 => {
                core.step(Event::Read {
                    request: ReadRequestId::new(1).unwrap(),
                })
                .unwrap();
                if variant == 1 {
                    core.ready_read = Some(core.read.take().unwrap().barrier);
                }
            }
            2 => {
                core.step(Event::TransferLeadership(LeadershipTransferRequest {
                    operation: OperationId::new(99).unwrap(),
                    target: PeerIdentity {
                        node: node(2),
                        store: core.membership().voter_store(node(2)).unwrap(),
                    },
                    configuration: core.membership().id(),
                }))
                .unwrap();
            }
            3 => {
                core.step(Event::Checkpoint).unwrap();
            }
            _ => core.role = Role::Follower,
        }
        let reset = core.election_reset;
        let requested = core.checkpoint_requested;
        let transfer = core.transfer;
        let barrier = core.read.as_ref().map(|r| r.barrier);
        let ready = core.ready_read;
        rejected(&mut core, &replies, vec![proposal(12)]);
        assert_eq!(core.election_reset, reset);
        assert_eq!(core.checkpoint_requested, requested);
        assert_eq!(core.transfer, transfer);
        assert_eq!(core.read.as_ref().map(|r| r.barrier), barrier);
        assert_eq!(core.ready_read, ready);
    }
    let (mut core, replies) = ready(0);
    let existing = update(core.propose_batch(vec![proposal(12)]).unwrap());
    let before = core.state().clone();
    assert_eq!(
        core.propose_after_ready_replies(&replies, vec![proposal(13)]),
        Err(RaftError::Busy)
    );
    assert_eq!(core.state(), &before);
    core.validate_persist_effect(&existing).unwrap();
    assert!(core.pending.as_ref().unwrap().ticket.is_none());
}
