// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Direct entry staging: `ProposalBatch` against the legacy proposal APIs.
use super::*;

const ENTRY: usize = size_of::<LogEntry>();
const HEADER: usize = size_of::<Vec<LogEntry>>();

fn operation(value: u128) -> OperationId {
    OperationId::new(value).unwrap()
}
fn mixed(width: usize) -> Vec<Proposal> {
    (1..=width)
        .map(|n| Proposal {
            operation: operation(n as u128),
            bytes: vec![n as u8; n % 5],
        })
        .collect()
}
fn staged(core: &Raft, proposals: &[Proposal]) -> ProposalBatch {
    let mut batch = core.proposal_batch(proposals.len()).unwrap();
    for proposal in proposals {
        batch
            .push(proposal.operation, proposal.bytes.clone())
            .unwrap();
    }
    batch
}
fn refused(core: &mut Raft, batch: ProposalBatch, error: RaftError) {
    let before = core.state().clone();
    let configuration = core.membership().clone();
    assert_eq!(core.propose_entries(batch), Err(error));
    assert_eq!(core.state(), &before);
    assert_eq!(core.membership(), &configuration);
    assert!(core.pending.is_none());
}
fn rejected_with(reason: &'static str) -> RaftError {
    StorageError::Rejected(reason).into()
}
fn completed(core: &mut Raft) -> Vec<Effect> {
    let exact = ticket(core);
    core.admitted(exact).unwrap();
    core.complete(&DurableLog {
        tickets: vec![exact],
    })
    .unwrap()
}
fn persisted(effects: &[Effect]) -> &Suffix {
    let [Effect::Persist(update)] = effects else {
        panic!("one persistence request, got {effects:?}")
    };
    update.suffix.as_ref().unwrap()
}
fn shared(len: usize) -> usize {
    CommandBytes::retained_bytes_for_len(len).unwrap()
}

#[test]
fn entry_batches_match_legacy_proposals_at_widths_one_two_and_max() {
    for width in [1, 2, MAX_PROPOSAL_BATCH_ENTRIES] {
        let proposals = mixed(width);
        let mut old = leader();
        let mut new = leader();
        let expected = if width == 1 {
            old.step(Event::Propose {
                operation: proposals[0].operation,
                bytes: proposals[0].bytes.clone(),
            })
        } else {
            old.propose_batch(proposals.clone())
        }
        .unwrap();
        let batch = staged(&new, &proposals);
        assert_eq!(batch.len(), width);
        let effects = new.propose_entries(batch).unwrap();
        assert_eq!(effects, expected);
        let entries = &persisted(&effects).entries;
        for (n, (entry, original)) in entries.iter().zip(&proposals).enumerate() {
            assert_eq!(entry.index, n as u64 + 2);
            assert_eq!(entry.term, 1);
            assert_eq!(
                entry.payload,
                EntryPayload::Command {
                    operation: original.operation,
                    bytes: original.bytes.clone().into(),
                }
            );
        }
        assert_eq!(
            new.propose_entries(staged(&new, &proposals[..1])),
            Err(RaftError::Busy)
        );
        assert_eq!(completed(&mut new), completed(&mut old));
        assert_eq!(new.state(), old.state());
        assert!(new.log_summary.matches(new.state()));
        assert_eq!(new.retained_log_bytes(), old.retained_log_bytes());
    }
}

#[test]
fn entry_batches_use_the_same_fence_dependency_role_and_transfer_guards() {
    let mut core = leader();
    let one = mixed(1);
    let empty = core.proposal_batch(4).unwrap();
    refused(&mut core, empty, rejected_with("proposal count budget"));
    core.role = Role::Follower;
    let batch = staged(&core, &one);
    refused(&mut core, batch, RaftError::NotLeader);
    core.role = Role::Leader;
    core.checkpoint_requested = Some(RequestContext {
        origin: core.binding,
        sequence: 1,
    });
    let batch = staged(&core, &one);
    refused(&mut core, batch, RaftError::Busy);
    core.checkpoint_requested = None;
    let target = PeerIdentity {
        node: node(2),
        store: core.membership().voter_store(node(2)).unwrap(),
    };
    core.step(Event::TransferLeadership(LeadershipTransferRequest {
        operation: operation(20),
        target,
        configuration: core.membership().id(),
    }))
    .unwrap();
    let batch = staged(&core, &one);
    refused(&mut core, batch, RaftError::Busy);
    core.storage_failed();
    let batch = staged(&core, &one);
    refused(&mut core, batch, RaftError::Fenced);
}

#[test]
fn a_builder_is_rechecked_under_the_receiving_cores_own_limits() {
    let wide = leader();
    let three: Vec<_> = (1..=3)
        .map(|n| Proposal {
            operation: operation(n),
            bytes: vec![n as u8; 10],
        })
        .collect();
    let mut narrow = leader();
    narrow.limits.max_command_bytes = 9;
    refused(
        &mut narrow,
        staged(&wide, &three),
        rejected_with("command byte budget"),
    );
    refused(
        &mut narrow,
        staged(&wide, &three[..1]),
        rejected_with("command byte budget"),
    );
    let mut narrow = leader();
    narrow.batch_limits.max_proposal_entries = 2;
    refused(
        &mut narrow,
        staged(&wide, &three),
        rejected_with("proposal count budget"),
    );
    let mut spare = wide.proposal_batch(8).unwrap();
    spare.push(operation(1), Vec::new()).unwrap();
    narrow.batch_limits.max_proposal_entries = 4;
    refused(
        &mut narrow,
        spare,
        rejected_with("proposal allocation count budget"),
    );
    let mut narrow = leader();
    narrow.limits.max_entries_per_group = narrow.state().entries.len() + 2;
    refused(
        &mut narrow,
        staged(&wide, &three),
        rejected_with("proposal retained entry budget"),
    );
    // The recorded peak: the final push holds the whole entry allocation, the
    // first two shared payloads, the third raw Vec and its shared copy.
    let large: Vec<_> = (1..=3)
        .map(|n| Proposal {
            operation: operation(n),
            bytes: vec![n as u8; 100],
        })
        .collect();
    let peak = HEADER + 3 * ENTRY + 3 * shared(100) + 100;
    let mut narrow = leader();
    narrow.limits.max_command_bytes = 100;
    narrow.limits.max_batch_bytes = peak - 1;
    refused(
        &mut narrow,
        staged(&wide, &large),
        rejected_with("proposal shared conversion peak budget"),
    );
    narrow.limits.max_batch_bytes = peak;
    narrow.propose_entries(staged(&wide, &large)).unwrap();
}

#[test]
fn a_builder_from_an_older_term_and_index_takes_the_current_ones() {
    let creator = leader();
    let early = staged(&creator, &mixed(3));
    let mut current = leader_at(2);
    current
        .propose_entries(staged(&current, &mixed(2)))
        .unwrap();
    completed(&mut current);
    let effects = current.propose_entries(early).unwrap();
    let suffix = persisted(&effects);
    assert_eq!(suffix.from, 4);
    let stamped: Vec<_> = suffix.entries.iter().map(|e| (e.index, e.term)).collect();
    assert_eq!(stamped, [(4, 2), (5, 2), (6, 2)]);
    completed(&mut current);
    assert_eq!(current.state().last_index(), 6);
    assert!(current.log_summary.matches(current.state()));
}

#[test]
fn a_refused_push_keeps_earlier_entries_and_a_dropped_batch_leaves_no_state() {
    let mut core = leader();
    assert_eq!(
        core.proposal_batch(MAX_PROPOSAL_BATCH_ENTRIES + 1).err(),
        Some(rejected_with("proposal allocation count budget"))
    );
    core.batch_limits.max_proposal_entries = 2;
    let mut batch = core.proposal_batch(1).unwrap();
    batch.push(operation(1), vec![1]).unwrap();
    batch.push(operation(2), vec![2, 2]).unwrap();
    let retained = batch.retained_bytes();
    assert_eq!(retained, HEADER + 2 * ENTRY + shared(1) + shared(2));
    assert_eq!(
        batch.push(operation(3), Vec::new()),
        Err(rejected_with("proposal count budget"))
    );
    core.batch_limits.max_proposal_entries = 4;
    let mut wider = core.proposal_batch(0).unwrap();
    wider.push(operation(1), vec![1]).unwrap();
    let too_long = vec![0; core.limits.max_command_bytes + 1];
    assert_eq!(
        wider.push(operation(2), too_long),
        Err(rejected_with("command byte budget"))
    );
    assert_eq!(wider.len(), 1);
    drop(wider);
    assert!(core.pending.is_none());
    assert_eq!(batch.len(), 2);
    assert_eq!(batch.retained_bytes(), retained);
    let effects = core.propose_entries(batch).unwrap();
    let operations: Vec<_> = persisted(&effects)
        .entries
        .iter()
        .map(|entry| match &entry.payload {
            EntryPayload::Command { operation, bytes } => (*operation, bytes.to_vec()),
            _ => panic!("command entry"),
        })
        .collect();
    assert_eq!(
        operations,
        [(operation(1), vec![1]), (operation(2), vec![2, 2])]
    );
}

#[test]
fn single_entries_keep_the_propose_ceiling_and_later_pushes_the_staging_peak() {
    let mut core = leader();
    core.limits.max_batch_bytes = 256;
    core.limits.max_command_bytes = 256;
    core.limits.validate().unwrap();
    // As `Event::Propose`, one original is bounded by its command length only.
    let mut single = core.proposal_batch(1).unwrap();
    single.push(operation(7), vec![7; 256]).unwrap();
    let mut pair = core.proposal_batch(2).unwrap();
    pair.push(operation(7), vec![7; 256]).unwrap();
    assert_eq!(
        pair.push(operation(8), Vec::new()),
        Err(rejected_with("proposal shared conversion peak budget"))
    );
    assert_eq!(pair.len(), 1);
    let expected = leader_with(&core)
        .step(Event::Propose {
            operation: operation(7),
            bytes: vec![7; 256],
        })
        .unwrap();
    assert_eq!(core.propose_entries(single).unwrap(), expected);

    // Exact multi-entry boundary: entry allocation, staged shared bytes, the
    // incoming Vec capacity and its prospective shared copy, simultaneously.
    let mut core = leader();
    let input = || {
        let mut bytes = Vec::with_capacity(140);
        bytes.extend_from_slice(&[9; 100]);
        bytes
    };
    let peak = HEADER + 2 * ENTRY + shared(100) + 140 + shared(100);
    core.limits.max_command_bytes = 100;
    core.limits.max_batch_bytes = peak - 1;
    let mut batch = core.proposal_batch(2).unwrap();
    batch.push(operation(1), input()).unwrap();
    assert_eq!(
        batch.push(operation(2), input()),
        Err(rejected_with("proposal shared conversion peak budget"))
    );
    core.limits.max_batch_bytes = peak;
    let mut batch = core.proposal_batch(2).unwrap();
    batch.push(operation(1), input()).unwrap();
    batch.push(operation(2), input()).unwrap();
    core.propose_entries(batch).unwrap();
}

#[test]
fn growth_is_clamped_so_old_and_new_buffers_fit_the_staging_peak() {
    let mut core = leader();
    // Doubling 4 → 8 would hold 12 entries at once; 4 → 7 holds exactly 11.
    core.limits.max_command_bytes = 64;
    core.limits.max_batch_bytes = HEADER + 11 * ENTRY;
    let mut batch = core.proposal_batch(4).unwrap();
    for n in 1..=7 {
        batch.push(operation(n), Vec::new()).unwrap();
    }
    assert_eq!(batch.allocation().1, 7);
    assert_eq!(batch.retained_bytes(), HEADER + 7 * ENTRY);
    // Growing again needs at least 7 + 8 entries at once.
    assert_eq!(
        batch.push(operation(8), Vec::new()),
        Err(rejected_with("proposal shared conversion peak budget"))
    );
    assert_eq!(batch.len(), 7);
    let effects = core.propose_entries(batch).unwrap();
    assert_eq!(persisted(&effects).entries.len(), 7);
    // Without a byte limit in the way growth doubles up to the count ceiling.
    let core = leader();
    let mut batch = core.proposal_batch(0).unwrap();
    let mut capacities = Vec::new();
    for n in 1..=MAX_PROPOSAL_BATCH_ENTRIES {
        batch.push(operation(n as u128), Vec::new()).unwrap();
        capacities.push(batch.allocation().1);
    }
    capacities.dedup();
    assert_eq!(capacities, [1, 2, 4, 8, 16, 32, 64]);
}

#[test]
fn the_builder_allocation_becomes_the_persisted_suffix() {
    let mut core = leader();
    let batch = staged(&core, &mixed(5));
    let (pointer, capacity) = batch.allocation();
    assert_eq!(capacity, 5);
    let effects = core.propose_entries(batch).unwrap();
    // No second proposal-to-entry collect: the request holds the same buffer.
    assert_eq!(persisted(&effects).entries.as_ptr(), pointer);
}

#[test]
fn legacy_batches_keep_their_proposal_charged_peak_boundary() {
    let proposals: Vec<_> = (1..=2)
        .map(|n| Proposal {
            operation: operation(n),
            bytes: vec![n as u8; 30],
        })
        .collect();
    let legacy = size_of::<Vec<Proposal>>()
        + proposals.capacity() * size_of::<Proposal>()
        + proposals.len() * ENTRY
        + 2 * (30 + shared(30));
    let mut core = leader();
    core.limits.max_command_bytes = 30;
    core.limits.max_batch_bytes = legacy - 1;
    rejected(&mut core, proposals.clone());
    assert_eq!(
        core.propose_batch(proposals.clone()),
        Err(rejected_with("proposal shared conversion peak budget"))
    );
    // The builder never holds the proposal Vec, so it fits where it does not.
    core.propose_entries(staged(&core, &proposals)).unwrap();
    let mut core = leader();
    core.limits.max_command_bytes = 30;
    core.limits.max_batch_bytes = legacy;
    core.propose_batch(proposals).unwrap();
}

fn leader_with(limits: &Raft) -> Raft {
    let mut core = leader();
    core.limits = limits.limits;
    core
}

/// A recovered leader whose log ends at `last_index`, behind a checkpoint.
fn near_exhaustion(last_index: u64) -> Raft {
    let mut core = recovered_at(last_index).unwrap();
    core.role = Role::Leader;
    core.initialize_replication().unwrap();
    core
}
fn recovered_at(last_index: u64) -> Result<Raft, RaftError> {
    let seed = leader();
    let mut state = seed.state().clone();
    state.entries.clear();
    state.commit_index = last_index;
    state.snapshot = Some(SnapshotRef {
        store: seed.binding.identity,
        group: state.bootstrap.group,
        configuration: state.bootstrap.configuration,
        generation: SnapshotGeneration::new(1).unwrap(),
        index: last_index,
        term: 1,
        application_schema: 1,
        file_bytes: 100,
        checksum: 7,
    });
    Raft::recover_verified(node(1), seed.binding, state, seed.limits)
}
fn legacy(core: &mut Raft, proposals: &[Proposal]) -> Result<Vec<Effect>, RaftError> {
    if let [one] = proposals {
        core.step(Event::Propose {
            operation: one.operation,
            bytes: one.bytes.clone(),
        })
    } else {
        core.propose_batch(proposals.to_vec())
    }
}

#[test]
fn entry_batches_match_legacy_proposals_at_the_last_representable_indexes() {
    for width in [1, 2, MAX_PROPOSAL_BATCH_ENTRIES] {
        let proposals = mixed(width);
        // First index `u64::MAX - width + 1` ends exactly at `u64::MAX`;
        // `u64::MAX - width + 2` would need index `u64::MAX + 1`.
        for (last_index, fits) in [
            (u64::MAX - width as u64, true),
            (u64::MAX - width as u64 + 1, false),
        ] {
            if last_index == u64::MAX {
                // Width 1 would start past `u64::MAX`; no recovered log ends there.
                assert_eq!(
                    recovered_at(last_index).err(),
                    Some(RaftError::InvalidRecovery)
                );
                continue;
            }
            let mut old = near_exhaustion(last_index);
            let mut new = near_exhaustion(last_index);
            let before = new.state().clone();
            let expected = legacy(&mut old, &proposals);
            let actual = new.propose_entries(staged(&new, &proposals));
            assert_eq!(actual, expected, "width {width}, last index {last_index}");
            if fits {
                let last = persisted(&actual.unwrap()).entries.last().unwrap().index;
                assert_eq!(last, u64::MAX);
                // Admission only: completing an entry at `u64::MAX` overflows
                // `append_for`'s `last_index() + 1` on both paths (existing).
                assert!(new.pending.is_some() && old.pending.is_some());
                assert_eq!(new.state(), old.state());
            } else {
                assert_eq!(actual, Err(RaftError::Exhausted));
                assert_eq!(new.state(), &before);
                assert!(new.pending.is_none());
            }
        }
    }
}

// Review probes (GPT-6.1-Sol, round 1): both panicked in the stamping loop.
#[test]
fn a_singleton_entry_batch_stages_at_the_last_representable_index() {
    let mut old = near_exhaustion(u64::MAX - 1);
    let mut new = near_exhaustion(u64::MAX - 1);
    let operation = operation(91);
    let expected = old
        .step(Event::Propose {
            operation,
            bytes: Vec::new(),
        })
        .unwrap();
    let mut batch = new.proposal_batch(1).unwrap();
    batch.push(operation, Vec::new()).unwrap();
    assert_eq!(new.propose_entries(batch).unwrap(), expected);
}

#[test]
fn a_singleton_entry_batch_stages_just_before_the_last_representable_index() {
    let mut old = near_exhaustion(u64::MAX - 2);
    let mut new = near_exhaustion(u64::MAX - 2);
    let operation = operation(92);
    let expected = old
        .step(Event::Propose {
            operation,
            bytes: Vec::new(),
        })
        .unwrap();
    let mut batch = new.proposal_batch(1).unwrap();
    batch.push(operation, Vec::new()).unwrap();
    assert_eq!(new.propose_entries(batch).unwrap(), expected);
}
