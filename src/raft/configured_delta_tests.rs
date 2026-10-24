// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Real configured pending ownership, connection and retirement controls.
use super::*;

fn compare_pending_connections(core: &mut Raft) {
    let pending = core.pending.as_ref().unwrap();
    assert!(pending.next.full().is_none());
    assert!(pending.membership.is_none());
    let update = pending.next.update();
    let next = OwnedNextLog::full_oracle(LogView::new(core.state()), &update, core.limits).unwrap();
    let delta = std::mem::replace(&mut core.pending.as_mut().unwrap().next, next);
    let oracle = core.connection_replicas(6).unwrap();
    let refusal = core.connection_replicas(oracle.len() - 1);
    core.pending.as_mut().unwrap().next = delta;
    assert_eq!(core.connection_replicas(6).unwrap(), oracle);
    assert_eq!(core.connection_replicas(oracle.len() - 1), refusal);
    assert!(refusal.is_err());
}

#[test]
fn configured_pending_append_and_compacted_joint_commit_keep_connections_and_membership() {
    for compact in [false, true] {
        let mut core = joint(false);
        committed_fixture(&mut core, 3);
        if compact {
            let reference = SnapshotRef {
                store: core.binding.identity,
                group: core.state().bootstrap.group,
                configuration: cid(3),
                generation: SnapshotGeneration::new(1).unwrap(),
                index: 3,
                term: 1,
                application_schema: 1,
                file_bytes: 128,
                checksum: 7,
            };
            let effects = core.begin_compact(reference).unwrap().into_owned();
            durable(&mut core, effects);
        }
        let membership = core.membership().clone();
        let effects = core
            .persist(
                core.state().hard_state,
                3,
                Some(Suffix {
                    from: 4,
                    entries: vec![LogEntry {
                        index: 4,
                        term: 1,
                        payload: EntryPayload::Command {
                            operation: operation(555),
                            bytes: vec![7].into(),
                        },
                    }]
                    .into(),
                }),
                After::LeaderAppend,
                None,
            )
            .unwrap()
            .into_owned();
        compare_pending_connections(&mut core);
        durable(&mut core, effects);
        assert_eq!(core.membership(), &membership);
        assert!(!core.durable.summary().is_plain());
        let effects = core
            .persist(core.state().hard_state, 4, None, After::Commit, None)
            .unwrap()
            .into_owned();
        compare_pending_connections(&mut core);
        durable(&mut core, effects);
        assert_eq!(core.membership(), &membership);
        assert_eq!(core.membership(), &core.state().membership().unwrap());
        assert!(!core.durable.summary().is_plain());
    }
}

#[test]
fn configured_final_commit_delta_retires_only_after_original_completion() {
    for removed in [false, true] {
        let mut core = retiring_leader(removed);
        let membership = core.membership().clone();
        assert!(acknowledge(&mut core, 2).is_empty());
        let effects = acknowledge(&mut core, 3);
        compare_pending_connections(&mut core);
        assert_eq!(core.role(), Role::Leader);
        assert_eq!(core.state().commit_index, 3);
        assert_eq!(
            core.complete(&DurableLog { tickets: vec![] }),
            Err(RaftError::WrongCompletion)
        );
        let completed = durable(&mut core, effects);
        assert_eq!(core.state().commit_index, 4);
        assert_eq!(core.role(), Role::Follower);
        assert_eq!(core.membership(), &membership);
        assert!(!core.durable.summary().is_plain());
        let notices: Vec<_> = completed
            .iter()
            .filter_map(|effect| {
                if let Effect::Send(message) = effect {
                    Some(message)
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(notices.len(), 4);
        assert!(notices.iter().all(|message| matches!(&message.rpc,
            Rpc::Append {previous_index: 4, entries, leader_commit: 4, ..} if entries.is_empty())));
    }
}
