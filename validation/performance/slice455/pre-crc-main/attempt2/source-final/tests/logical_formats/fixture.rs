// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use std::collections::BTreeMap;
use voteboat::{
    contracts::HardState, identity::*, log::*, membership::Membership, quorum::*, raft::*,
    snapshot::*, wire::*,
};

pub fn node(id: u64) -> NodeId {
    NodeId::new(id).unwrap()
}
pub fn store(id: u128) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(id).unwrap(),
        incarnation: StoreIncarnation::new(2).unwrap(),
    }
}
pub fn bootstrap() -> Bootstrap {
    Bootstrap {
        group: GroupIdentity {
            id: GroupId::new(7).unwrap(),
            incarnation: GroupIncarnation::new(3).unwrap(),
        },
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority((1..=3).map(|id| Tree::Voter(node(id))).collect()),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: (1..=3).map(|id| (node(id), store(id.into()))).collect(),
    }
}
pub fn entries() -> Vec<LogEntry> {
    (1..=2)
        .map(|index| LogEntry {
            index,
            term: 3,
            payload: EntryPayload::Command {
                operation: OperationId::new(index.into()).unwrap(),
                bytes: vec![88 + index as u8, 0, 7].into(),
            },
        })
        .collect()
}
pub fn mutations() -> Vec<LogMutation> {
    let bootstrap = bootstrap();
    vec![LogMutation::Update(LogUpdate {
        group: bootstrap.group,
        expected_revision: LogRevision::new(1).unwrap(),
        hard_state: HardState {
            term: 3,
            voted_for: Some(node(1)),
        },
        commit_index: 2,
        suffix: Some(Suffix {
            from: 1,
            entries: entries(),
        }),
        snapshot: None,
        snapshot_membership: None,
    })]
}
pub fn state() -> BTreeMap<GroupIdentity, GroupLog> {
    let mut state = BTreeMap::new();
    apply_batch(
        &mut state,
        &[LogMutation::Create(bootstrap())],
        LogLimits::default(),
    )
    .unwrap();
    apply_batch(&mut state, &mutations(), LogLimits::default()).unwrap();
    state
}
pub fn snapshot(member: bool) -> Snapshot {
    let bootstrap = bootstrap();
    Snapshot {
        metadata: SnapshotMetadata {
            membership: member.then(|| Box::new(Membership::replay(&bootstrap, &[], 0).unwrap())),
            bootstrap,
            index: 2,
            term: 3,
            application_schema: 1,
        },
        application: vec![1, 0, 255, 7],
    }
}
pub fn scope() -> WireScope {
    WireScope {
        from: node(1),
        sender: StoreBinding {
            identity: store(1),
            session: StoreSession::new(5).unwrap(),
        },
        to: node(2),
    }
}
pub fn messages() -> Vec<Message> {
    let scope = scope();
    vec![Message {
        group: bootstrap().group,
        configuration: ConfigurationId::new(1).unwrap(),
        from: scope.from,
        sender: scope.sender,
        to: scope.to,
        term: 3,
        context: RequestContext {
            origin: scope.sender,
            sequence: 9,
        },
        rpc: Rpc::Append {
            previous_index: 0,
            previous_term: 0,
            entries: entries(),
            leader_commit: 2,
        },
    }]
}
