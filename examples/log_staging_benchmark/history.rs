// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Validated initial configured and checkpoint shapes, outside timed regions.
use super::{checked, Failure, History, Workload};
use voteboat::{
    identity::*,
    log::{EntryPayload, GroupLog, LogEntry, LogMutation, LogUpdate},
    membership::{Configuration, ConfigurationChange, ConfigurationRecord},
    quorum::{Limits, Policy, Tree},
    snapshot::SnapshotRef,
};

fn learner(state: &GroupLog) -> Result<Configuration, Failure> {
    checked(Configuration::new(
        ConfigurationId::new(2).unwrap(),
        state.bootstrap.policy.clone(),
        state.bootstrap.voter_stores.clone(),
        [(
            NodeId::new(2).unwrap(),
            StoreIdentity {
                id: StoreId::new(2).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
        )]
        .into(),
    ))
}

fn record(index: u64, expected: u64, change: ConfigurationChange) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Configuration(Box::new(ConfigurationRecord {
            operation: OperationId::new(10_000 + u128::from(index)).unwrap(),
            expected: ConfigurationId::new(expected).unwrap(),
            change,
        })),
    }
}

pub(super) fn entries(state: &GroupLog, config: Workload) -> Result<Vec<LogEntry>, Failure> {
    let mut entries = (1..=config.history + config.kind.base())
        .map(|index| super::workload::entry(index, index as u128))
        .collect::<Vec<_>>();
    if config.kind != History::Plain {
        let learner = learner(state)?;
        entries[0] = record(1, 1, ConfigurationChange::Learners(learner.clone()));
        if config.kind == History::Joint {
            let mut stores = learner.voter_stores().clone();
            stores.extend(learner.learners());
            let policy = checked(Policy::new(
                Tree::Majority(stores.keys().copied().map(Tree::Voter).collect()),
                Limits::default(),
            ))?;
            let next = checked(Configuration::new(
                ConfigurationId::new(4).unwrap(),
                policy,
                stores,
                Default::default(),
            ))?;
            entries[1] = record(
                2,
                2,
                ConfigurationChange::Joint {
                    id: ConfigurationId::new(3).unwrap(),
                    next,
                },
            );
        }
    }
    Ok(entries)
}

pub(super) fn checkpoint(state: &GroupLog) -> Result<LogMutation, Failure> {
    let membership = checked(state.checkpoint_membership(1))?;
    let configuration = membership
        .as_ref()
        .ok_or("checkpoint lacks configuration")?
        .id();
    Ok(LogMutation::Update(LogUpdate {
        group: state.bootstrap.group,
        expected_revision: state.revision,
        hard_state: state.hard_state,
        commit_index: state.commit_index,
        snapshot: Some(SnapshotRef {
            store: state.bootstrap.voter_stores[&NodeId::new(1).unwrap()],
            group: state.bootstrap.group,
            configuration,
            generation: SnapshotGeneration::new(1).unwrap(),
            index: 1,
            term: 1,
            application_schema: 1,
            file_bytes: 100,
            checksum: 7,
        }),
        snapshot_membership: membership,
        suffix: None,
    }))
}
