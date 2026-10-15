// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{
    native::runtime::{DeadlineQueue, FairScheduler, JitterEntropy},
    raft::{Event, Raft},
};
fn args(values: &[&str]) -> Vec<String> {
    values.iter().map(|s| (*s).to_owned()).collect()
}
#[test]
fn parser_refuses_invalid_modes_and_keeps_legacy_windows() {
    for values in [
        vec!["--offered-capacity"],
        vec!["--offered-capacity", "64"],
        vec!["--offered-capacity", "4096", "--offered-capacity", "4096"],
    ] {
        let mut input = args(&values);
        let before = input.clone();
        assert!(CapacityProfile::extract(&mut input).is_err());
        assert_eq!(input, before);
    }
    let mut input = args(&[
        "root",
        "tcp",
        "8192",
        "4096",
        "--offered-capacity",
        "4096",
        "1",
        "--offered",
        "512",
    ]);
    let selected = CapacityProfile::extract(&mut input).unwrap();
    assert_eq!(
        input,
        args(&["root", "tcp", "8192", "4096", "1", "--offered", "512"])
    );
    assert!(selected.validate(false, 1).is_err());
    assert!(selected.validate(true, 4096).is_ok());
    assert!(selected.validate(true, 4097).is_err());
    assert!(selected.validate_workload(0, true, 4096).is_err());
    assert!(selected.validate_workload(50001, true, 4096).is_err());
    assert!(selected.validate_workload(8192, true, 4096).is_ok());
    assert!(selected.validate(true, 0).is_err());
    assert!(CapacityProfile::Legacy.validate(true, 65).is_err());
    assert!(CapacityProfile::Legacy.validate(false, 64).is_ok());
    assert_eq!(
        CapacityProfile::extract(&mut input).unwrap(),
        CapacityProfile::Legacy
    );
}
fn shard(profile: CapacityProfile) -> TimedShard<FairScheduler, DeadlineQueue, JitterEntropy> {
    let binding = StoreBinding {
        identity: store(1),
        session: StoreSession::new(1).unwrap(),
    };
    let owner = RuntimeOwner {
        store: binding,
        lane: ExecutionLaneId::new(1).unwrap(),
        generation: RuntimeGeneration::new(1).unwrap(),
    };
    let state = GroupLog {
        bootstrap: Bootstrap {
            group: group(),
            configuration: ConfigurationId::new(1).unwrap(),
            policy: Policy::new(Tree::Voter(node(1)), Limits::default()).unwrap(),
            voter_stores: BTreeMap::from([(node(1), store(1))]),
        },
        revision: LogRevision::new(1).unwrap(),
        generation: LogGeneration::new(1).unwrap(),
        hard_state: Default::default(),
        ballot_origin: None,
        commit_index: 0,
        snapshot: None,
        snapshot_membership: None,
        entries: Vec::new(),
    };
    let core = Raft::recover(node(1), binding, state, LogLimits::default()).unwrap();
    shared::timed_shard_profile(vec![core], owner, 1, 1, &Instant::now(), 16, profile)
        .unwrap()
        .1
}
#[test]
fn selected_queue_retains_4096_originals_and_separate_control_reserve() {
    for (profile, count) in [
        (CapacityProfile::Legacy, 240),
        (CapacityProfile::Offered4096, 4096),
    ] {
        let mut shard = shard(profile);
        for id in 1..=count {
            shard
                .admit(
                    group(),
                    Event::Propose {
                        operation: OperationId::new(id as u128).unwrap(),
                        bytes: vec![0; 8],
                    },
                )
                .unwrap();
        }
        let rejected = shard
            .admit(
                group(),
                Event::Propose {
                    operation: OperationId::new(99999).unwrap(),
                    bytes: vec![0; 8],
                },
            )
            .unwrap_err();
        assert_eq!(rejected.reason, RuntimeError::Overloaded);
        assert_eq!(shard.usage().items, count);
        for _ in 0..16 {
            shard.admit(group(), Event::Campaign).unwrap();
        }
        assert_eq!(shard.usage().items, count + 16);
        assert_eq!(
            shard.admit(group(), Event::Campaign).unwrap_err().reason,
            RuntimeError::Overloaded
        );
    }
}
#[test]
fn actual_readback_rejects_changed_independent_bounds_and_profile_drift() {
    for profile in [CapacityProfile::Legacy, CapacityProfile::Offered4096] {
        let shard = shard(profile);
        let binding = ClientRouterBinding::new(
            RuntimeAuthorization::physical(shard.owner()),
            ClientRouterGeneration::new(1).unwrap(),
        );
        let clients = ClientRouter::<CounterReceipt>::new(binding, profile.clients()).unwrap();
        profile
            .readback(shard.limits(), clients.limits(), 16, 1)
            .unwrap();
        for changed in [profile.shard(32, 1), profile.shard(16, 2)] {
            assert!(profile.readback(changed, clients.limits(), 16, 1).is_err());
        }
        let mut changed = clients.limits();
        changed.bytes += 1;
        assert!(profile.readback(shard.limits(), changed, 16, 1).is_err());
        assert!(profile
            .readback(
                CapacityProfile::Offered4096.shard(16, 1),
                CapacityProfile::Legacy.clients(),
                16,
                1
            )
            .is_err());
    }
}
