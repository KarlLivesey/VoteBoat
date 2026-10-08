// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
use voteboat::{
    application::*, identity::*, log::*, placement::PlacementRequirements, routed::*, routing::*,
};
#[path = "routed/host.rs"]
mod host;
#[cfg(feature = "tls")]
#[path = "routed/native.rs"]
mod native;
#[cfg(feature = "tls")]
mod support;
#[derive(Clone)]
struct HostPolicy;
impl PartitionPolicy for HostPolicy {
    fn scheme(&self) -> PartitionScheme {
        PartitionScheme {
            id: RoutingSchemeId::new(1).unwrap(),
            version: 1,
        }
    }
    fn bucket(&self, key: &[u8]) -> Result<u16, RoutingError> {
        match key {
            [byte] => Ok((*byte).into()),
            _ => Err(RoutingError::InvalidKey),
        }
    }
}
fn group(value: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(value).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn responsibility(value: u128) -> ResponsibilityIdentity {
    ResponsibilityIdentity {
        id: ResponsibilityId::new(value).unwrap(),
        incarnation: ResponsibilityIncarnation::new(1).unwrap(),
    }
}
fn grant() -> ResponsibilityManifest {
    ResponsibilityManifest::new(ManifestInput {
        responsibility: responsibility(2),
        parent: Some(ParentAuthority {
            responsibility: responsibility(1),
            group: group(1),
        }),
        authority: group(1),
        application: ApplicationAdapter {
            id: ApplicationAdapterId::new(1).unwrap(),
            version: 1,
        },
        scheme: HostPolicy.scheme(),
        scope: BucketRange::new(0, 128).unwrap(),
        epoch: OwnershipEpoch::new(1).unwrap(),
        generation: RouteGeneration::new(1).unwrap(),
        placement: PlacementRequirements {
            minimum_voting_domains: 2,
            survive_any_single_domain_loss: false,
        },
        state: ResponsibilityState::Active,
        execution: ExecutionMode::Single(group(20)),
    })
    .unwrap()
}
fn hint(key: u8) -> RouteHint {
    let input = grant().into_input();
    RouteHint {
        responsibility: input.responsibility,
        group: group(20),
        application: input.application,
        scheme: input.scheme,
        scope: input.scope,
        bucket: key.into(),
        epoch: input.epoch,
        generation: input.generation,
    }
}
fn limits() -> RoutedLimits {
    RoutedLimits {
        operations: 64,
        semantic_bytes: 4096,
        payload_bytes: 8,
        inner_checkpoint_bytes: 32 + 33 * 64,
    }
}
fn fresh() -> RoutedApplication<Counter, HostPolicy> {
    RoutedApplication::new(
        group(20),
        grant(),
        Counter::new(64).unwrap(),
        HostPolicy,
        limits(),
    )
    .unwrap_or_else(|r| panic!("{:?}", r.error))
}
fn entry(index: u64, operation: u128, bytes: Vec<u8>) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: OperationId::new(operation).unwrap(),
            bytes,
        },
    }
}
fn initialized() -> RoutedApplication<Counter, HostPolicy> {
    let mut app = fresh();
    let bytes = app.bootstrap_command(MAX_ROUTED_COMMAND_BYTES).unwrap();
    assert_eq!(
        app.apply_batch(&[entry(1, 10000, bytes)]).unwrap()[0].outcome,
        RoutedOutcome::Bootstrapped
    );
    app
}
fn data(key: u8, delta: i64) -> Vec<u8> {
    encode_routed(
        hint(key),
        &[key],
        &delta.to_le_bytes(),
        MAX_ROUTED_COMMAND_BYTES,
    )
    .unwrap()
}
fn value(app: &RoutedApplication<Counter, HostPolicy>) -> i64 {
    app.application().read_applied(app.applied_index()).unwrap()
}

#[test]
fn bootstrap_binds_owner_namespace_policy_and_initial_application_before_service() {
    let mut app = fresh();
    assert_eq!(app.validate_group(group(20)), Ok(()));
    assert_eq!(
        app.validate_group(group(21)),
        Err(ApplicationError::InvalidCommand)
    );
    let bytes = data(10, 7);
    let operation = OperationId::new(1).unwrap();
    assert_eq!(
        app.validate_proposal(operation, &bytes, std::iter::empty()),
        Err(ApplicationError::NotApplied)
    );
    assert_eq!(
        app.apply_batch(&[entry(1, 1, bytes)]),
        Err(ApplicationError::NotApplied)
    );
    assert_eq!(app.applied_index(), 0);
    let binding = app.bootstrap_command(MAX_ROUTED_COMMAND_BYTES).unwrap();
    assert!(app.bootstrap_command(binding.len() - 1).is_err());
    for field in 0..4 {
        let mut g = grant().into_input();
        let mut config = limits();
        let mut local = group(20);
        let mut counter = Counter::new(64).unwrap();
        match field {
            0 => g.epoch = OwnershipEpoch::new(2).unwrap(),
            1 => config.semantic_bytes -= 1,
            2 => counter = Counter::new(63).unwrap(),
            _ => {
                local = group(21);
                g.execution = ExecutionMode::Single(local);
            }
        }
        let mut wrong = RoutedApplication::new(
            local,
            ResponsibilityManifest::new(g).unwrap(),
            counter,
            HostPolicy,
            config,
        )
        .unwrap_or_else(|r| panic!("{:?}", r.error));
        assert_eq!(
            wrong.apply_batch(&[entry(1, 10000, binding.clone())]),
            Err(ApplicationError::InvalidCommand)
        );
        assert_eq!(wrong.applied_index(), 0);
        assert!(!wrong.is_initialized());
    }
    app.apply_batch(&[entry(1, 10000, binding.clone()), entry(2, 10000, binding)])
        .unwrap();
    assert!(app.is_initialized());
    assert_eq!(value(&app), 0);
    assert_eq!(app.remaining_operations(), 64);
}

#[test]
fn admission_and_apply_recheck_context_and_bind_retries_to_key_and_payload() {
    let mut app = initialized();
    let bytes = data(10, 7);
    app.validate_proposal(OperationId::new(1).unwrap(), &bytes, std::iter::empty())
        .unwrap();
    let first = app.apply_batch(&[entry(2, 1, bytes)]).unwrap().remove(0);
    assert!(matches!(
        first.outcome,
        RoutedOutcome::Applied(CounterReceipt {
            outcome: CounterOutcome::Value(7),
            duplicate: false,
            ..
        })
    ));
    let mut refreshed = hint(10);
    refreshed.generation = RouteGeneration::new(99).unwrap();
    let retry = encode_routed(refreshed, &[10], &7i64.to_le_bytes(), 1000).unwrap();
    assert!(matches!(
        app.apply_batch(&[entry(3, 1, retry)]).unwrap()[0].outcome,
        RoutedOutcome::Applied(CounterReceipt {
            outcome: CounterOutcome::Value(7),
            duplicate: true,
            ..
        })
    ));
    assert_eq!(
        app.apply_batch(&[entry(4, 1, data(11, 7))]).unwrap()[0].outcome,
        RoutedOutcome::OperationConflict
    );
    assert_eq!(
        app.apply_batch(&[entry(5, 1, data(10, 8))]).unwrap()[0].outcome,
        RoutedOutcome::OperationConflict
    );
    for field in 0..6 {
        let mut changed = hint(10);
        match field {
            0 => changed.group = group(21),
            1 => changed.epoch = OwnershipEpoch::new(2).unwrap(),
            2 => changed.responsibility.incarnation = ResponsibilityIncarnation::new(2).unwrap(),
            3 => changed.scope = BucketRange::new(0, 256).unwrap(),
            4 => changed.bucket = 11,
            _ => changed.scheme.version = 2,
        }
        let bytes = encode_routed(changed, &[10], &5i64.to_le_bytes(), 1000).unwrap();
        assert!(app
            .validate_proposal(OperationId::new(2).unwrap(), &bytes, std::iter::empty())
            .is_err());
        assert!(matches!(
            app.apply_batch(&[entry(6 + field, 2, bytes)]).unwrap()[0].outcome,
            RoutedOutcome::Rejected(_)
        ));
    }
    assert_eq!(value(&app), 7);
    assert_eq!(app.remaining_operations(), 63);
    assert_eq!(app.applied_index(), 11);
}

#[test]
fn durable_local_fence_rejects_previously_admitted_data_and_reads_without_thaw() {
    let mut app = initialized();
    app.apply_batch(&[entry(2, 1, data(10, 7))]).unwrap();
    let queued = data(10, 3);
    app.validate_proposal(OperationId::new(2).unwrap(), &queued, std::iter::empty())
        .unwrap();
    let epoch = grant().input().epoch;
    let fence = encode_fence(epoch);
    app.validate_proposal(OperationId::new(500).unwrap(), &fence, std::iter::empty())
        .unwrap();
    let receipt = app
        .apply_batch(&[entry(3, 500, fence.clone()), entry(4, 2, queued)])
        .unwrap();
    let original = app.fence().unwrap();
    assert_eq!(original.index, 3);
    assert_eq!(receipt[0].outcome, RoutedOutcome::Fenced(original));
    assert_eq!(
        receipt[1].outcome,
        RoutedOutcome::Rejected(RoutingError::Fenced)
    );
    assert_eq!(
        app.read_at(
            2,
            RoutedQuery {
                hint: hint(10),
                key: vec![10],
                query: ()
            }
        )
        .unwrap(),
        RoutedRead::Rejected(RoutingError::Fenced)
    );
    assert!(app
        .validate_proposal(
            OperationId::new(1).unwrap(),
            &data(10, 7),
            std::iter::empty()
        )
        .is_err());
    assert_eq!(
        app.apply_batch(&[entry(5, 500, fence.clone())]).unwrap()[0].outcome,
        RoutedOutcome::Fenced(original)
    );
    assert_eq!(
        app.apply_batch(&[entry(6, 501, fence)]).unwrap()[0].outcome,
        RoutedOutcome::Rejected(RoutingError::Fenced)
    );
    assert_eq!(value(&app), 7);
    assert_eq!(app.remaining_operations(), 63);
    let image = app.checkpoint(100000).unwrap();
    let mut reopened = fresh();
    reopened.restore_checkpoint(1, 6, &image).unwrap();
    assert_eq!(reopened.fence(), Some(original));
    assert_eq!(value(&reopened), 7);
    assert_eq!(
        reopened.apply_batch(&[entry(7, 1, data(10, 7))]).unwrap()[0].outcome,
        RoutedOutcome::Rejected(RoutingError::Fenced)
    );
}

#[test]
fn malformed_commands_and_failed_batches_leave_the_application_unchanged() {
    let mut app = initialized();
    let bytes = data(10, 7);
    let before = app.checkpoint(100000).unwrap();
    for end in 0..bytes.len() {
        assert!(app
            .apply_batch(&[entry(2, 1, bytes[..end].to_vec())])
            .is_err());
        assert_eq!(app.checkpoint(100000).unwrap(), before);
    }
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(app.apply_batch(&[entry(2, 1, trailing)]).is_err());
    assert_eq!(
        app.apply_batch(&[entry(2, 1, bytes), entry(4, 2, data(10, 3))]),
        Err(ApplicationError::IndexGap)
    );
    assert_eq!(app.checkpoint(100000).unwrap(), before);
    assert!(encode_routed(
        hint(10),
        &vec![10; MAX_ROUTING_KEY_BYTES + 1],
        &[],
        MAX_ROUTED_COMMAND_BYTES
    )
    .is_err());
    assert!(encode_routed(
        hint(10),
        &[10],
        &vec![0; MAX_ROUTED_PAYLOAD_BYTES + 1],
        MAX_ROUTED_COMMAND_BYTES
    )
    .is_err());
    let invalid = encode_routed(hint(10), &[10], &[0; 9], 1000).unwrap();
    assert!(app.apply_batch(&[entry(2, 1, invalid)]).is_err());
    let mut fresh = fresh();
    let binding = fresh.bootstrap_command(MAX_ROUTED_COMMAND_BYTES).unwrap();
    for end in 0..binding.len() {
        assert!(fresh
            .apply_batch(&[entry(1, 10000, binding[..end].to_vec())])
            .is_err());
        assert_eq!(fresh.applied_index(), 0);
    }
}

#[test]
fn semantic_and_inner_capacity_reservations_preserve_control_space() {
    let mut config = limits();
    config.operations = 1;
    config.semantic_bytes = 9;
    let mut app = RoutedApplication::new(
        group(20),
        grant(),
        Counter::new(64).unwrap(),
        HostPolicy,
        config,
    )
    .unwrap_or_else(|r| panic!("{:?}", r.error));
    let binding = app.bootstrap_command(MAX_ROUTED_COMMAND_BYTES).unwrap();
    app.apply_batch(&[entry(1, 10000, binding)]).unwrap();
    let bytes = data(10, 7);
    assert!(app
        .validate_proposal(
            OperationId::new(1).unwrap(),
            &bytes,
            [(OperationId::new(1).unwrap(), bytes.as_slice())].into_iter()
        )
        .is_ok());
    assert_eq!(
        app.validate_proposal(
            OperationId::new(2).unwrap(),
            &bytes,
            [(OperationId::new(1).unwrap(), bytes.as_slice())].into_iter()
        ),
        Err(ApplicationError::DedupCapacity)
    );
    app.apply_batch(&[entry(2, 1, bytes.clone())]).unwrap();
    assert!(app
        .validate_proposal(OperationId::new(1).unwrap(), &bytes, std::iter::empty())
        .is_ok());
    assert_eq!(
        app.validate_proposal(OperationId::new(2).unwrap(), &bytes, std::iter::empty()),
        Err(ApplicationError::DedupCapacity)
    );
    let before = app.checkpoint(100000).unwrap();
    assert_eq!(
        app.apply_batch(&[entry(3, 2, bytes)]),
        Err(ApplicationError::DedupCapacity)
    );
    assert_eq!(app.checkpoint(100000).unwrap(), before);
    let fence = encode_fence(grant().input().epoch);
    app.validate_proposal(OperationId::new(99).unwrap(), &fence, std::iter::empty())
        .unwrap();
    app.apply_batch(&[entry(3, 99, fence)]).unwrap();
    assert!(app.fence().is_some());
}

#[test]
fn checkpoint_preserves_semantic_bindings_and_rejects_every_truncation_atomically() {
    let mut app = initialized();
    app.apply_batch(&[
        entry(2, 5, data(10, 7)),
        entry(3, 2, data(11, 3)),
        entry(4, 5, data(10, 7)),
    ])
    .unwrap();
    let image = app.checkpoint(100000).unwrap();
    assert_eq!(app.checkpoint(image.len()).unwrap(), image);
    assert!(app.checkpoint(image.len() - 1).is_err());
    let mut restored = fresh();
    restored.restore_checkpoint(1, 4, &image).unwrap();
    assert_eq!(restored.checkpoint(image.len()).unwrap(), image);
    assert_eq!(
        restored.apply_batch(&[entry(5, 5, data(11, 7))]).unwrap()[0].outcome,
        RoutedOutcome::OperationConflict
    );
    assert!(matches!(
        restored.apply_batch(&[entry(6, 5, data(10, 7))]).unwrap()[0].outcome,
        RoutedOutcome::Applied(CounterReceipt {
            outcome: CounterOutcome::Value(7),
            duplicate: true,
            ..
        })
    ));
    assert_eq!(value(&restored), 10);
    let stable = restored.checkpoint(100000).unwrap();
    for end in 0..image.len() {
        assert_eq!(
            restored.restore_checkpoint(1, 4, &image[..end]),
            Err(ApplicationError::InvalidCheckpoint)
        );
        assert_eq!(restored.checkpoint(100000).unwrap(), stable);
    }
    assert_eq!(
        restored.restore_checkpoint(2, 4, &image),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert!(restored.restore_checkpoint(1, 3, &image).is_err());
    assert!(image.len() <= app.readiness_requirements().snapshot_bytes);
}

#[cfg(feature = "tls")]
#[path = "transfer_source/fixtures.rs"]
pub mod source_fixture;
