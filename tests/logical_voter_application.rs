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
//! Downstream public application contracts; inspection data grants no authority.
use std::{collections::BTreeMap, mem::size_of};
use voteboat::{
    application::*,
    identity::*,
    log::*,
    log_format::NativeLogCodec,
    logical_voter::*,
    quorum::{Limits as PolicyLimits, Policy, Tree},
    snapshot_format::NativeSnapshotCodec,
    wire::WireLimits,
    wire_format::NativeWireCodec,
};
#[path = "logical_voter_application/candidate.rs"]
mod candidate;
fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn store(id: u128) -> StoreIdentity {
    StoreIdentity {
        id: StoreId::new(id).unwrap(),
        incarnation: StoreIncarnation::new(1).unwrap(),
    }
}
fn bootstrap(group: GroupIdentity, node: u64, store: StoreIdentity) -> Bootstrap {
    let node = NodeId::new(node).unwrap();
    Bootstrap {
        group,
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(Tree::Voter(node), PolicyLimits::default()).unwrap(),
        voter_stores: BTreeMap::from([(node, store)]),
    }
}
fn app(limits: LogicalLimits) -> LogicalApplication {
    LogicalApplication::new(
        mapping(),
        bootstrap(group(2), 2, store(2)),
        bootstrap(group(1), 1, store(1)),
        limits,
        NativeLogCodec,
        NativeSnapshotCodec,
        NativeWireCodec::with_membership(WireLimits::default()).unwrap(),
    )
    .unwrap()
}
fn mapping() -> LogicalStoreMapping {
    LogicalStoreMapping::new(group(1), NodeId::new(1).unwrap(), store(1), group(2)).unwrap()
}
#[test]
fn public_bindings_and_lifetime_deployment_envelopes() {
    let limits = LogicalLimits::default();
    let a = app(limits);
    assert_eq!(a.mapping(), mapping());
    assert_eq!(a.child_bootstrap().group, group(2));
    assert_eq!(a.parent_bootstrap().group, group(1));
    assert!(a.validate_group(group(2)).is_ok());
    assert!(a.validate_group(group(1)).is_err());
    let requirements = a.deployment_requirements().unwrap();
    assert_eq!(requirements.application_schema, LOGICAL_APPLICATION_SCHEMA);
    assert_eq!(requirements.command_bytes, limits.max_command_bytes);
    assert_eq!(
        requirements.snapshot_bytes,
        limits.max_child_checkpoint_bytes
    );
    assert!(a.validate_deployment_requirements(requirements).is_ok());
    assert!(a
        .validate_deployment_requirements(voteboat::raft::ReadinessRequirements {
            command_bytes: requirements.command_bytes - 1,
            ..requirements
        })
        .is_err());
    assert!(
        LogicalStoreMapping::new(group(1), NodeId::new(1).unwrap(), store(1), group(1)).is_err()
    );
    assert!(LogicalLimits {
        parent_log: LogLimits {
            max_groups: 2,
            ..limits.parent_log
        },
        ..limits
    }
    .validate()
    .is_err());
}
#[test]
fn inspection_reads_are_pure_and_bounds_are_stable_before_initialization() {
    let mut a = app(LogicalLimits::default());
    let bounds = [
        a.read_result_bound(&LogicalQuery::Image).unwrap(),
        a.read_result_bound(&LogicalQuery::Metadata).unwrap(),
    ];
    let bytes = a.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        a.read_at(0, LogicalQuery::Image).unwrap(),
        LogicalRead::Uninitialized
    );
    let result = a.read_at(0, LogicalQuery::Metadata).unwrap();
    assert_eq!(
        a.read_result_bytes(&result, usize::MAX).unwrap(),
        size_of::<LogicalMetadata>()
    );
    assert_eq!(
        bounds[1],
        size_of::<LogicalRead>() + size_of::<LogicalMetadata>()
    );
    assert!(bounds[0] > bounds[1]);
    assert_eq!(a.checkpoint(usize::MAX).unwrap(), bytes);
    assert_eq!(
        a.read_at(1, LogicalQuery::Metadata),
        Err(ApplicationError::NotApplied)
    );
    a.apply_batch(&[LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Noop,
    }])
    .unwrap();
    assert_eq!(
        a.read_result_bound(&LogicalQuery::Image).unwrap(),
        bounds[0]
    );
    assert_eq!(
        a.read_result_bound(&LogicalQuery::Metadata).unwrap(),
        bounds[1]
    );
    assert_eq!(a.metadata().child_applied, 1);
    assert_eq!(a.metadata().representative, None);
}
#[test]
fn checkpoints_require_original_configuration_and_restore_atomically() {
    let limits = LogicalLimits::default();
    let mut a = app(limits);
    a.apply_batch(&[LogEntry {
        index: 1,
        term: 1,
        payload: EntryPayload::Noop,
    }])
    .unwrap();
    let bytes = a.checkpoint(usize::MAX).unwrap();
    let mut restored = app(limits);
    restored
        .restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 1, &bytes)
        .unwrap();
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), bytes);
    let old = restored.checkpoint(usize::MAX).unwrap();
    for cut in 0..bytes.len() {
        assert!(restored.restore_checkpoint(1, 1, &bytes[..cut]).is_err());
        assert_eq!(restored.checkpoint(usize::MAX).unwrap(), old);
    }
    assert_eq!(
        restored.restore_checkpoint(2, 1, &bytes),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert!(restored.restore_checkpoint(1, 2, &bytes).is_err());
    let mut changed = app(LogicalLimits {
        max_actions: limits.max_actions + 1,
        ..limits
    });
    assert!(!changed.same_configuration(&a));
    assert!(changed.restore_checkpoint(1, 1, &bytes).is_err());
    assert_eq!(changed.applied_index(), 0);
}
#[test]
fn malformed_batches_and_untrusted_business_intake_leave_state_intact() {
    let mut a = app(LogicalLimits::default());
    let before = a.checkpoint(usize::MAX).unwrap();
    let invalid = LogEntry {
        index: 2,
        term: 1,
        payload: EntryPayload::Noop,
    };
    assert_eq!(a.apply_batch(&[invalid]), Err(ApplicationError::IndexGap));
    let operation = OperationId::new(1).unwrap();
    assert_eq!(
        a.validate_proposal(operation, b"boolean-ready", std::iter::empty()),
        Err(ApplicationError::InvalidCommand)
    );
    let command = LogEntry {
        index: 2,
        term: 1,
        payload: EntryPayload::Command {
            operation,
            bytes: b"ready".to_vec(),
        },
    };
    let entries = [
        LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Noop,
        },
        command,
    ];
    assert_eq!(
        a.receipt_bytes_bound(&entries).unwrap(),
        size_of::<LogicalReceipt>()
    );
    assert_eq!(
        a.apply_batch(&entries),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(a.checkpoint(usize::MAX).unwrap(), before);
}

#[test]
fn selected_actual_activation_profile_pins_configuration_and_refuses_rebinding() {
    let limits = LogicalLimits::default();
    let wrapper = LogicalActivationApplication::new(
        group(1),
        vec![mapping()],
        LogicalActivationLimits {
            command_bytes: 4096,
            ..LogicalActivationLimits::default()
        },
        Counter::new(16).unwrap(),
    )
    .unwrap();
    let profile = wrapper.control_profile();
    let ordinary = app(limits);
    assert!(ordinary.activation_control_profile().is_none());
    let selected = ordinary
        .clone()
        .with_activation_control_profile(profile.clone())
        .unwrap();
    assert_eq!(selected.activation_control_profile(), Some(&profile));
    assert!(
        voteboat::runtime::LogicalResponseCodec::logical_activation_control_profile(&selected)
            .is_none()
    );
    assert!(!selected.same_configuration(&ordinary));
    assert!(selected
        .clone()
        .with_activation_control_profile(profile.clone())
        .is_err());
    let bytes = selected.checkpoint(usize::MAX).unwrap();
    let mut restored = app(limits)
        .with_activation_control_profile(profile.clone())
        .unwrap();
    restored
        .restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 0, &bytes)
        .unwrap();
    assert_eq!(restored.checkpoint(usize::MAX).unwrap(), bytes);
    let mut ordinary = ordinary;
    let before = ordinary.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        ordinary.restore_checkpoint(LOGICAL_APPLICATION_SCHEMA, 0, &bytes),
        Err(ApplicationError::InvalidCheckpoint)
    );
    assert_eq!(ordinary.checkpoint(usize::MAX).unwrap(), before);
    ordinary
        .apply_batch(&[LogEntry {
            index: 1,
            term: 1,
            payload: EntryPayload::Noop,
        }])
        .unwrap();
    assert!(ordinary
        .with_activation_control_profile(profile.clone())
        .is_err());
    assert!(app(LogicalLimits {
        control_action_bytes: 512 * 1024 - 1,
        ..limits
    })
    .with_activation_control_profile(profile)
    .is_err());
    let foreign = LogicalActivationApplication::new(
        group(3),
        vec![
            LogicalStoreMapping::new(group(3), NodeId::new(1).unwrap(), store(1), group(4))
                .unwrap(),
        ],
        LogicalActivationLimits {
            command_bytes: 4096,
            ..LogicalActivationLimits::default()
        },
        Counter::new(16).unwrap(),
    )
    .unwrap();
    assert!(app(limits)
        .with_activation_control_profile(foreign.control_profile())
        .is_err());
}
