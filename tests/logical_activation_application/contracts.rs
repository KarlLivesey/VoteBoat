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
use super::*;
#[test]
fn original_business_content_retry_and_contiguous_control_positions() {
    let mut a = app();
    let business = a.business(&Counter::command(0, 7)).unwrap();
    assert!(a
        .validate_proposal(business_op(), &business, std::iter::empty())
        .is_ok());
    let receipts = a
        .apply_batch(&[
            entry(1, business_op(), business.clone()),
            entry(2, begin_op(), a.begin(&intent()).unwrap()),
            entry(3, business_op(), business),
        ])
        .unwrap();
    let ActivationReceipt::Business(first) = receipts[0] else {
        panic!("business")
    };
    let ActivationReceipt::Business(retry) = receipts[2] else {
        panic!("business retry")
    };
    assert_eq!(first.operation, business_op());
    assert_eq!(first.outcome, CounterOutcome::Value(7));
    assert!(retry.duplicate);
    assert_eq!(a.inner().applied_index(), 3);
    assert_eq!(a.applied_index(), 3);
    assert_eq!(a.active(), Some(begin_op()));
    assert_eq!(
        a.read_at(3, ActivationQuery::Business(())).unwrap(),
        ActivationRead::Business(7)
    );
    let bytes = a.checkpoint(usize::MAX).unwrap();
    let mut reopened = app();
    reopened
        .restore_checkpoint(a.schema_version(), 3, &bytes)
        .unwrap();
    assert_eq!(reopened.checkpoint(usize::MAX).unwrap(), bytes);
    let status = reopened.status(begin_op()).unwrap();
    assert_eq!(status.begin.first, ActivationPosition { index: 2, term: 1 });
    let suffix = entry(
        4,
        business_op(),
        reopened.business(&Counter::command(0, 7)).unwrap(),
    );
    let receipt = reopened.apply_batch(&[suffix]).unwrap();
    assert!(matches!(
        &receipt[0],
        ActivationReceipt::Business(CounterReceipt {
            duplicate: true,
            ..
        })
    ));
    assert_eq!(
        reopened.read_at(4, ActivationQuery::Business(())).unwrap(),
        ActivationRead::Business(7)
    );
}
#[test]
fn bounded_receipts_reads_and_faithful_response_tags() {
    let mut a = app();
    let entries = [
        entry(1, begin_op(), a.begin(&intent()).unwrap()),
        entry(
            2,
            business_op(),
            a.business(&Counter::command(0, 3)).unwrap(),
        ),
    ];
    let receipt_bound = a.receipt_bytes_bound(&entries).unwrap();
    let encoded_bound = a.receipt_encoding_bound(&entries).unwrap();
    let receipts = a.apply_batch(&entries).unwrap();
    assert!(receipts.capacity() * size_of::<ActivationReceipt<CounterReceipt>>() <= receipt_bound);
    for r in &receipts {
        assert_eq!(r.nested_bytes(0).unwrap(), 0);
        let mut out = vec![0; encoded_bound];
        let n = a.encode_receipt(r, &mut out).unwrap();
        assert!(n <= encoded_bound);
        assert_eq!(out[0], u8::from(matches!(r, ActivationReceipt::Control(_))));
    }
    assert_eq!(
        a.logical_response_schema(),
        LOGICAL_ACTIVATION_RESPONSE_SCHEMA
    );
    for q in [
        ActivationQuery::Business(()),
        ActivationQuery::Status(begin_op()),
        ActivationQuery::Status(op(99)),
        ActivationQuery::Active,
    ] {
        let query_bound = a.query_encoding_bound(&q).unwrap();
        let read_bound = a.read_encoding_bound(&q).unwrap();
        let result_bound = a.read_result_bound(&q).unwrap();
        assert_eq!(a.query_bytes(&q, 0).unwrap(), 0);
        let mut out = vec![0; query_bound];
        assert!(a.encode_query(&q, &mut out).unwrap() <= query_bound);
        let read = a.read_at(2, q).unwrap();
        let nested = a.read_result_bytes(&read, usize::MAX).unwrap();
        assert!(size_of::<ActivationRead<i64>>() + nested <= result_bound);
        let mut out = vec![0; read_bound];
        assert!(a.encode_read(&read, &mut out).unwrap() <= read_bound);
        if nested > 0 {
            assert_eq!(
                a.read_result_bytes(&read, nested - 1),
                Err(ApplicationError::ReceiptBudget)
            );
        }
    }
    assert_eq!(
        a.read_at(3, ActivationQuery::Active),
        Err(ApplicationError::NotApplied)
    );
    assert!(a.encode_receipt(&receipts[0], &mut [0; 1]).is_err());
    assert!(a
        .encode_read(&ActivationRead::Business(3), &mut [0; 1])
        .is_err());
}
#[test]
fn canonical_status_and_active_queries_have_complete_distinct_envelopes() {
    let a = app();
    for id in [1, 99, u128::MAX] {
        let q = ActivationQuery::Status(op(id));
        let mut encoded = vec![0; a.query_encoding_bound(&q).unwrap()];
        assert_eq!(a.encode_query(&q, &mut encoded).unwrap(), 17);
        let mut expected = vec![1];
        expected.extend(id.to_le_bytes());
        assert_eq!(encoded, expected);
        assert!(a.encode_query(&q, &mut [0; 16]).is_err());
    }
    for (q, expected) in [
        (ActivationQuery::Active, vec![2]),
        (ActivationQuery::Business(()), vec![0]),
    ] {
        let mut encoded = vec![0; a.query_encoding_bound(&q).unwrap()];
        assert_eq!(a.encode_query(&q, &mut encoded).unwrap(), expected.len());
        assert_eq!(encoded, expected);
        assert!(a.encode_query(&q, &mut []).is_err());
    }
}
#[test]
fn explicit_schema_mapping_and_lifetime_envelopes() {
    let mut a = app();
    assert_eq!(
        a.schema_version(),
        logical_activation_schema(a.inner().schema_version()).unwrap()
    );
    assert!(a.validate_group(group(1)).is_ok());
    assert!(a.validate_group(group(2)).is_err());
    let deployment = a.deployment_requirements().unwrap();
    assert_eq!(deployment.application_schema, a.schema_version());
    assert!(a.validate_deployment_requirements(deployment).is_ok());
    assert!(a
        .validate_deployment_requirements(voteboat::raft::ReadinessRequirements {
            command_bytes: deployment.command_bytes - 1,
            ..deployment
        })
        .is_err());
    let inner = App::new();
    let snapshot = inner.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        a.restore_checkpoint(inner.schema_version(), 0, &snapshot),
        Err(ApplicationError::UnsupportedSchema)
    );
    assert!(LogicalActivationApplication::new(
        group(1),
        vec![mapping(), mapping()],
        LogicalActivationLimits::default(),
        App::new()
    )
    .is_err());
    let mut wrong = intent().mapping();
    wrong.parent.incarnation = GroupIncarnation::new(2).unwrap();
    assert!(LogicalActivationApplication::new(
        group(1),
        vec![wrong],
        LogicalActivationLimits::default(),
        App::new()
    )
    .is_err());
    assert!(LogicalActivationLimits {
        intents: usize::MAX,
        ..a.limits()
    }
    .validate()
    .is_err());
    assert!(LogicalActivationLimits {
        checkpoint_bytes: 1024,
        ..a.limits()
    }
    .validate()
    .is_err());
    assert!(a
        .validate_proposal(op(100), &7i64.to_le_bytes(), std::iter::empty())
        .is_err());
}

#[test]
fn selected_inner_context_is_forwarded_without_freezing_parent_configuration() {
    // Own recovered core supplies group/context only; this does not claim a
    // committed membership transition or a child activation authority.
    let a = app();
    let configuration = config([1, 2, 3], 9);
    let bootstrap = Bootstrap {
        group: group(1),
        configuration: configuration.id(),
        policy: configuration.policy().clone(),
        voter_stores: configuration.voter_stores().clone(),
    };
    let mut groups = BTreeMap::new();
    voteboat::log::apply_batch(
        &mut groups,
        &[LogMutation::Create(bootstrap.clone())],
        LogLimits::default(),
    )
    .unwrap();
    let core = voteboat::raft::Raft::recover(
        NodeId::new(1).unwrap(),
        StoreBinding {
            identity: store(1),
            session: StoreSession::new(1).unwrap(),
        },
        groups.remove(&group(1)).unwrap(),
        LogLimits::default(),
    )
    .unwrap();
    let bytes = a.business(&Counter::command(0, 7)).unwrap();
    assert!(a
        .validate_proposal_context(&core, business_op(), &bytes)
        .is_ok());
    assert_eq!(
        a.validate_proposal_context(&core, Counter::operation(1, 9).unwrap(), &bytes),
        Err(ApplicationError::InvalidCommand)
    );
    assert!(a
        .validate_proposal_context(&core, begin_op(), &a.begin(&intent()).unwrap())
        .is_ok());
    assert_eq!(
        core.membership().stable().id(),
        ConfigurationId::new(9).unwrap()
    );
}

#[test]
fn opaque_control_profile_is_derived_from_exact_actual_wrapper_configuration() {
    let a = app();
    let profile = a.control_profile();
    assert_eq!(
        a.logical_activation_control_profile(),
        Some(profile.clone())
    );
    assert_eq!(profile.parent(), a.parent());
    assert_eq!(profile.mappings(), a.mappings());
    assert_eq!(profile.limits(), a.limits());
    assert_eq!(profile.inner_schema(), a.inner().schema_version());
    assert_eq!(profile.application_schema(), a.schema_version());
    assert_eq!(
        profile.response_schema(),
        LOGICAL_ACTIVATION_RESPONSE_SCHEMA
    );
    assert_eq!(profile.format_version(), 2);
    let changed = LogicalActivationApplication::new(
        a.parent(),
        a.mappings().to_vec(),
        LogicalActivationLimits {
            intents: a.limits().intents + 1,
            ..a.limits()
        },
        App::new(),
    )
    .unwrap();
    assert_ne!(profile, changed.control_profile());
    let original = a.checkpoint(usize::MAX).unwrap();
    let mut restored = app();
    restored
        .restore_checkpoint(a.schema_version(), 0, &original)
        .unwrap();
    assert_eq!(profile, restored.control_profile());
}

#[test]
fn public_operation_space_split_refuses_tagged_business_writes() {
    let mut a = app();
    let tagged = OperationId::new((u128::from(ACTIVATION_OPERATION_TAG) << 64) | 77).unwrap();
    assert!(ChildActivationOperations::is_reserved(tagged));
    assert!(!ChildActivationOperations::is_reserved(op(100)));
    assert!(ChildActivationOperations::reserved(1)
        .unwrap()
        .begin
        .get()
        .eq(&((u128::from(ACTIVATION_OPERATION_TAG) << 64) | 8)));
    let business = a.business(&Counter::command(0, 7)).unwrap();
    assert_eq!(
        a.validate_proposal(tagged, &business, std::iter::empty()),
        Err(ApplicationError::InvalidCommand)
    );
    let before = a.checkpoint(usize::MAX).unwrap();
    assert_eq!(
        a.apply_batch(&[entry(1, tagged, business.clone())]).err(),
        Some(ApplicationError::InvalidCommand)
    );
    assert_eq!(a.checkpoint(usize::MAX).unwrap(), before);
    // Hand-built untagged phase IDs are not an intent.
    let untagged = ChildActivationOperations {
        begin: op(10),
        learners: op(11),
        membership: op(12),
        outcome: op(13),
        release: op(14),
    };
    assert!(ChildActivationIntent::new(
        mapping(),
        untagged,
        &config([1, 2, 3], 1),
        ConfigurationId::new(2).unwrap(),
        ConfigurationId::new(3).unwrap(),
        &config([1, 2, 4], 4),
    )
    .is_err());
    assert!(a
        .validate_proposal(business_op(), &business, std::iter::empty())
        .is_ok());
}
