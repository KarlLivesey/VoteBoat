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
//! The parent's operation-ID space split between activation phases and
//! business writes, and the limits left once no business ID is retained.
use super::*;
const TAG: u128 = ACTIVATION_OPERATION_TAG as u128;
fn tagged(low: u64) -> OperationId {
    op((TAG << 64) | u128::from(low))
}
#[test]
fn reserved_layout_is_exact_and_bounded() {
    let first = ops(0);
    assert_eq!(first.ids(), [0, 1, 2, 3, 4].map(tagged));
    let last = ops((1 << 61) - 1);
    assert_eq!(last.begin, tagged(u64::MAX - 7));
    assert_eq!(last.release, tagged(u64::MAX - 3));
    assert_eq!(
        ChildActivationOperations::reserved(1 << 61),
        Err(ApplicationError::InvalidCommand)
    );
    assert_eq!(
        ChildActivationOperations::reserved(u64::MAX),
        Err(ApplicationError::InvalidCommand)
    );
    // Distinct activation numbers never share a phase ID.
    for (a, b) in [(0, 1), (1, 2), (7, 8), (0, (1 << 61) - 1)] {
        let (a, b) = (ops(a).ids(), ops(b).ids());
        assert!(a.iter().all(|id| !b.contains(id)));
    }
    assert_eq!(
        ChildActivationOperations::of_begin(ops(9).begin),
        Some(ops(9))
    );
    for id in [ops(9).learners, ops(9).release, tagged(8 * 9 + 5), op(72)] {
        assert_eq!(ChildActivationOperations::of_begin(id), None);
    }
}
#[test]
fn every_phase_id_is_reserved_and_no_business_id_is() {
    for n in [0, 1, 2, 1000, (1 << 61) - 1] {
        assert!(ops(n)
            .ids()
            .into_iter()
            .all(ChildActivationOperations::is_reserved));
    }
    // The whole tagged half-plane is reserved, including unused low slots.
    assert!(ChildActivationOperations::is_reserved(tagged(5)));
    assert!(ChildActivationOperations::is_reserved(tagged(u64::MAX)));
    for id in [
        1,
        100,
        u128::from(u64::MAX),
        (7 << 64) | 1,
        (TAG - 1) << 64 | u128::from(u64::MAX),
        (TAG + 1) << 64,
        u128::MAX,
    ] {
        assert!(!ChildActivationOperations::is_reserved(op(id)), "{id:#x}");
    }
}
#[test]
fn intents_require_the_exact_reserved_layout() {
    let build = |operations| {
        ChildActivationIntent::new(
            mapping(),
            operations,
            &config([1, 2, 3], 1),
            ConfigurationId::new(2).unwrap(),
            ConfigurationId::new(3).unwrap(),
            &config([1, 2, 4], 4),
        )
    };
    assert!(build(ops(3)).is_ok());
    let untagged = ChildActivationOperations {
        begin: op(10),
        learners: op(11),
        membership: op(12),
        outcome: op(13),
        release: op(14),
    };
    assert_eq!(
        build(untagged).err(),
        Some(ApplicationError::InvalidCommand)
    );
    // Tagged but not the layout of one activation number.
    let mixed = ChildActivationOperations {
        learners: ops(4).learners,
        ..ops(3)
    };
    let swapped = ChildActivationOperations {
        outcome: ops(3).release,
        release: ops(3).outcome,
        ..ops(3)
    };
    let shifted = ChildActivationOperations {
        begin: tagged(8 * 3 + 1),
        ..ops(3)
    };
    let one_untagged = ChildActivationOperations {
        release: op(8 * 3 + 4),
        ..ops(3)
    };
    for operations in [mixed, swapped, shifted, one_untagged] {
        assert_eq!(
            build(operations).err(),
            Some(ApplicationError::InvalidCommand)
        );
    }
}
#[test]
fn decoded_begin_with_untagged_phase_ids_is_invalid() {
    let a = app(2);
    let bytes = a.begin(&intent(1)).unwrap();
    assert!(matches!(
        codec::decode(&bytes, CONTROL_BYTES),
        Ok(codec::Envelope::Control(ControlCommand::Begin(_)))
    ));
    // Phase IDs follow the 8-byte magic, the kind byte and the 80-byte mapping;
    // the high 64 bits of each little-endian u128 are its last 8 bytes.
    for phase in 0..5 {
        let high = 8 + 1 + 80 + 16 * phase + 8;
        let mut changed = bytes.clone();
        changed[high..high + 8].fill(0);
        assert_eq!(
            codec::decode(&changed, CONTROL_BYTES).err(),
            Some(ApplicationError::InvalidCommand)
        );
        assert_eq!(
            a.validate_proposal(ops(1).begin, &changed, std::iter::empty()),
            Err(ApplicationError::InvalidCommand)
        );
        assert!(a
            .control_profile()
            .classify_command(ops(1).begin, &changed)
            .is_err());
    }
}
#[test]
fn business_then_begin_and_begin_then_business_keep_both_meanings() {
    let business = app(2).business(&Counter::command(0, 5)).unwrap();
    let i = intent(1);
    for business_first in [true, false] {
        let mut a = app(2);
        let begin_bytes = a.begin(&i).unwrap();
        let mut log = vec![
            (business_op(1), business.clone()),
            (i.operations.begin, begin_bytes),
        ];
        if !business_first {
            log.reverse();
        }
        for (k, (id, bytes)) in log.into_iter().enumerate() {
            assert!(a.validate_proposal(id, &bytes, std::iter::empty()).is_ok());
            a.apply_batch(&[entry(k as u64 + 1, id, bytes)]).unwrap();
        }
        // A lost-receipt retry of the business write still reaches the inner
        // application and gets its original outcome.
        assert!(a
            .validate_proposal(business_op(1), &business, std::iter::empty())
            .is_ok());
        let retry = a
            .apply_batch(&[entry(3, business_op(1), business.clone())])
            .unwrap();
        assert!(matches!(
            retry[0],
            ActivationReceipt::Business(CounterReceipt {
                outcome: CounterOutcome::Value(5),
                duplicate: true,
                ..
            })
        ));
        let status = a.status(i.operations.begin).unwrap();
        assert_eq!(status.begin.outcome, ChildActivationOutcome::Begun);
        assert_eq!(status.begin.first.index, if business_first { 2 } else { 1 });
        assert_eq!(a.active(), Some(i.operations.begin));
        assert_eq!(a.inner.read_at(3, ()).unwrap(), 5);
    }
}
#[test]
fn limits_carry_no_business_bound() {
    let limits = LogicalActivationLimits::default();
    assert_eq!(limits.validate(), Ok(limits));
    assert_eq!(
        limits.control_checkpoint_bound(),
        Ok(64 * (3 * (CONTROL_BYTES + 16) + 128) + 2048)
    );
    for bad in [
        LogicalActivationLimits {
            intents: 0,
            ..limits
        },
        LogicalActivationLimits {
            intents: 16385,
            ..limits
        },
        LogicalActivationLimits {
            command_bytes: CONTROL_BYTES - 1,
            ..limits
        },
        LogicalActivationLimits {
            checkpoint_bytes: 1023,
            ..limits
        },
    ] {
        assert_eq!(bad.validate(), Err(ApplicationError::InvalidCommand));
    }
    // The largest intent budget fits the smallest checkpoint that holds it.
    let max = LogicalActivationLimits {
        intents: 16384,
        checkpoint_bytes: 16384 * (3 * (CONTROL_BYTES + 16) + 128) + 2048,
        ..limits
    };
    assert_eq!(max.validate(), Ok(max));
    assert!(LogicalActivationLimits {
        checkpoint_bytes: max.checkpoint_bytes - 1,
        ..max
    }
    .validate()
    .is_err());
    // The inner application gets the whole budget beyond control state.
    let a = app(2);
    let inner = a.inner.deployment_requirements().unwrap();
    let wrapped = a.deployment_requirements().unwrap();
    assert_eq!(
        wrapped.application_schema,
        LOGICAL_ACTIVATION_SCHEMA ^ inner.application_schema
    );
    assert_eq!(wrapped.application_schema, a.schema_version());
    assert_eq!(wrapped.command_bytes, a.limits().command_bytes);
    assert_eq!(wrapped.snapshot_bytes, a.limits().checkpoint_bytes);
    assert!(
        inner.snapshot_bytes + a.limits().control_checkpoint_bound().unwrap()
            <= wrapped.snapshot_bytes
    );
}

#[test]
fn activation_wrappers_around_counters_with_different_limits_refuse_each_other() {
    // The same activation limits around Counters with windows 16 and 32: the
    // byte envelope is the activation's own, so only the schema can tell the
    // replicas apart, and it carries the inner Counter's limits.
    let wrap = |window| {
        LogicalActivationApplication::new(
            group(1),
            vec![mapping()],
            LogicalActivationLimits::default(),
            Counter::new(window).unwrap(),
        )
        .unwrap()
    };
    let (small, large) = (wrap(16), wrap(32));
    assert_ne!(small.schema_version(), large.schema_version());
    for (replica, leader) in [(&small, &large), (&large, &small)] {
        let declared = leader.deployment_requirements().unwrap();
        assert_eq!(
            declared.snapshot_bytes,
            replica.deployment_requirements().unwrap().snapshot_bytes
        );
        assert_eq!(leader.validate_deployment_requirements(declared), Ok(()));
        assert_eq!(
            replica.validate_deployment_requirements(declared),
            Err(ApplicationError::UnsupportedSchema)
        );
        // A checkpoint of the other replica is refused by schema too.
        let image = leader.checkpoint(usize::MAX).unwrap();
        let mut target = replica.clone();
        assert_eq!(
            target.restore_checkpoint(leader.schema_version(), 0, &image),
            Err(ApplicationError::UnsupportedSchema)
        );
    }
    assert_eq!(
        small.control_profile().application_schema(),
        small.schema_version()
    );
}
