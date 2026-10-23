// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Downstream public-only host: no private root/comparison APIs or native feature.
#[path = "../examples/inprocess_counter_delivery/host.rs"]
pub mod host;
use host::*;
use voteboat::{application::*, embedding::writes::*, identity::*};
fn group(id: u128, incarnation: u64) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(incarnation).unwrap(),
    }
}
fn id(n: u128) -> OperationId {
    OperationId::new(n).unwrap()
}
fn write(
    fleet: &mut Fleet,
    slot: usize,
    port: &mut WritePort<CounterReceipt>,
    ids: &[u128],
    delta: i64,
) -> Vec<CounterReceipt> {
    let bytes = delta.to_le_bytes();
    let inputs: Vec<_> = ids
        .iter()
        .map(|n| WriteInput {
            operation: id(*n),
            bytes: &bytes,
        })
        .collect();
    let (submission, mut invocation) = port.begin(&inputs).unwrap();
    fleet.submit(slot, submission).unwrap();
    fleet.pump().unwrap();
    let mut receipts = Vec::new();
    for (ordinal, expected) in ids.iter().enumerate() {
        let envelope = invocation
            .try_take(ordinal)
            .unwrap()
            .expect("every original delivered");
        assert!(invocation.matches(&envelope, ordinal));
        assert_eq!(envelope.operation(), id(*expected));
        let OriginalOutcome::Applied { position, receipt } = envelope.into_outcome() else {
            panic!("real Counter original was not Applied");
        };
        assert_eq!(position.index, receipt.index);
        assert!(position.term > 0);
        assert_eq!(receipt.operation, id(*expected));
        receipts.push(receipt);
    }
    invocation.finish().unwrap();
    receipts
}
#[test]
fn public_counter_sparse_retries_conflicts_overflow_and_owned_control_keep_all_three_results() {
    let mut fleet = Fleet::new();
    let (slot, factory) = fleet.add_group(group(11, 1)).unwrap();
    let (owned, _) = fleet.add_group(group(29, 1)).unwrap();
    let mut port = factory.open_port(port_limits(7).unwrap()).unwrap();
    let sequence = [
        (u128::MAX - 9, 7),
        (19, -3),
        (1u128 << 113, 2),
        (19, -3),
        (19, 8),
        (901, i64::MAX),
        (902, -6),
    ];
    for (operation, delta) in sequence {
        let actual = write(&mut fleet, slot, &mut port, &[operation], delta).remove(0);
        fleet
            .write_owned(owned, id(operation), delta.to_le_bytes().to_vec())
            .unwrap();
        fleet.pump().unwrap();
        let expected = fleet.take_owned(owned).expect("owned API result");
        assert_eq!(actual, expected);
    }
    assert_eq!(fleet.read(slot).unwrap(), fleet.read(owned).unwrap());
    for evidence in fleet.evidence(slot) {
        assert_eq!(evidence[0], 8);
        assert_eq!(evidence[1], 8);
        assert_eq!(evidence[5], 7);
    }
    assert_eq!(
        write(&mut fleet, slot, &mut port, &[19], -3)[0].outcome,
        CounterOutcome::Value(4)
    );
    assert_eq!(
        write(&mut fleet, slot, &mut port, &[19], 99)[0].outcome,
        CounterOutcome::OperationConflict
    );
    assert_eq!(
        write(&mut fleet, slot, &mut port, &[901], i64::MAX)[0].outcome,
        CounterOutcome::Overflow
    );
}
#[test]
fn independent_domains_share_the_real_driver_and_one_close_does_not_close_the_other() {
    let mut fleet = Fleet::new();
    let (a, factory_a) = fleet.add_group(group(11, 1)).unwrap();
    let (b, factory_b) = fleet.add_group(group(29, 1)).unwrap();
    let mut pa = factory_a.open_port(port_limits(2).unwrap()).unwrap();
    let mut pb = factory_b.open_port(port_limits(2).unwrap()).unwrap();
    let bytes = 3i64.to_le_bytes();
    let (sa, mut ia) = pa
        .begin(&[WriteInput {
            operation: id(1u128 << 101),
            bytes: &bytes,
        }])
        .unwrap();
    let (sb, mut ib) = pb
        .begin(&[WriteInput {
            operation: id(u128::MAX),
            bytes: &bytes,
        }])
        .unwrap();
    fleet.submit(a, sa).unwrap();
    fleet.submit(b, sb).unwrap();
    fleet.pump().unwrap();
    assert!(matches!(
        ia.try_take(0).unwrap().unwrap().into_outcome(),
        OriginalOutcome::Applied { .. }
    ));
    assert!(matches!(
        ib.try_take(0).unwrap().unwrap().into_outcome(),
        OriginalOutcome::Applied { .. }
    ));
    ia.finish().unwrap();
    ib.finish().unwrap();
    fleet.close_group(a);
    assert!(matches!(
        factory_a.open_port(port_limits(1).unwrap()),
        Err(WriteError::Closed)
    ));
    assert_eq!(
        write(&mut fleet, b, &mut pb, &[19], -1)[0].outcome,
        CounterOutcome::Value(2)
    );
    // Reuse a live group slot with a fresh caller-authorized incarnation, never
    // silently reconstruct an old group's state from an empty core.
    let (fresh, _) = fleet.add_group(group(11, 2)).unwrap();
    assert_eq!(fresh, a);
    assert_eq!(fleet.read(b).unwrap(), 2);
}
#[test]
fn public_counter_default_historical_checkpoints_and_actual_snapshot_install_resume_port_results() {
    let mut fleet = Fleet::new();
    let (slot, factory) = fleet.add_group(group(71, 1)).unwrap();
    let mut port = factory.open_port(port_limits(64).unwrap()).unwrap();
    for start in (1..=12000u128).step_by(16) {
        let ids: Vec<_> = (start..start + 16).map(|n| (n << 64) | 17).collect();
        assert_eq!(write(&mut fleet, slot, &mut port, &ids, 1).len(), 16);
    }
    for e in fleet.evidence(slot) {
        assert_eq!(e[0], 12001);
        assert_eq!(e[1], 12001);
        assert_eq!(e[3], 2);
        assert!(e[2] >= 9000);
        assert_eq!(e[5], 12000);
    }
    fleet.pause(slot, 2, true);
    for start in (12001..=18000u128).step_by(16) {
        let ids: Vec<_> = (start..start + 16).map(|n| (n << 64) | 17).collect();
        write(&mut fleet, slot, &mut port, &ids, 1);
    }
    assert!(fleet.evidence(slot)[2][1] < 13000);
    fleet.pause(slot, 2, false);
    fleet.heartbeat(slot).unwrap();
    fleet.pump().unwrap();
    assert!(
        fleet.evidence(slot)[2][4] > 0,
        "must install an actual anchored Counter snapshot"
    );
    let after = write(&mut fleet, slot, &mut port, &[u128::MAX, 1u128 << 120], -1);
    assert_eq!(after.len(), 2);
    for e in fleet.evidence(slot) {
        assert_eq!(e[0], 18003);
        assert_eq!(e[1], 18003);
    }
    assert_eq!(fleet.read(slot).unwrap(), 17998);
    assert!(write(&mut fleet, slot, &mut port, &[(1u128 << 64) | 17], 1)[0].duplicate);
}

#[test]
fn downstream_nested_receipt_capacity_and_exact_error_return_are_charged() {
    // Routing/budget control only; synthetic LogEntry is no commit certificate.
    #[derive(Debug)]
    struct Nested {
        bytes: Vec<u8>,
    }
    let binding = WriteBinding {
        group: group(91, 1),
        node: NodeId::new(1).unwrap(),
        store: StoreBinding {
            identity: StoreIdentity {
                id: StoreId::new(17).unwrap(),
                incarnation: StoreIncarnation::new(1).unwrap(),
            },
            session: StoreSession::new(1).unwrap(),
        },
    };
    let (domain, factory) = WriteDomain::new(
        binding,
        WriteDomainLimits {
            ports: 1,
            retained_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    let mut limits = WritePortLimits {
        originals: 1,
        command_bytes: 8,
        command_capacity_bytes: 8,
        receipt_bytes: std::mem::size_of::<Nested>() + 8,
        waker_bytes: 1024,
        retained_bytes: 0,
    };
    limits.retained_bytes = limits.required_bytes::<Nested>().unwrap();
    let mut port = factory.open_port(limits).unwrap();
    for capacity in [8, 9] {
        let bytes = 1i64.to_le_bytes();
        let (mut submission, mut invocation) = port
            .begin(&[WriteInput {
                operation: id(u128::MAX),
                bytes: &bytes,
            }])
            .unwrap();
        let (_, _, mut reply) = submission.take_next().unwrap().unwrap();
        let entry = voteboat::log::LogEntry {
            index: capacity as u64,
            term: 1,
            payload: voteboat::log::EntryPayload::Command {
                operation: id(u128::MAX),
                bytes: bytes.to_vec().into(),
            },
        };
        reply.bind_proposal(&domain, &entry).unwrap();
        let receipt = Nested {
            bytes: vec![1; capacity],
        };
        let pointer = receipt.bytes.as_ptr();
        let result = reply.publish_applied(&domain, &entry, receipt, capacity);
        if capacity == 8 {
            assert!(result.is_ok());
            let OriginalOutcome::Applied { receipt, .. } =
                invocation.try_take(0).unwrap().unwrap().into_outcome()
            else {
                panic!("nested R must remain individual");
            };
            assert_eq!(receipt.bytes.as_ptr(), pointer);
        } else {
            let rejected = result.unwrap_err();
            assert_eq!(rejected.reason, WriteError::ProviderViolation);
            assert_eq!(rejected.receipt.bytes.as_ptr(), pointer);
            assert_eq!(rejected.receipt.bytes.capacity(), capacity);
            assert!(matches!(
                invocation.try_take(0).unwrap().unwrap().into_outcome(),
                OriginalOutcome::Unknown(_)
            ));
        }
        drop(submission);
        invocation.finish().unwrap();
        port.reap().unwrap();
    }
}
