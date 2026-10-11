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
//! Downstream driver composition through actual public Node completions.
//! Host storage/scheduler/worker providers are shared with this test target.
//! These single-voter histories are not native network quorum qualification.
use super::*;
use voteboat::transaction::*;
#[path = "transaction_driver/fixture.rs"]
mod fixture;
use fixture::*;

type Driver<'a> = TransferDriver<
    'a,
    Ready,
    Timers,
    Entropy,
    HostWorker,
    support::outbound::HostOutbound,
    snapshot_routes::Worker,
    peer_driver::Connector,
    peer_driver::Factory,
>;

fn driver(nodes: &mut [HostNode], generation: u64, capacity: usize) -> Driver<'_> {
    TransferDriver::new(
        profile(),
        TransferDriverGeneration::new(generation).unwrap(),
        TransferDriverLimits::new(capacity).unwrap(),
        nodes,
    )
    .unwrap()
}
fn outcome(d: &mut Driver<'_>, ticket: DriverTicket) -> DriverOutcome {
    for _ in 0..1000 {
        d.advance(MonoTime(0), DriverBudget::default()).unwrap();
        if let Some(completion) = d.poll_completion() {
            assert_eq!(completion.ticket(), ticket);
            return d.complete(completion).unwrap();
        }
    }
    panic!("host transaction did not finish");
}
fn drain(d: &mut Driver<'_>) {
    for _ in 0..1000 {
        if d.is_drained() {
            return;
        }
        d.advance(MonoTime(0), DriverBudget::default()).unwrap();
    }
    panic!("driver credits did not drain");
}
fn transfer(nodes: &mut [HostNode], generation: u64, spec: TransferSpec) -> DriverOutcome {
    let mut d = driver(nodes, generation, 1);
    let ticket = d.submit(spec).unwrap();
    let result = outcome(&mut d, ticket);
    d.close();
    drain(&mut d);
    result
}

#[test]
fn host_components_commit_reverse_and_retry_without_native_providers() {
    let mut nodes = nodes();
    let first = transfer(&mut nodes, 1, spec(1, 7, false));
    assert!(matches!(
        first,
        DriverOutcome::Finished {
            decision: TransferDecision::Commit,
            ..
        }
    ));
    assert_eq!(
        balances(&nodes),
        [AccountRead::Balance(13), AccountRead::Balance(7)]
    );
    assert!(matches!(
        transfer(&mut nodes, 2, spec(2, 2, true)),
        DriverOutcome::Finished {
            decision: TransferDecision::Commit,
            ..
        }
    ));
    assert_eq!(
        balances(&nodes),
        [AccountRead::Balance(15), AccountRead::Balance(5)]
    );
    let duplicate = transfer(&mut nodes, 3, spec(1, 7, false));
    assert_eq!(first, duplicate);
    assert_eq!(
        balances(&nodes),
        [AccountRead::Balance(15), AccountRead::Balance(5)]
    );
    close(&mut nodes);
}

#[test]
fn host_durable_refusal_aborts_absent_peer_and_leaves_balances_unchanged() {
    let mut nodes = nodes();
    assert!(matches!(
        transfer(&mut nodes, 1, spec(1, 21, false)),
        DriverOutcome::Finished {
            decision: TransferDecision::Abort,
            ..
        }
    ));
    assert_eq!(
        balances(&nodes),
        [AccountRead::Balance(20), AccountRead::Balance(0)]
    );
    let status = nodes[2].local().applications[&profile().participants()[1]]
        .status(TransactionId::new(1).unwrap())
        .unwrap();
    assert_eq!(
        status.phase,
        TransferPhase::Released(TransferDecision::Abort)
    );
    assert!(status.phases[0].is_none());
    assert!(matches!(
        transfer(&mut nodes, 2, spec(2, 7, false)),
        DriverOutcome::Finished {
            decision: TransferDecision::Commit,
            ..
        }
    ));
    close(&mut nodes);
}

#[test]
fn lost_actual_start_output_cancels_observation_then_resumes_from_quorum_status() {
    let mut nodes = nodes();
    let transfer = spec(1, 7, false);
    {
        let mut d = driver(&mut nodes, 1, 1);
        let ticket = d.submit(transfer).unwrap();
        for _ in 0..20 {
            d.advance(
                MonoTime(0),
                DriverBudget {
                    completions: 0,
                    transitions: 0,
                    ..Default::default()
                },
            )
            .unwrap();
        }
        d.stop_waiting().unwrap();
        assert!(matches!(
            outcome(&mut d, ticket),
            DriverOutcome::Unknown { .. }
        ));
        drain(&mut d);
    }
    assert_eq!(
        nodes[0].local().applications[&profile().coordinator()]
            .status(transfer.id())
            .unwrap()
            .phase,
        TransferPhase::Started
    );
    assert_eq!(
        balances(&nodes),
        [AccountRead::Balance(20), AccountRead::Balance(0)]
    );
    {
        let mut d = driver(&mut nodes, 2, 1);
        let ticket = d.resume(transfer.id()).unwrap();
        assert!(matches!(
            outcome(&mut d, ticket),
            DriverOutcome::Finished {
                decision: TransferDecision::Commit,
                ..
            }
        ));
        drain(&mut d);
    }
    assert_eq!(
        balances(&nodes),
        [AccountRead::Balance(13), AccountRead::Balance(7)]
    );
    close(&mut nodes);
}

#[test]
fn original_driver_output_cannot_release_a_new_drivers_credits() {
    let mut nodes = nodes();
    let old_output = {
        let mut d = driver(&mut nodes, 1, 1);
        d.submit(spec(1, 7, false)).unwrap();
        let mut output = None;
        for _ in 0..1000 {
            d.advance(MonoTime(0), DriverBudget::default()).unwrap();
            if let Some(completion) = d.poll_completion() {
                output = Some(completion);
                break;
            }
        }
        assert!(!d.is_drained());
        output.expect("actual original Finished output")
    };
    let mut d = driver(&mut nodes, 2, 1);
    let ticket = d.submit(spec(2, 2, true)).unwrap();
    assert!(d.complete(old_output).is_err());
    assert!(d.submit(spec(3, 1, false)).is_err());
    assert!(matches!(
        outcome(&mut d, ticket),
        DriverOutcome::Finished {
            decision: TransferDecision::Commit,
            ..
        }
    ));
    d.close();
    drain(&mut d);
    drop(d);
    assert_eq!(
        balances(&nodes),
        [AccountRead::Balance(15), AccountRead::Balance(5)]
    );
    assert!(nodes[0].local().applications[&profile().coordinator()]
        .status(TransactionId::new(3).unwrap())
        .is_none());
    close(&mut nodes);
}
