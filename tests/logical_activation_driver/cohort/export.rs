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
//! Original sole-consumer live checkpoint export, not readiness permission.
use super::*;
use voteboat::snapshot::Snapshot;
pub fn snapshot(
    driver: &mut ChildActivationDriver<'_, fixture::FacadeNode>,
    peers: &mut progress::Peers<'_>,
    target: &mut aliases::Bridge<'_>,
) -> Snapshot {
    target.checkpoint_backing(0).unwrap();
    let end = harness::hang_guard();
    loop {
        peers.step(driver, target);
        if target
            .inspect_backing(0, |l| {
                l.owner
                    .core(groups::mapping(0).child)
                    .unwrap()
                    .state()
                    .snapshot
                    .is_some()
            })
            .unwrap()
        {
            break;
        }
        assert!(
            Instant::now() < end,
            "original target durable checkpoint anchor"
        );
    }
    let ticket = loop {
        match target.export_checkpoint_backing(0) {
            Ok(ticket) => break ticket,
            Err(SnapshotRouteError::Overloaded) => peers.step(driver, target),
            Err(reason) => panic!("original target export {reason:?}"),
        }
        assert!(Instant::now() < end, "not-admitted export retry");
    };
    loop {
        peers.step(driver, target);
        if let Some(output) = target.poll_checkpoint_export_backing(0) {
            assert_eq!(output.ticket(), ticket);
            assert_eq!(
                target.cancel_checkpoint_export_backing(0, ticket),
                Err(SnapshotRouteError::StaleCompletion)
            );
            let rejected = target
                .complete_checkpoint_export_backing(1, output)
                .unwrap_err();
            assert_eq!(rejected.reason, SnapshotRouteError::WrongOwner);
            assert_eq!(rejected.completion.ticket(), ticket);
            return match target
                .complete_checkpoint_export_backing(0, *rejected.completion)
                .unwrap()
            {
                SnapshotExportOutcome::Loaded(snapshot) => snapshot,
                SnapshotExportOutcome::Failed(reason) => panic!("original target load {reason:?}"),
            };
        }
        assert!(Instant::now() < end, "original target export completion");
    }
}
