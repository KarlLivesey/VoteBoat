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
fn insertion_and_cancellation_are_exclusive_in_both_orders() {
    for cancel_first in [true, false] {
        let fresh = directory()
            .with_creation_cancellation()
            .unwrap_or_else(|_| panic!("schema16"));
        let (mut d, intent) = setup_in(fresh);
        let s = d
            .group_creation_at(d.applied_index(), group(21))
            .unwrap()
            .unwrap();
        let cancel = CancelGroupCreation::from_status(&s).encode(56).unwrap();
        let insert = intent.encode(100000).unwrap();
        if cancel_first {
            assert_eq!(
                commit(&mut d, 199, cancel).outcome,
                DirectoryOutcome::CreationCancelled
            );
            assert_eq!(
                commit(&mut d, 200, insert).outcome,
                DirectoryOutcome::TransferEvidenceMismatch
            );
            // A plain transfer without creation references must not recycle the target either.
            for incarnation in 1..=2 {
                let mut reused = group(21);
                reused.incarnation = GroupIncarnation::new(incarnation).unwrap();
                assert_eq!(
                    commit(&mut d, 200 + u128::from(incarnation), plain_reuse(reused)).outcome,
                    DirectoryOutcome::TransferGroupBusy
                );
            }
            assert_eq!(d.manifest(grant().input().responsibility), Some(&grant()));
        } else {
            assert_eq!(
                commit(&mut d, 200, insert).outcome,
                DirectoryOutcome::TransferIntentRecorded
            );
            assert_eq!(
                commit(&mut d, 199, cancel).outcome,
                DirectoryOutcome::TransferEvidenceMismatch
            );
            assert!(d
                .group_creation_at(d.applied_index(), group(21))
                .unwrap()
                .is_some());
        }
        let image = d.checkpoint(1000000).unwrap();
        let mut reopened = directory()
            .with_creation_cancellation()
            .unwrap_or_else(|_| panic!("schema16"));
        reopened
            .restore_checkpoint(16, d.applied_index(), &image)
            .unwrap();
        assert_eq!(
            reopened
                .group_creation_cancellation_at(d.applied_index(), s.operation)
                .unwrap()
                .is_some(),
            cancel_first
        );
        assert_eq!(
            reopened
                .transfer_intent_at(d.applied_index(), op(200))
                .unwrap()
                .is_some(),
            !cancel_first
        );
    }
}

fn plain_reuse(reused: GroupIdentity) -> Vec<u8> {
    let mut after = grant().into_input();
    after.execution = ExecutionMode::Partitioned(vec![
        RouteEntry {
            scope: range(0, 128),
            target: RouteTarget::Group(reused),
        },
        RouteEntry {
            scope: range(128, 256),
            target: RouteTarget::Group(group(23)),
        },
    ]);
    after.epoch = OwnershipEpoch::new(2).unwrap();
    after.generation = RouteGeneration::new(2).unwrap();
    TransferIntent::new(grant(), ResponsibilityManifest::new(after).unwrap())
        .unwrap()
        .encode(100000)
        .unwrap()
}
