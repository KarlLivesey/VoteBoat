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
fn original_storage_error_is_typed_terminal_and_state_stays_unchanged() {
    let mut f = Fixture::new(1);
    let before = f.owner.core(group(1)).unwrap().state().clone();
    let ticket = f.admit();
    let mut event = f.event();
    event.result = Err(StorageError::Uncertain("original read".into()));
    f.deliver(event).unwrap();
    let output = f.router.poll_export().unwrap();
    assert_eq!(output.ticket(), ticket);
    assert!(matches!(
        output.outcome(),
        SnapshotExportOutcome::Failed(StorageError::Uncertain(_))
    ));
    assert!(matches!(
        f.router.complete_export(output).unwrap(),
        SnapshotExportOutcome::Failed(_)
    ));
    assert!(f.router.is_drained());
    assert!(!f.owner.is_failed());
    assert_eq!(f.owner.core(group(1)).unwrap().state(), &before);
}
#[test]
fn malformed_loaded_shape_reference_metadata_and_capacity_never_escape_as_images() {
    for variant in 0..7 {
        let mut f = Fixture::new(1);
        f.admit();
        let mut event = f.event();
        let Ok(SnapshotOutput::Loaded {
            reference,
            snapshot,
            reconciled,
        }) = &mut event.result
        else {
            panic!("loaded");
        };
        match variant {
            0 => reference.checksum += 1,
            1 => snapshot.metadata.application_schema += 1,
            2 => snapshot.metadata.bootstrap.group = group(99),
            3 => *reconciled = true,
            4 => snapshot.application.reserve(2 * ALLOWANCE),
            5 => snapshot.metadata.bootstrap.configuration = ConfigurationId::new(2).unwrap(),
            _ => event.result = Ok(SnapshotOutput::Reconciled(*reference)),
        }
        assert_eq!(f.deliver(event), Err(SnapshotRouteError::ProviderContract));
        assert!(f.owner.is_failed());
        assert!(matches!(
            f.complete(),
            SnapshotExportOutcome::Failed(StorageError::Corrupt(_))
        ));
        assert!(f.router.is_drained());
    }
}
#[test]
fn wrong_original_envelope_retains_credit_then_failure_drain_reports_uncertain() {
    let mut f = Fixture::new(1);
    let ticket = f.admit();
    let charge = f.router.usage();
    let mut event = f.event();
    event.visit.sequence += 1;
    assert_eq!(f.deliver(event), Err(SnapshotRouteError::StaleCompletion));
    assert!(f.owner.is_failed());
    assert_eq!(f.router.usage(), charge);
    f.router
        .drain_failed_exports(&mut f.owner, &mut f.worker, 1)
        .unwrap();
    let output = f.router.poll_export().unwrap();
    assert_eq!(output.ticket(), ticket);
    assert!(matches!(
        f.router.complete_export(output).unwrap(),
        SnapshotExportOutcome::Failed(StorageError::Uncertain(_))
    ));
    assert!(f.router.is_drained());
}
#[test]
fn owner_failure_preserves_pending_cancel_and_externally_held_original_output() {
    let mut f = Fixture::new(1);
    let ticket = f.admit();
    let mut bad = f.event();
    bad.request.binding.generation = SnapshotWorkerGeneration::new(2).unwrap();
    assert_eq!(f.deliver(bad), Err(SnapshotRouteError::StaleCompletion));
    f.router.cancel_export(ticket).unwrap();
    f.router
        .drain_failed_exports(&mut f.owner, &mut f.worker, 1)
        .unwrap();
    assert!(f.router.is_drained());
    let mut f = Fixture::new(1);
    f.admit();
    let event = f.event();
    f.deliver(event).unwrap();
    let original = f.router.poll_export().unwrap();
    let charge = f.router.usage();
    let mut clients = ClientRouter::<CounterReceipt>::new(
        ClientRouterBinding::new(
            f.owner.authorization(),
            ClientRouterGeneration::new(1).unwrap(),
        ),
        ClientRouterLimits::default(),
    )
    .unwrap();
    clients.abort(&mut f.owner).unwrap();
    f.router
        .drain_failed_exports(&mut f.owner, &mut f.worker, 1)
        .unwrap();
    assert_eq!(f.router.usage(), charge);
    f.router.complete_export(original).unwrap();
    assert!(f.router.is_drained());
}
