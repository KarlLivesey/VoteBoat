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
//! Downstream construction and application-contract checks using public APIs.
use voteboat::log::{EntryPayload, LogEntry};
use voteboat::{application::*, identity::*, transaction::*};

fn group(id: u128) -> GroupIdentity {
    GroupIdentity {
        id: GroupId::new(id).unwrap(),
        incarnation: GroupIncarnation::new(1).unwrap(),
    }
}
fn profile() -> TransferProfile {
    TransferProfile::new(group(1), [group(2), group(3)]).unwrap()
}
fn phases(base: u128) -> PhaseIds {
    PhaseIds::new(
        std::array::from_fn(|i| OperationId::new(base + i as u128 + 1).unwrap()),
        std::array::from_fn(|_| {
            std::array::from_fn(|i| OperationId::new(base + i as u128 + 1).unwrap())
        }),
    )
    .unwrap()
}
fn spec(id: u128, amount: i64, reverse: bool) -> TransferSpec {
    let refs = [group(2), group(3)].map(|g| AccountRef::new(g, AccountId::new(1).unwrap()));
    let (debit, credit) = if reverse {
        (refs[1], refs[0])
    } else {
        (refs[0], refs[1])
    };
    TransferSpec::new(
        TransactionId::new(id).unwrap(),
        profile(),
        debit,
        credit,
        amount,
        phases(id * 100),
    )
    .unwrap()
}

#[test]
fn public_transfer_identity_and_phase_namespaces_preserve_exact_binding() {
    assert!(TransactionId::new(0).is_none());
    assert!(AccountId::new(0).is_none());
    let forward = spec(1, 5, false);
    let reverse = spec(2, 7, true);
    assert_eq!(forward.profile(), reverse.profile());
    assert_eq!(forward.debit(), reverse.credit());
    assert_eq!(forward.credit(), reverse.debit());
    assert_eq!(forward.amount(), 5);
    assert_ne!(forward.id(), reverse.id());
    let scoped = phases(0);
    assert_eq!(scoped.coordinator()[0], scoped.participants()[0][0]);
    let mut aliased = scoped.participants();
    aliased[1][2] = aliased[1][0];
    assert_eq!(
        PhaseIds::new(scoped.coordinator(), aliased),
        Err(ApplicationError::OperationConflict)
    );
}

#[test]
fn invalid_profile_direction_amount_and_limits_refuse_before_application() {
    assert!(TransferProfile::new(group(1), [group(3), group(2)]).is_err());
    assert!(TransferProfile::new(group(1), [group(2), group(2)]).is_err());
    assert!(TransferProfile::new(group(2), [group(2), group(3)]).is_err());
    let original = spec(1, 5, false);
    for amount in [0, -1] {
        assert!(TransferSpec::new(
            original.id(),
            profile(),
            original.debit(),
            original.credit(),
            amount,
            original.phases()
        )
        .is_err());
    }
    for credit in [
        original.debit(),
        AccountRef::new(group(4), AccountId::new(1).unwrap()),
    ] {
        assert!(TransferSpec::new(
            original.id(),
            profile(),
            original.debit(),
            credit,
            5,
            original.phases()
        )
        .is_err());
    }
    for (transactions, accounts) in [(0, 1), (1, 0), (4097, 1), (1, 4097)] {
        assert!(TransferLimits::new(transactions, accounts).is_err());
    }
    let limits = TransferLimits::new(2, 3).unwrap();
    assert_eq!((limits.transactions(), limits.accounts()), (2, 3));
}

fn coordinator(capacity: usize) -> TransferApplication {
    TransferApplication::coordinator(
        group(1),
        profile(),
        TransferLimits::new(capacity, 2).unwrap(),
    )
    .unwrap()
}
fn start(index: u64, request: TransferSpec) -> LogEntry {
    LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Command {
            operation: request.phases().coordinator()[0],
            bytes: TransferApplication::start(request).unwrap().into(),
        },
    }
}
fn image(app: &TransferApplication) -> Vec<u8> {
    app.checkpoint(app.deployment_requirements().unwrap().snapshot_bytes)
        .unwrap()
}
fn bytes(entry: &LogEntry) -> (OperationId, &[u8]) {
    let EntryPayload::Command { operation, bytes } = &entry.payload else {
        panic!("expected a command");
    };
    (*operation, bytes)
}

#[test]
fn public_application_receipts_reads_and_checkpoint_use_existing_contracts() {
    let mut app = coordinator(2);
    let noop = |index| LogEntry {
        index,
        term: 1,
        payload: EntryPayload::Noop,
    };
    let request = spec(1, 5, false);
    let entries = [
        noop(1),
        start(2, request),
        start(3, spec(2, 7, true)),
        start(4, request),
        noop(5),
    ];
    let bound = app.receipt_bytes_bound(&entries).unwrap();
    let receipts = app.apply_batch(&entries).unwrap();
    assert_eq!(receipts.len(), 3);
    assert!(receipts.capacity() * std::mem::size_of::<TransferReceipt>() <= bound);
    for receipt in &receipts {
        assert_eq!(receipt.nested_bytes(0), Ok(0));
        assert_eq!(receipt.outcome, TransferOutcome::Started);
    }
    assert_eq!(
        receipts
            .iter()
            .map(|r| (r.index(), r.operation()))
            .collect::<Vec<_>>(),
        entries
            .iter()
            .filter_map(|e| match e.payload {
                EntryPayload::Command { operation, .. } => Some((e.index, operation)),
                _ => None,
            })
            .collect::<Vec<_>>()
    );
    assert_eq!(
        (
            receipts[2].index,
            receipts[2].first_index,
            receipts[2].duplicate
        ),
        (4, 2, true)
    );
    assert_eq!(app.applied_index(), 5);
    assert_eq!(app.remaining_transactions(), 0);
    let before = image(&app);
    let query = TransferQuery::Status(request.id());
    assert_eq!(app.query_bytes(&query, 0), Ok(0));
    let result = app.read_at(5, query).unwrap();
    let TransferRead::Status(Some(status)) = &result else {
        panic!("missing original status");
    };
    assert_eq!(
        (status.spec, status.phase),
        (request, TransferPhase::Started)
    );
    let phase = status.phases[0].unwrap();
    assert_eq!(
        (phase.index, phase.operation),
        (2, request.phases().coordinator()[0])
    );
    assert!(status.phases[1..].iter().all(Option::is_none));
    let nested = std::mem::size_of::<TransferStatus>();
    assert_eq!(
        app.read_result_bound(&query).unwrap(),
        std::mem::size_of_val(&result) + nested
    );
    assert_eq!(app.read_result_bytes(&result, nested), Ok(nested));
    assert_eq!(
        app.read_result_bytes(&result, nested - 1),
        Err(ApplicationError::ReceiptBudget)
    );
    assert_eq!(app.read_at(6, query), Err(ApplicationError::NotApplied));
    assert_eq!(image(&app), before);
    let mut restored = coordinator(2);
    restored
        .restore_checkpoint(app.schema_version(), 5, &before)
        .unwrap();
    assert_eq!(restored, app);
    assert_eq!(
        restored.deployment_requirements(),
        app.deployment_requirements()
    );
    let retry = restored.apply_batch(&[start(6, request)]).unwrap()[0];
    assert_eq!(
        (retry.index, retry.first_index, retry.duplicate),
        (6, 2, true)
    );
}

#[test]
fn pending_start_capacity_and_all_destination_phase_ids_are_reserved() {
    let app = coordinator(1);
    let first = start(1, spec(1, 5, false));
    let second = start(2, spec(2, 7, false));
    let (first_id, first_bytes) = bytes(&first);
    let (second_id, second_bytes) = bytes(&second);
    assert_eq!(first_bytes.len(), TRANSFER_COMMAND_BYTES);
    assert!(app
        .validate_proposal(first_id, first_bytes, std::iter::empty())
        .is_ok());
    assert!(app
        .validate_proposal(first_id, first_bytes, [(first_id, first_bytes)].into_iter())
        .is_ok());
    assert_eq!(
        app.validate_proposal(
            second_id,
            second_bytes,
            [(first_id, first_bytes)].into_iter()
        ),
        Err(ApplicationError::DedupCapacity)
    );
    let original = spec(1, 5, false);
    let next = spec(2, 7, false);
    let mut participants = next.phases().participants();
    participants[0][2] = original.phases().participants()[0][1];
    let alias = TransferSpec::new(
        next.id(),
        next.profile(),
        next.debit(),
        next.credit(),
        next.amount(),
        PhaseIds::new(next.phases().coordinator(), participants).unwrap(),
    )
    .unwrap();
    let alias = start(2, alias);
    let (alias_id, alias_bytes) = bytes(&alias);
    let mut app = coordinator(2);
    assert_eq!(
        app.validate_proposal(alias_id, alias_bytes, [(first_id, first_bytes)].into_iter()),
        Err(ApplicationError::OperationConflict)
    );
    app.apply_batch(&[first]).unwrap();
    assert_eq!(
        app.validate_proposal(alias_id, alias_bytes, std::iter::empty()),
        Err(ApplicationError::OperationConflict)
    );
}

#[test]
fn public_failed_batches_and_restores_preserve_original_application() {
    let mut app = coordinator(2);
    let pristine = app.clone();
    let mut malformed = start(2, spec(2, 7, false));
    let EntryPayload::Command { bytes, .. } = &mut malformed.payload else {
        unreachable!();
    };
    let mut replacement = bytes.to_vec();
    replacement.push(0);
    *bytes = replacement.into();
    assert!(app
        .apply_batch(&[start(1, spec(1, 5, false)), malformed])
        .is_err());
    assert_eq!(app, pristine);
    app.apply_batch(&[start(1, spec(1, 5, false))]).unwrap();
    let checkpoint = image(&app);
    let mut target = coordinator(2);
    for end in 0..checkpoint.len() {
        let before = target.clone();
        assert!(target
            .restore_checkpoint(app.schema_version(), 1, &checkpoint[..end])
            .is_err());
        assert_eq!(target, before);
    }
    for (schema, index) in [(app.schema_version() + 1, 1), (app.schema_version(), 2)] {
        let before = target.clone();
        assert!(target
            .restore_checkpoint(schema, index, &checkpoint)
            .is_err());
        assert_eq!(target, before);
    }
    let mut incompatible = coordinator(3);
    let before = incompatible.clone();
    assert!(incompatible
        .restore_checkpoint(app.schema_version(), 1, &checkpoint)
        .is_err());
    assert_eq!(incompatible, before);
    assert!(app.checkpoint(checkpoint.len() - 1).is_err());
}

#[test]
fn public_role_incarnation_and_opening_account_configuration_are_enforced() {
    let account = AccountId::new(1).unwrap();
    let limits = TransferLimits::new(2, 2).unwrap();
    let app =
        TransferApplication::participant(group(2), profile(), &[(account, 20)], limits).unwrap();
    assert_eq!(app.role(), TransferRole::Participant(0));
    assert!(app.contains_account(account));
    assert_eq!(app.account_ids().collect::<Vec<_>>(), vec![account]);
    assert_eq!(
        app.read_at(0, TransferQuery::Account(account)),
        Ok(TransferRead::Account(AccountRead::Balance(20)))
    );
    let account_query = TransferQuery::Account(account);
    let account_result = app.read_at(0, account_query).unwrap();
    assert_eq!(app.read_result_bytes(&account_result, 0), Ok(0));
    assert_eq!(
        app.read_result_bound(&account_query).unwrap(),
        std::mem::size_of::<TransferRead>()
    );
    let absent = app
        .read_at(0, TransferQuery::Status(TransactionId::new(99).unwrap()))
        .unwrap();
    assert_eq!(absent, TransferRead::Status(None));
    assert_eq!(app.read_result_bytes(&absent, 0), Ok(0));
    assert_eq!(
        app.account(AccountId::new(2).unwrap()),
        AccountRead::Unknown
    );
    assert!(app.validate_group(group(2)).is_ok());
    let mut stale = group(2);
    stale.incarnation = GroupIncarnation::new(2).unwrap();
    assert!(app.validate_group(stale).is_err());
    assert!(TransferApplication::coordinator(group(2), profile(), limits).is_err());
    assert!(
        TransferApplication::participant(group(1), profile(), &[(account, 20)], limits).is_err()
    );
    assert!(
        TransferApplication::participant(group(2), profile(), &[(account, -1)], limits).is_err()
    );
    assert!(TransferApplication::participant(
        group(2),
        profile(),
        &[(account, 1), (account, 2)],
        limits
    )
    .is_err());
    let mut changed =
        TransferApplication::participant(group(2), profile(), &[(account, 19)], limits).unwrap();
    let before = changed.clone();
    assert!(changed
        .restore_checkpoint(app.schema_version(), 0, &image(&app))
        .is_err());
    assert_eq!(changed, before);
}
