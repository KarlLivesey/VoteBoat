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
use voteboat::{group_creation::*, native::log_store::*, raft::Raft};

fn prefix(p: &NamespacePlan) -> Vec<LogEntry> {
    vec![
        entry(
            1,
            1,
            fresh(16)
                .bootstrap_command(MAX_DIRECTORY_COMMAND_BYTES)
                .unwrap(),
        ),
        entry(
            2,
            2,
            DirectoryCommand {
                expected: None,
                manifest: manifest(1, 20),
            }
            .encode(100000)
            .unwrap(),
        ),
        entry(
            3,
            3,
            p.creation.intent.encode(MAX_GROUP_CREATION_BYTES).unwrap(),
        ),
    ]
}
fn seed(p: &NamespacePlan) -> (ModelIo, NativeLogStore<ModelIo>) {
    let io = ModelIo::default();
    let mut log = NativeLogStore::create(io.clone(), identity(1), LogLimits::default()).unwrap();
    append(&mut log, vec![LogMutation::Create(bootstrap(1, 3))]);
    let state = log.state(group(1)).unwrap();
    append(
        &mut log,
        vec![update(
            &state,
            1,
            3,
            Some(Suffix {
                from: 1,
                entries: prefix(p),
            }),
        )],
    );
    (io, log)
}
fn recovered(log: &NativeLogStore<ModelIo>) -> (Raft, Directory) {
    let core = Raft::recover(
        node(1),
        log.binding(),
        log.state(group(1)).unwrap(),
        log.limits(),
    )
    .unwrap();
    let mut app = fresh(16);
    app.apply_batch(core.replay_committed()).unwrap();
    (core, app)
}
#[test]
fn every_cancel_frame_cut_and_barrier_fault_preserves_old_or_cancelled_then_exact_retry() {
    let (_, p) = reserve_directory(fresh(16));
    let bytes = cancellation(&p);
    let (_, log) = seed(&p);
    let mutation = update(
        &log.state(group(1)).unwrap(),
        1,
        4,
        Some(Suffix {
            from: 4,
            entries: vec![entry(4, 4, bytes.clone())],
        }),
    );
    let frame = NativeLogCodec
        .encode_batch(3, std::slice::from_ref(&mutation), log.limits())
        .unwrap();
    let mut old = 0;
    let mut cancelled = 0;
    for fault in (0..=frame.len()).map(Fault::Append).chain([
        Fault::Sync,
        Fault::PublishBefore,
        Fault::PublishAfter,
    ]) {
        let (io, mut log) = seed(&p);
        io.0.borrow_mut().fault = fault;
        if let Ok(tickets) = log.append_batch(vec![mutation.clone()]) {
            assert!(log.barrier(&tickets).is_err());
        }
        drop(log);
        io.0.borrow_mut().power_loss();
        let mut log = NativeLogStore::recover(io, identity(1), LogLimits::default()).unwrap();
        let (_, app) = recovered(&log);
        let status = app
            .group_creation_cancellation_at(app.applied_index(), p.creation.operation)
            .unwrap();
        if status.is_some() {
            cancelled += 1;
            assert_eq!(app.applied_index(), 4);
            assert!(app.group_creation_at(0, group(100)).unwrap().is_none());
        } else {
            old += 1;
            assert_eq!(app.applied_index(), 3);
            assert_eq!(
                app.group_creation_at(0, group(100)).unwrap(),
                Some(p.creation.clone())
            );
        }
        let state = log.state(group(1)).unwrap();
        let at = state.last_index() + 1;
        append(
            &mut log,
            vec![update(
                &state,
                1,
                at,
                Some(Suffix {
                    from: at,
                    entries: vec![entry(at, 4, bytes.clone())],
                }),
            )],
        );
        let (core, mut app) = recovered(&log);
        let final_status = app
            .group_creation_cancellation_at(at, p.creation.operation)
            .unwrap()
            .unwrap();
        assert_eq!(final_status.index, 4);
        assert_eq!(final_status.operation, OperationId::new(4).unwrap());
        assert!(LocalCreationAuthority {
            core: &core,
            directory: &app
        }
        .verify(&p.creation)
        .is_err());
        let mut target = target(&p);
        let late = ready(&mut target);
        assert_eq!(
            commit(&mut app, 5, late.encode(1024).unwrap()).outcome,
            DirectoryOutcome::TransferEvidenceMismatch
        );
        assert_eq!(
            target.read_at(target.applied_index(), query()).unwrap(),
            NamespaceRead::NotActive
        );
    }
    assert!(old > 0 && cancelled > 0);
    eprintln!(
        "cancel frame cuts: {old} old, {cancelled} cancelled; all exact retries remain cancelled"
    );
}
