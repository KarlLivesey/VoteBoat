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
use voteboat::retirement::*;

type Guard = <Guarded as TargetProfile>::Application;
fn recovered(
    rig: &mut Moves<Guarded>,
    retired: RetirementStatus,
    frozen: &SourceFreezeStatus,
    lineage: &[u8],
    bytes: &[u8],
) {
    let clock = rig.base.clock;
    for n in rig.nodes(31).iter() {
        let app = &n.local().applications[&group(31)];
        assert!(app.owner().is_none());
        assert_eq!(app.status(), Some(retired));
        assert_eq!(app.freeze_status().unwrap().as_ref(), Some(frozen));
        assert_eq!(app.retired_lineage(), Some(lineage));
        assert!(app.export_target(group(41), 65536).is_err());
        assert!(app.export_target(group(42), 65536).is_err());
    }
    assert_eq!(
        split::observe(rig.nodes(31), &clock, 31, RetirementQuery::Status),
        RetirementRead::Status(Some(retired))
    );
    assert_eq!(
        split::observe(rig.nodes(31), &clock, 31, RetirementQuery::Freeze),
        RetirementRead::Freeze(Some(frozen.clone()))
    );
    assert_eq!(
        split::observe(
            rig.nodes(31),
            &clock,
            31,
            RetirementQuery::Owner(TargetQuery::Status)
        ),
        RetirementRead::Retired
    );
    let retry = propose_recovering(rig.nodes(31), &clock, 31, 501, bytes.to_vec());
    assert_eq!(retry.operation, source_fixture::op(501));
    assert_eq!(retry.outcome, RetirementOutcome::Retired(retired));
    assert!(rig.nodes(31)[0]
        .propose(ClientRequest {
            group: group(31),
            operation: source_fixture::op(9999),
            bytes: request_data(frozen.intent.before(), 1, 1),
        })
        .is_err());
    for c in configuration_for(&rig.base.root, 31) {
        let mut b = FileCreationBindings::open(&c.directory).unwrap();
        assert_eq!(b.load().unwrap().unwrap(), rig.base.bindings[&(31, c.node)]);
    }
}
fn reopen(rig: &mut Moves<Guarded>) {
    let intent = rig.base.bound.clone().unwrap();
    rig.base.targets[0] = open(
        configuration_for(&rig.base.root, 31),
        &rig.base.clock,
        rig.base.protocol,
        || profile_target::<Guarded>(&intent, 31),
    );
}
fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Moves::<Guarded>::new(protocol, checkpoint);
    rig.keep_unread = false;
    let clock = rig.base.clock;
    let original_activation = rig.original.activated.unwrap();
    while let Some(r) = rig.resume(501) {
        eprintln!(
            "nested retirement {protocol:?} checkpoint={checkpoint} setup={:?}",
            r.phase
        );
        let s = rig.observed(501);
        rig.check(501, &s);
    }
    let before = rig.observed(501);
    rig.restart();
    assert_eq!(rig.observed(501), before);
    rig.check(501, &before);
    let frozen = before.sources[0].2.clone().unwrap();
    assert_eq!(frozen.fence.group, group(31));
    assert_eq!(frozen.fence.operation, source_fixture::op(501));
    assert_eq!(before.sources[0].1.activated, Some(original_activation));
    let lineage = rig.nodes(31)[0].local().applications[&group(31)]
        .owner()
        .unwrap()
        .retirement_lineage()
        .unwrap();
    assert!(!lineage.is_empty());
    let targets = before
        .targets
        .iter()
        .map(|(g, s)| {
            TargetActivationEvidence::from_status(
                Moves::<Guarded>::configuration(rig.nodes(*g), *g),
                s.clone().unwrap(),
            )
            .unwrap_or_else(|e| panic!("activation: {:?}", e.0))
        })
        .collect();
    let proof = RetirementProof {
        metadata_configuration: Moves::<Guarded>::configuration(&rig.base.parent, 1),
        decision: before.publication.clone().unwrap(),
        targets,
        release: RetentionRelease {
            source: group(31),
            operation: source_fixture::op(501),
            fence_index: frozen.fence.index,
            release: source_fixture::op(901),
        },
    };
    let mut incomplete = proof.clone();
    incomplete.targets.pop();
    let mut wrong_release = proof.clone();
    wrong_release.release.fence_index += 1;
    for invalid in [&incomplete, &wrong_release] {
        assert!(rig.nodes(31)[0].local().applications[&group(31)]
            .retirement_command(invalid, MAX_RETIREMENT_COMMAND_BYTES)
            .is_err());
    }
    assert!(rig
        .nodes(31)
        .iter()
        .all(|n| n.local().applications[&group(31)].status().is_none()));
    let bytes = rig.nodes(31)[0].local().applications[&group(31)]
        .retirement_command(&proof, MAX_RETIREMENT_COMMAND_BYTES)
        .unwrap();
    // restart above installed only live-owner checkpoints, when enabled.
    unread(
        rig.nodes(31),
        &clock,
        31,
        501,
        bytes.clone(),
        |a: &Guard| a.status().is_some(),
    );
    let retired = rig.nodes(31)[0].local().applications[&group(31)]
        .status()
        .unwrap();
    assert_eq!(retired.fence_index, frozen.fence.index);
    assert!(retired.index > frozen.fence.index);
    assert_eq!(retired.retention_release, proof.release.release);
    assert_eq!(retired.publication_index, proof.decision.index);
    let logs = creation::abandon(std::mem::take(&mut rig.base.targets[0]), 31);
    for log in logs.values() {
        assert!(log.base_index() < retired.index);
        assert!(
            log.entries
                .iter()
                .any(|e| e.index == retired.index
                    && matches!(e.payload, EntryPayload::Command { .. }))
        );
    }
    reopen(&mut rig);
    recovered(&mut rig, retired, &frozen, &lineage, &bytes);
    if checkpoint {
        split::compact(rig.nodes(31), &clock, 31);
        let requests = rig
            .nodes(31)
            .iter_mut()
            .map(|n| n.reclaim(LogLimits::default().max_wal_bytes).unwrap())
            .collect::<Vec<_>>();
        let mut completed = [false; 3];
        drive(rig.nodes(31), &clock, |ns| {
            for (i, n) in ns.iter_mut().enumerate() {
                if let Some(event) = n.poll_reclaim() {
                    assert_eq!(event.request, requests[i]);
                    let report = event.result.unwrap();
                    assert!(report.after_bytes < report.before_bytes);
                    completed[i] = true;
                }
            }
            completed.iter().all(|x| *x)
        });
        let logs = creation::abandon(std::mem::take(&mut rig.base.targets[0]), 31);
        for log in logs.values() {
            assert!(log.base_index() >= retired.index);
            assert!(log
                .entries
                .iter()
                .all(|e| !matches!(e.payload, EntryPayload::Command { .. })));
        }
        reopen(&mut rig);
        recovered(&mut rig, retired, &frozen, &lineage, &bytes);
    }
    creation::abandon(std::mem::take(&mut rig.base.targets[0]), 31);
    creation::abandon(std::mem::take(&mut rig.base.parent), 1);
    creation::abandon(std::mem::take(&mut rig.base.source), 21);
    let mut stopped = BTreeMap::new();
    for g in [1, 21, 31] {
        stopped.extend(durable_files(&rig.base.root.join(g.to_string())));
    }
    rig.write(41, &before.child, 1, 1, 7, 7, true);
    rig.write(42, &before.child, 81, 40, 3, 3, true);
    rig.write(41, &before.child, 82, 1, 2, 9, false);
    rig.write(42, &before.child, 83, 40, 4, 7, false);
    rig.write(32, &before.sibling, 80, 80, 5, 5, true);
    rig.write(32, &before.sibling, 84, 80, 1, 6, false);
    for (g, manifest, key, value) in [
        (41, &before.child, 1, 9),
        (42, &before.child, 40, 7),
        (32, &before.sibling, 80, 6),
    ] {
        assert_eq!(
            observe_target::<Guarded>(rig.nodes(g), &clock, g, request_query(manifest, key)),
            TargetRead::Data(value)
        );
        assert!(rig
            .nodes(g)
            .iter()
            .all(|n| Guarded::owner(&n.local().applications[&group(g)])
                .application()
                .outbox()
                .count()
                == 2));
    }
    let mut after = BTreeMap::new();
    for g in [1, 21, 31] {
        after.extend(durable_files(&rig.base.root.join(g.to_string())));
    }
    assert_eq!(after, stopped);
    let mut old = durable_files(&rig.base.root.join("20"));
    old.extend(durable_files(&rig.base.root.join("22")));
    assert_eq!(old, rig.base.stopped);
    reopen(&mut rig);
    recovered(&mut rig, retired, &frozen, &lineage, &bytes);
    rig.stop();
    std::fs::remove_dir_all(rig.base.root).unwrap();
}
#[test]
fn tcp_assigned_nested_retirement_recovers_unread_result_from_wal() {
    history(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_assigned_nested_retirement_recovers_then_reclaims_checkpoint() {
    history(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_assigned_nested_retirement_recovers_unread_result_from_wal() {
    history(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_assigned_nested_retirement_recovers_then_reclaims_checkpoint() {
    history(NativePeerProtocol::Quic, true);
}
