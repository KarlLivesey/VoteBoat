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
struct RetiredSource {
    frozen: SourceFreezeStatus,
    lineage: Vec<u8>,
    bytes: Vec<u8>,
    status: RetirementStatus,
}
fn proof(rig: &mut Moves<Guarded>, frozen: &SourceFreezeStatus) -> RetirementProof {
    let clock = rig.base.clock;
    let DirectoryRead::Publication(Some(decision)) =
        rig.meta(DirectoryQuery::Publication(source_fixture::op(601)))
    else {
        panic!("merge publication")
    };
    let TargetRead::Status(target) =
        observe_target::<Guarded>(rig.nodes(43), &clock, 43, TargetQuery::Status)
    else {
        panic!("merged activation")
    };
    RetirementProof {
        metadata_configuration: Moves::<Guarded>::configuration(&rig.base.parent, 1),
        decision,
        targets: vec![TargetActivationEvidence::from_status(
            Moves::<Guarded>::configuration(rig.nodes(43), 43),
            target,
        )
        .unwrap_or_else(|e| panic!("activation: {:?}", e.0))],
        release: RetentionRelease {
            source: frozen.fence.group,
            operation: frozen.fence.operation,
            fence_index: frozen.fence.index,
            release: source_fixture::op(900 + frozen.fence.group.id.get()),
        },
    }
}
fn retire(
    rig: &mut Moves<Guarded>,
    frozen: SourceFreezeStatus,
    initial: &TransferIntent,
) -> RetiredSource {
    let clock = rig.base.clock;
    let g = frozen.fence.group.id.get();
    let lineage = rig.nodes(g)[0].local().applications[&group(g)]
        .owner()
        .unwrap()
        .retirement_lineage()
        .unwrap();
    assert!(!lineage.is_empty());
    let p = proof(rig, &frozen);
    let mut swapped = p.clone();
    swapped.release.source = group(if g == 41 { 42 } else { 41 });
    assert!(rig.nodes(g)[0].local().applications[&group(g)]
        .retirement_command(&swapped, MAX_RETIREMENT_COMMAND_BYTES)
        .is_err());
    assert!(rig
        .nodes(g)
        .iter()
        .all(|n| n.local().applications[&group(g)].status().is_none()));
    let bytes = rig.nodes(g)[0].local().applications[&group(g)]
        .retirement_command(&p, MAX_RETIREMENT_COMMAND_BYTES)
        .unwrap();
    unread(rig.nodes(g), &clock, g, 601, bytes.clone(), |a: &Guard| {
        a.status().is_some()
    });
    let status = rig.nodes(g)[0].local().applications[&group(g)]
        .status()
        .unwrap();
    assert_eq!(status.source, group(g));
    assert_eq!(status.operation, source_fixture::op(601));
    assert_eq!(status.fence_index, frozen.fence.index);
    assert!(status.index > frozen.fence.index);
    assert_eq!(status.retention_release, p.release.release);
    assert_eq!(status.publication_index, p.decision.index);
    let logs = creation::abandon(std::mem::take(rig.nodes(g)), g);
    for log in logs.values() {
        assert!(log.base_index() < status.index);
        assert!(log
            .entries
            .iter()
            .any(|e| e.index == status.index && matches!(e.payload, EntryPayload::Command { .. })));
    }
    reopen(rig, g, initial);
    recovered(rig, status, &frozen, &lineage, &bytes);
    RetiredSource {
        frozen,
        lineage,
        bytes,
        status,
    }
}
fn check(rig: &mut Moves<Guarded>, r: &RetiredSource) {
    recovered(rig, r.status, &r.frozen, &r.lineage, &r.bytes);
}
fn serve_after_retirement(rig: &mut Moves<Guarded>, before: &State) {
    let clock = rig.base.clock;
    creation::abandon(std::mem::take(&mut rig.base.parent), 1);
    creation::abandon(std::mem::take(&mut rig.base.source), 21);
    creation::abandon(std::mem::take(&mut rig.base.targets[0]), 31);
    let mut stopped = BTreeMap::new();
    for g in [1, 21, 31, 41, 42] {
        stopped.extend(durable_files(&rig.base.root.join(g.to_string())));
    }
    for (id, key, delta, value) in [(1, 1, 7, 7), (81, 40, 3, 3), (82, 1, 2, 9), (83, 40, 4, 7)] {
        rig.write(43, &before.child, id, key, delta, value, true);
    }
    rig.write(43, &before.child, 84, 1, 1, 10, false);
    rig.write(32, &before.sibling, 80, 80, 5, 5, true);
    rig.write(32, &before.sibling, 85, 80, 1, 6, false);
    for (g, m, key, value, outbox) in [
        (43, &before.child, 1, 10, 5),
        (43, &before.child, 40, 7, 5),
        (32, &before.sibling, 80, 6, 2),
    ] {
        assert_eq!(
            observe_target::<Guarded>(rig.nodes(g), &clock, g, request_query(m, key)),
            TargetRead::Data(value)
        );
        drive(rig.nodes(g), &clock, |ns| {
            ns.iter().all(|n| {
                Guarded::owner(&n.local().applications[&group(g)])
                    .application()
                    .outbox()
                    .count()
                    == outbox
            })
        });
    }
    let mut after = BTreeMap::new();
    for g in [1, 21, 31, 41, 42] {
        after.extend(durable_files(&rig.base.root.join(g.to_string())));
    }
    assert_eq!(after, stopped);
    let mut old = durable_files(&rig.base.root.join("20"));
    old.extend(durable_files(&rig.base.root.join("22")));
    assert_eq!(old, rig.base.stopped);
}
fn history(protocol: NativePeerProtocol, checkpoint: bool) {
    let _history = NATIVE_HISTORY.lock().unwrap_or_else(|e| e.into_inner());
    let mut rig = Moves::<Guarded>::new(protocol, checkpoint);
    rig.keep_unread = false;
    for operation in [501, 601] {
        while let Some(r) = rig.resume(operation) {
            eprintln!("nested merge retirement {protocol:?} checkpoint={checkpoint} operation={operation} setup={:?}", r.phase);
            let s = rig.observed(operation);
            rig.check(operation, &s);
        }
        if operation == 501 {
            let s = rig.observed(501);
            rig.write(41, &s.child, 1, 1, 7, 7, true);
            rig.write(42, &s.child, 81, 40, 3, 3, true);
            rig.write(41, &s.child, 82, 1, 2, 9, false);
        }
    }
    let initial = rig.bound(501);
    let before = rig.observed(601);
    rig.restart();
    assert_eq!(rig.observed(601), before);
    rig.check(601, &before);
    let clock = rig.base.clock;
    let frozen = before
        .sources
        .iter()
        .map(|(g, status, f)| {
            assert!(status.activated.is_some());
            let f = f.clone().unwrap();
            assert_eq!(f.fence.group, group(*g));
            assert_eq!(f.fence.operation, source_fixture::op(601));
            (*g, f)
        })
        .collect::<BTreeMap<_, _>>();
    let first = retire(&mut rig, frozen[&41].clone(), &initial);
    // Reopen the entire fixture with exactly one former owner retired.
    rig.restart();
    check(&mut rig, &first);
    let RetirementRead::Freeze(Some(second_fence)) =
        split::observe(rig.nodes(42), &clock, 42, RetirementQuery::Freeze)
    else {
        panic!("pending second freeze")
    };
    assert_eq!(second_fence, frozen[&42]);
    for n in rig.nodes(42).iter() {
        let app = &n.local().applications[&group(42)];
        assert!(app.status().is_none());
        assert!(app.owner().is_some());
        assert_eq!(app.owner().unwrap().status(), before.sources[1].1);
    }
    assert_eq!(
        observe_target::<Guarded>(
            rig.nodes(42),
            &clock,
            42,
            request_query(second_fence.intent.before(), 40)
        ),
        TargetRead::Rejected(RoutingError::Fenced)
    );
    rig.write(43, &before.child, 1, 1, 7, 7, true);
    rig.write(43, &before.child, 81, 40, 3, 3, true);
    rig.write(43, &before.child, 82, 1, 2, 9, true);
    rig.write(43, &before.child, 83, 40, 4, 7, false);
    let second = retire(&mut rig, second_fence, &initial);
    assert_ne!(
        first.status.retention_release,
        second.status.retention_release
    );
    assert_eq!(
        first.status.publication_index,
        second.status.publication_index
    );
    for r in [&first, &second] {
        if checkpoint {
            reclaim(&mut rig, r.status, &initial);
        }
        check(&mut rig, r);
        creation::abandon(
            std::mem::take(rig.nodes(r.status.source.id.get())),
            r.status.source.id.get(),
        );
    }
    serve_after_retirement(&mut rig, &before);
    for r in [&first, &second] {
        reopen(&mut rig, r.status.source.id.get(), &initial);
        check(&mut rig, r);
    }
    rig.stop();
    std::fs::remove_dir_all(rig.base.root).unwrap();
}
#[test]
fn tcp_nested_merge_retirement_recovers_partial_cleanup_from_wal() {
    history(NativePeerProtocol::TcpTls, false);
}
#[test]
fn tcp_nested_merge_retirement_recovers_partial_cleanup_then_reclaims() {
    history(NativePeerProtocol::TcpTls, true);
}
#[cfg(feature = "quic")]
#[test]
fn quic_nested_merge_retirement_recovers_partial_cleanup_from_wal() {
    history(NativePeerProtocol::Quic, false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_nested_merge_retirement_recovers_partial_cleanup_then_reclaims() {
    history(NativePeerProtocol::Quic, true);
}
