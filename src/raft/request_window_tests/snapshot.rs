// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Genuine snapshot sends with exact verified-pin fixture boundaries.
use super::*;

fn required(effects: Vec<Effect>, peer: u64) -> (RequestContext, SnapshotRef) {
    effects
        .into_iter()
        .find_map(|e| match e {
            Effect::SnapshotRequired {
                to,
                context,
                reference,
            } if to == node(peer) => Some((context, reference)),
            _ => None,
        })
        .expect("missing original snapshot requirement")
}

#[test]
fn relay_snapshot_scope_keeps_exact_original_boundary_and_retires_on_reply() {
    let (mut core, reference, snapshot) = compacted_joint();
    let probe = sent(core.step(Event::Heartbeat).unwrap(), 2);
    let behind = response(
        &probe,
        Rpc::Appended {
            success: false,
            matching_index: 0,
        },
    );
    let (context, actual) = required(core.step(Event::Receive(behind)).unwrap(), 2);
    assert_eq!(actual, reference);
    let original = sent(
        core.snapshot_send(node(2), context, actual, snapshot)
            .unwrap()
            .into_owned(),
        2,
    );
    let scope = window(&core, &original);
    let mut wrong = original.clone();
    if let Rpc::Snapshot { snapshot } = &mut wrong.rpc {
        snapshot.metadata.index += 1;
    }
    assert!(matches!(
        core.request_window(&wrong),
        OriginalScope::Retired
    ));
    core.step(Event::Receive(response(
        &original,
        Rpc::SnapshotAck {
            index: actual.index,
        },
    )))
    .unwrap();
    assert!(!scope.current(&core));
}

fn committed_pin(source: &mut Raft, historical: bool) -> (SnapshotRef, Snapshot) {
    let index = source.state().last_index() - u64::from(historical);
    let snapshot = Snapshot {
        metadata: crate::snapshot::SnapshotMetadata {
            bootstrap: source.state().bootstrap.clone(),
            membership: source.state().checkpoint_membership(index).unwrap(),
            index,
            term: source.state().term_at(index).unwrap(),
            application_schema: 1,
        },
        application: vec![7],
    };
    let reference = SnapshotRef {
        store: source.binding.identity,
        group: source.state().bootstrap.group,
        generation: SnapshotGeneration::new(1).unwrap(),
        configuration: snapshot.metadata.configuration(),
        index,
        term: snapshot.metadata.term,
        application_schema: 1,
        file_bytes: 128,
        checksum: 7,
    };
    // Existing contract fixture asserts a host-verified pin; no native filesystem
    // publication claim. Complete actual compaction before requesting the image.
    let compact = source.begin_compact(reference).unwrap().into_owned();
    durable(source, compact);
    (reference, snapshot)
}

#[test]
fn relay_committed_snapshot_repair_preserves_historical_and_final_scope_variants() {
    for historical in [true, false] {
        let (mut source, _) = finalized_repair_pair();
        let (reference, snapshot) = committed_pin(&mut source, historical);
        source = source.with_committed_snapshot_repair();
        let campaign = source.step(Event::Campaign).unwrap();
        let (context, actual) = required(durable(&mut source, campaign), 4);
        assert_eq!(actual, reference);
        let original = sent(
            source
                .snapshot_send(node(4), context, actual, snapshot)
                .unwrap()
                .into_owned(),
            4,
        );
        assert_eq!(source.role(), Role::Candidate);
        if historical {
            assert_ne!(original.configuration, source.membership().id());
            assert!(matches!(original.rpc, Rpc::LearnerRepairSnapshot { .. }));
        } else {
            assert_eq!(original.configuration, source.membership().id());
            assert!(matches!(
                original.rpc,
                Rpc::CommittedLearnerRepairSnapshot { .. }
            ));
        }
        let scope = window(&source, &original);
        assert!(scope.current(&source));
        source
            .step(Event::SetCampaigning { enabled: false })
            .unwrap();
        assert!(!scope.current(&source));
    }
}

#[test]
fn relay_historical_batched_repair_does_not_require_current_configuration() {
    let (mut source, _) = finalized_repair_pair();
    let campaign = source.step(Event::Campaign).unwrap();
    let original = durable(&mut source, campaign)
        .into_iter()
        .find_map(|e| match e {
            Effect::Send(m) if m.to == node(4) && matches!(m.rpc, Rpc::LearnerRepair { .. }) => {
                Some(m)
            }
            _ => None,
        })
        .unwrap();
    assert_ne!(original.configuration, source.membership().id());
    assert_eq!(source.role(), Role::Candidate);
    let scope = window(&source, &original);
    let reply = response(
        &original,
        Rpc::LearnerRepaired {
            success: true,
            matching_index: source.repair_requests[&node(4)].end,
            matching_term: source
                .state()
                .term_at(source.repair_requests[&node(4)].end)
                .unwrap(),
        },
    );
    source.step(Event::Receive(reply)).unwrap();
    assert!(!scope.current(&source));
}
