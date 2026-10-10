// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{drain::*, identity::*, native::drain_journal::*, secure::PeerIdentity};

#[test]
fn planned_runner_drains_original_source_after_leader_change_tcp() {
    history(false);
}

#[cfg(feature = "quic")]
#[test]
fn planned_runner_drains_original_source_after_leader_change_quic() {
    history(true);
}

fn history(quic: bool) {
    let (mut c, source, target) = membership_drain::prepare(quic);
    let plan = c.membership_drain.as_ref().unwrap().1.clone();
    let original = fs::read(&plan).unwrap();
    let target_text = target.to_string();
    let moved = c.ok(
        source,
        &["move-leader", "19761", "1", &target_text, &target_text, "1"],
    );
    assert!(moved.contains("operation=19761"), "{moved}");
    let (leader, receipt) = leadership::status(&mut c, "19761", "phase=Completed");
    assert_eq!(leader, target, "{receipt}");
    assert!(c.ok(source, &["status"]).contains("role=Follower"));
    assert_eq!(fs::read(&plan).unwrap(), original);
    finish(&mut c, source);
    assert_eq!(fs::read(&plan).unwrap(), original);
    let _gate = fixture_gate();
    let owner = PeerIdentity {
        node: NodeId::new(source as u64).unwrap(),
        store: StoreIdentity {
            id: StoreId::new(source as u128).unwrap(),
            incarnation: StoreIncarnation::new(1).unwrap(),
        },
    };
    let journal = NativeDrainJournal::recover(
        FileDrainRecord::new(c.root.join(source.to_string()).join("drain.record")),
        owner,
    )
    .ok()
    .unwrap();
    let durable = journal.latest().unwrap().unwrap();
    assert_eq!(durable.owner, owner);
    assert_eq!(durable.sequence, 1);
    assert_eq!(durable.request.operation.get(), 19701);
    assert_eq!(durable.phase, DrainPhase::Active);
    assert!(durable.plan.is_some());
    assert_eq!(durable.request.groups.len(), 1);
    assert_eq!(durable.request.groups[0].configuration.get(), 1);
}
