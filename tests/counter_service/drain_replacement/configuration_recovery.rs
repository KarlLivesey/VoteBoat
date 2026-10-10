// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const OP: &str = "24101";
const PLAN: &str = "voteboat-counter-admin-v1\nplacement 2 false\nreplica 1 1 1 1\nreplica 2 2 2 1\nreplica 3 3 3 1\njoint 24101 1 2 3 - m:3 v:1 v:2 v:3\nfinal 24101 2 3\n";

fn setup(quic: bool) -> Cluster {
    let mut c = drain::cluster(quic);
    c.stop();
    let path = c.root.join("original-configuration.admin");
    fs::write(&path, PLAN).unwrap();
    c.admin_plan = Some(path);
    c.remote_admin = true;
    for id in 1..=3 {
        c.start(id, "recover-member");
    }
    c
}
fn interrupted(c: &mut Cluster) -> (usize, String) {
    let sampled = c.leader();
    let target = sampled % 3 + 1;
    c.ok(
        sampled,
        &[
            "drain-node",
            "1",
            "19701",
            "1",
            &target.to_string(),
            &target.to_string(),
            "1",
        ],
    );
    let (_, handoff) = leadership::status(c, "19701", "phase=Completed");
    let leader = c.leader();
    assert_ne!(leader, sampled);
    for id in (1..=3).filter(|id| *id != leader) {
        drain::kill(c, id);
    }
    let unread = UnobservedCommand::send(c, leader, &format!("configure {OP}"));
    wait_administration_event(c, leader, "phase=configuration_queued");
    let pending = c.wait_configuration_status(leader, OP);
    for field in [
        "committed=NotFoundLocally",
        "accepted=Joint",
        "action=wait_for_commit",
    ] {
        assert!(pending.contains(field), "{pending}");
    }
    drain::kill(c, leader);
    unread.disconnect();
    let saved = stored(c, leader);
    assert!(saved.entries.iter().any(|entry| entry.index > saved.commit_index
        && matches!(&entry.payload, EntryPayload::Configuration(r) if r.operation.get() == 24101)));
    for id in (1..=3).filter(|id| *id != leader) {
        c.start(id, "recover-member");
    }
    assert_ne!(c.leader(), leader);
    c.start(leader, "recover-member");
    (sampled, handoff)
}
fn history(quic: bool) {
    let mut c = setup(quic);
    let (sampled, handoff) = interrupted(&mut c);
    // This exact first endpoint was sampled before the handoff; its durable
    // drain gate now suppresses campaigning. It is not current authority.
    configure(&mut c, OP, Some(sampled));
    for id in 1..=3 {
        let status = c.wait_configuration_status(id, OP);
        assert!(status.contains("committed=Final"), "{status}");
        assert!(status.contains("evidence=local_durable"), "{status}");
    }
    assert_eq!(
        fs::read_to_string(c.admin_plan.as_ref().unwrap()).unwrap(),
        PLAN
    );
    let duplicate = leader_request(&mut c, &["configure", OP]);
    assert!(duplicate.contains("operation=24101"), "{duplicate}");
    assert!(duplicate.contains("action=completed"), "{duplicate}");
    assert!(authenticated_write(&c, &["add", "19700", "7"]).contains("duplicate=true"));
    assert!(authenticated_write(&c, &["add", "24110", "3"]).contains("Value(10)"));
    if quic {
        let leader = c.leader();
        let minimum = field(&c.ok(leader, &["status"]), "committed=");
        for id in 1..=3 {
            checkpoint_after(&c, id, minimum);
        }
    }
    c.stop();
    for id in 1..=3 {
        let saved = stored(&c, id);
        let membership = saved.membership_at(saved.commit_index).unwrap();
        assert_eq!(membership.id().get(), 3);
        assert!(membership.joint().is_none());
        c.start(id, "recover-member");
    }
    assert_eq!(leader_request(&mut c, &["configure", OP]), duplicate);
    let (_, recovered) = leadership::status(&mut c, "19701", "phase=Completed");
    assert_eq!(recovered, handoff);
    assert!(authenticated_write(&c, &["add", "19700", "7"]).contains("duplicate=true"));
    assert!(authenticated_write(&c, &["add", "24110", "3"]).contains("duplicate=true"));
    assert_eq!(c.routed(&["read"]), "OK value=10\n");
    assert_eq!(
        fs::read_to_string(c.admin_plan.as_ref().unwrap()).unwrap(),
        PLAN
    );
    c.stop();
    fs::remove_dir_all(&c.root).unwrap();
}
#[test]
fn planned_configuration_recovers_unread_proposal_and_sampled_follower_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn planned_configuration_recovers_unread_proposal_and_sampled_follower_quic() {
    history(true);
}

#[test]
fn planned_configuration_classification_keeps_unrelated_replies_terminal() {
    let args = ["configure", OP];
    for text in [
        "ERR NOT_LEADER\n",
        "UNKNOWN LeadershipChanged; retry the same configuration operation ID and record\n",
        "UNKNOWN authenticated read failed; retry the same configuration operation ID and record\n",
        "UNKNOWN exact record locally durable but not committed; preserve original record\n",
    ] {
        assert!(retryable_leader_response(&args, text));
    }
    for text in [
        "ERR AUTHORIZATION\n",
        "ERR administration blocked; inspect server log\n",
        "ERR operation absent from provisioned plan\n",
        "ERR configuration operation conflicts with retained record\n",
        "ERR not_proposed=Busy\n",
        "UNKNOWN unspecified outcome\n",
        "UNKNOWN LeadershipChanged; retry the same configuration operation ID and record\nextra\n",
    ] {
        assert!(!retryable_leader_response(&args, text));
    }
    assert!(!retryable_leader_response(
        &["configure", OP, "extra"],
        "ERR NOT_LEADER\n"
    ));
}
