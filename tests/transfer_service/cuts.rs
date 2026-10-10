// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
#[path = "cuts_support.rs"]
pub(super) mod support;

fn ownership(rig: &Cluster, phase: usize) {
    if phase < 4 {
        assert!(rig.ok(20, &["read", "1"]).contains("value=7"));
    } else {
        let out = rig.request(0, 3, 20, &["read", "1"]);
        assert!(!out.status.success());
        assert!(String::from_utf8_lossy(&out.stderr).contains("Fenced"));
        assert!(!rig
            .request(0, 3, 20, &["add", "900", "1", "1"])
            .status
            .success());
    }
    for (g, key, active, value) in [(21, "1", 8, "7"), (22, "200", 9, "11")] {
        if phase >= active {
            assert!(rig
                .ok(g, &["read", key])
                .contains(&format!("value={value}")));
        } else {
            let out = rig.request(0, 3, g, &["add", "901", key, "1"]);
            assert!(!out.status.success(), "target{g} serves in phase{phase}");
            assert!(String::from_utf8_lossy(&out.stderr).contains("InvalidCommand"));
        }
    }
}
fn phase_status(rig: &Cluster, phase: usize) -> String {
    let text = rig.operate("status");
    let expected = [
        "RecordIntent",
        "Stage(",
        "Stage(",
        "Fence(",
        "Export(",
        "Export(",
        "Publish(",
        "Activate",
        "Activate",
        "Complete",
    ][phase];
    assert!(
        text.starts_with(&format!("OK next={expected}")),
        "phase{phase}: {text}"
    );
    text
}
fn history(quic: bool) {
    let mut rig = Cluster::new(quic);
    initialize(&rig);
    let profile = fs::read(rig.root.join("profile")).unwrap();
    for phase in 0..=9 {
        let before = phase_status(&rig, phase);
        ownership(&rig, phase);
        if quic {
            support::checkpoint(&rig);
        }
        rig.crash();
        rig.start("recover");
        assert_eq!(fs::read(rig.root.join("profile")).unwrap(), profile);
        assert_eq!(phase_status(&rig, phase), before);
        ownership(&rig, phase);
        eprintln!("recovered phase={phase} checkpoint={quic} next={before}");
        match phase {
            3 => support::lost_proposal(&mut rig, 20, "fence", "Fence("),
            6 => support::lost_proposal(&mut rig, 1, "publish", "Publish("),
            9 => (),
            _ => {
                rig.operate("step");
            }
        }
    }
    assert!(rig.operate("resume").contains("OK complete"));
    finish(rig);
}
#[test]
fn tcp_wal_recovery_at_every_split_phase_and_lost_fence_publication_receipts() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_checkpoint_recovery_at_every_split_phase_and_lost_fence_publication_receipts() {
    history(true);
}
