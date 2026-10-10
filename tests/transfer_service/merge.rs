// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{cuts::support, *};
use voteboat::{
    identity::RoutingSchemeId,
    routing::{BucketRange, PartitionScheme},
    scope::ScopeImage,
    transfer::*,
};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}
fn plan(rig: &Cluster) -> TransferOperation {
    let text = fs::read_to_string(rig.root.join("profile")).unwrap();
    let intent = TransferIntent::decode(&unhex(
        text.lines()
            .nth(1)
            .unwrap()
            .strip_prefix("intent ")
            .unwrap(),
    ))
    .unwrap();
    TransferOperation::new(intent, source_fixture::op(200), source_fixture::op(201)).unwrap()
}
fn observations(rig: &Cluster, plan: &TransferOperation) -> Vec<TransferObservation> {
    plan.reads()
        .into_iter()
        .map(|read| {
            let kind = match read.kind {
                TransferReadKind::Intent => "intent",
                TransferReadKind::Publication => "publication",
                TransferReadKind::Source => "source",
                TransferReadKind::Target => "target",
            };
            let reply = rig.ok(read.group.id.get(), &["transfer-read", kind]);
            TransferObservation::decode_authenticated(&unhex(
                reply.trim().strip_prefix("OK observation ").unwrap(),
            ))
            .unwrap()
        })
        .collect()
}
fn exported_image(rig: &Cluster, export: &TransferExport) -> ScopeImage {
    let reply = rig.ok(
        export.source.id.get(),
        &["transfer-export", &export.target.id.get().to_string()],
    );
    let bytes = unhex(reply.trim().strip_prefix("OK image ").unwrap());
    ScopeImage::new(
        u64::from_le_bytes(bytes[..8].try_into().unwrap()),
        PartitionScheme {
            id: RoutingSchemeId::new(u128::from_le_bytes(bytes[8..24].try_into().unwrap()))
                .unwrap(),
            version: u32::from_le_bytes(bytes[24..28].try_into().unwrap()),
        },
        BucketRange::new(
            u16::from_le_bytes(bytes[28..30].try_into().unwrap()),
            u16::from_le_bytes(bytes[30..32].try_into().unwrap()),
        )
        .unwrap(),
        u64::from_le_bytes(bytes[32..40].try_into().unwrap()),
        bytes[40..].to_vec(),
    )
    .unwrap()
}
fn lost_import(rig: &mut Cluster) {
    let plan = plan(rig);
    let reads = observations(rig, &plan);
    let mut images = Vec::new();
    let import = loop {
        let refs = images
            .iter()
            .map(|(source, target, image)| TransferImage {
                source: *source,
                target: *target,
                image,
            })
            .collect::<Vec<_>>();
        match plan.next(&reads, &refs).unwrap() {
            TransferAction::Export(export) => {
                let image = exported_image(rig, &export);
                images.push((export.source, export.target, image));
            }
            TransferAction::Import(import) => break import,
            other => panic!("expected combined import, got {other:?}"),
        }
    };
    assert_eq!(images.len(), 2);
    let words = vec![
        "transfer-step".into(),
        "import".into(),
        hex(&import.encode(65536).unwrap()),
    ];
    support::lost_command(rig, 22, &words, "Export(");
}
fn lost_activation(rig: &mut Cluster) {
    let plan = plan(rig);
    let TransferAction::Activate { target, activation } =
        plan.next(&observations(rig, &plan), &[]).unwrap()
    else {
        panic!("expected activation");
    };
    let words = vec![
        "transfer-step".into(),
        "activate".into(),
        activation.metadata_configuration.get().to_string(),
        hex(&activation.decision.encode(65536).unwrap()),
    ];
    support::lost_command(rig, target.id.get(), &words, "Activate");
}
fn ownership(rig: &Cluster, phase: usize) {
    for (g, key, value, fenced_at) in [(20, "1", "7", 3), (21, "200", "11", 4)] {
        if phase < fenced_at {
            assert!(rig
                .ok(g, &["read", key])
                .contains(&format!("value={value}")));
        } else {
            let out = rig.request(0, 3, g, &["read", key]);
            assert!(!out.status.success());
            assert!(String::from_utf8_lossy(&out.stderr).contains("Fenced"));
            assert!(!rig
                .request(0, 3, g, &["add", "900", key, "1"])
                .status
                .success());
        }
    }
    if phase < 7 {
        let out = rig.request(0, 3, 22, &["add", "901", "1", "1"]);
        assert!(!out.status.success(), "target served in phase {phase}");
    } else {
        assert!(rig.ok(22, &["read", "1"]).contains("value=7"));
        assert!(rig.ok(22, &["read", "200"]).contains("value=11"));
    }
}
fn retire(rig: &Cluster, source: &str, release: &str) -> Output {
    rig.client(0, 3, "client")
        .args(["retire", source, release])
        .output()
        .unwrap()
}
fn retired(rig: &Cluster, source: &str, release: &str) -> String {
    let out = retire(rig, source, release);
    assert!(
        out.status.success(),
        "{} {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8(out.stdout).unwrap();
    assert!(
        text.contains(&format!("source={source}")) && text.contains(&format!("release={release}")),
        "{text}"
    );
    // A fresh read prefix can advance; the original retirement record cannot.
    text.lines()
        .find(|line| line.contains("retirement=retired"))
        .unwrap()
        .split_whitespace()
        .filter(|field| !field.starts_with("read_index="))
        .collect::<Vec<_>>()
        .join(" ")
}
fn finish(mut rig: Cluster) {
    assert!(rig.operate("resume").contains("OK complete"));
    let first = retired(&rig, "20", "700");
    assert!(rig
        .ok(21, &["retirement-status"])
        .contains("retirement=live"));
    let second = retired(&rig, "21", "701");
    for g in [20, 21] {
        assert!(!rig
            .request(0, 3, g, &["transfer-export", "22"])
            .status
            .success());
    }
    if rig.quic {
        support::checkpoint(&rig);
    }
    rig.crash();
    rig.start("recover");
    rig.stop_group(1);
    assert_eq!(retired(&rig, "20", "700"), first);
    assert_eq!(retired(&rig, "21", "701"), second);
    assert!(!retire(&rig, "21", "702").status.success());
    rig.stop_group(20);
    rig.stop_group(21);
    for (key, id, value) in [("1", "1", "7"), ("200", "2", "11")] {
        assert!(rig
            .ok(22, &["add", id, key, value])
            .contains("duplicate: true"));
        rig.ok(22, &["add", &format!("90{id}"), key, "2"]);
        assert!(rig
            .ok(22, &["read", key])
            .contains(&format!("value={}", value.parse::<i64>().unwrap() + 2)));
    }
    rig.stop_group(22);
    let root = rig.root.clone();
    drop(rig);
    fs::remove_dir_all(root).unwrap();
}
fn initialize_merge(rig: &Cluster) {
    rig.ok(1, &["initialize"]);
    rig.ok(1, &["grant"]);
    for (g, key, id, value) in [(20, "1", "1", "7"), (21, "200", "2", "11")] {
        rig.ok(g, &["initialize"]);
        rig.ok(g, &["add", id, key, value]);
    }
    assert!(!rig.request(0, 3, 20, &["read", "200"]).status.success());
    assert!(!rig.request(0, 3, 21, &["read", "1"]).status.success());
    assert!(!retire(rig, "20", "700").status.success());
}
fn history(quic: bool) {
    let mut rig = Cluster::with_profile(quic, true, true);
    initialize_merge(&rig);
    let phases = [
        "RecordIntent",
        "Stage(",
        "Fence(",
        "Fence(",
        "Export(",
        "Publish(",
        "Activate",
        "Complete",
    ];
    for (phase, expected) in phases.into_iter().enumerate() {
        let before = rig.operate("status");
        assert!(
            before.starts_with(&format!("OK next={expected}")),
            "phase={phase}: {before}"
        );
        ownership(&rig, phase);
        if quic {
            support::checkpoint(&rig);
        }
        rig.crash();
        rig.start("recover");
        assert_eq!(rig.operate("status"), before);
        ownership(&rig, phase);
        match phase {
            4 => lost_import(&mut rig),
            6 => lost_activation(&mut rig),
            7 => (),
            _ => {
                rig.operate("step");
            }
        }
        eprintln!("merge recovered phase={phase} checkpoint={quic}");
    }
    finish(rig);
}
#[test]
fn tcp_merge_recovers_both_sources_import_activation_and_independent_retirement() {
    history(false);
}
#[test]
fn tcp_operator_collects_both_images_before_importing_a_merge() {
    let rig = Cluster::with_profile(false, true, true);
    initialize_merge(&rig);
    assert!(rig.operate("start").contains("OK complete"));
    finish(rig);
}
#[cfg(feature = "quic")]
#[test]
fn quic_merge_recovers_checkpoints_both_sources_and_independent_retirement() {
    history(true);
}
