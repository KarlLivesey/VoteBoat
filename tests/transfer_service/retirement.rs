// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use voteboat::{retirement::*, transfer::*};

fn proof(rig: &Cluster) -> RetirementProof {
    let observations = [
        (1, "intent"),
        (1, "publication"),
        (20, "source"),
        (21, "target"),
        (22, "target"),
    ]
    .map(|(g, kind)| {
        let reply = rig.ok(g, &["transfer-read", kind]);
        let hex = reply.trim().strip_prefix("OK observation ").unwrap();
        let bytes = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect::<Vec<_>>();
        TransferObservation::decode_authenticated(&bytes).unwrap()
    });
    TransferOperation::new(
        source_fixture::intent(),
        source_fixture::op(200),
        source_fixture::op(201),
    )
    .unwrap()
    .retirement_proof(
        &observations,
        source_fixture::group(20),
        source_fixture::op(700),
    )
    .unwrap()
}
fn encoded(proof: &RetirementProof) -> String {
    proof
        .encode(MAX_RETIREMENT_PROOF_BYTES)
        .unwrap()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn retire(rig: &Cluster, release: &str) -> Output {
    rig.client(0, 3, "client")
        .args(["retire", "20", release])
        .output()
        .unwrap()
}
fn require_success(out: Output) -> String {
    assert!(
        out.status.success(),
        "{} {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}
fn checkpoint(rig: &Cluster, index: u64) {
    for n in 1..=3 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            rig.request(n, 3, 20, &["checkpoint"]);
            let s = require_success(rig.request(n, 3, 20, &["status"]));
            let current = s
                .split_whitespace()
                .find_map(|w| w.strip_prefix("checkpoint_index="))
                .unwrap()
                .parse::<u64>()
                .unwrap();
            if current >= index {
                break;
            }
            assert!(Instant::now() < deadline, "checkpoint not published: {s}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
fn lost_wait(rig: &mut Cluster, encoded: &str) {
    let leader = (1..=3)
        .find(|n| require_success(rig.request(*n, 3, 20, &["status"])).contains("role=Leader"))
        .unwrap();
    for n in (1..=3).filter(|n| *n != leader) {
        let at = rig
            .children
            .iter()
            .position(|(g, node, _)| *g == 20 && *node == n)
            .unwrap();
        let (_, _, mut child) = rig.children.remove(at);
        child.kill().unwrap();
        child.wait().unwrap();
    }
    let log = rig.root.join(format!("20-{leader}.log"));
    let offset = fs::read_to_string(&log).unwrap().len();
    let mut waiting = rig
        .client(leader, 3, "command")
        .args(["20", "retire-group", encoded])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !fs::read_to_string(&log).unwrap()[offset..].contains("transfer_proposal accepted") {
        assert!(
            waiting.try_wait().unwrap().is_none(),
            "retirement rejected before admission"
        );
        assert!(Instant::now() < deadline, "retirement admission timeout");
        std::thread::sleep(Duration::from_millis(1));
    }
    waiting.kill().unwrap();
    waiting.wait().unwrap();
    rig.crash();
    rig.start("recover");
}
fn rejected_proofs(rig: &Cluster, original: &RetirementProof) {
    let mut wrong = original.clone();
    wrong.targets.pop();
    // Re-encode an invalid proof by truncating the valid wire bytes instead.
    assert!(wrong.encode(MAX_RETIREMENT_PROOF_BYTES).is_err());
    let hex = encoded(original);
    assert!(!rig
        .request(0, 3, 20, &["retire-group", &hex[..hex.len() - 2]])
        .status
        .success());
    wrong = original.clone();
    wrong.release.fence_index += 1;
    assert!(wrong.encode(MAX_RETIREMENT_PROOF_BYTES).is_err());
    assert!(!rig
        .request(0, 2, 20, &["retire-group", &hex])
        .status
        .success());
    assert!(!rig
        .request(0, 1, 20, &["retirement-status"])
        .status
        .success());
    assert!(rig
        .ok(20, &["retirement-status"])
        .contains("retirement=live"));
}
fn rejected_binding(rig: &Cluster) {
    let profile_path = rig.root.join("profile");
    let original = fs::read_to_string(&profile_path).unwrap();
    let plain = original.replace(
        "voteboat-transfer-profile-v2-retirement",
        "voteboat-transfer-profile-v1",
    );
    fs::write(&profile_path, &plain).unwrap();
    binding_refusal(rig);
    fs::write(&profile_path, original.replacen("200 201", "200  201", 1)).unwrap();
    binding_refusal(rig);
    fs::write(&profile_path, &original).unwrap();
    let marker = rig.root.join("20/1/transfer-source-profile");
    let contents = fs::read(&marker).unwrap();
    fs::remove_file(&marker).unwrap();
    binding_refusal(rig);
    fs::write(&marker, b"truncated").unwrap();
    binding_refusal(rig);
    fs::write(marker, contents).unwrap();
}
fn binding_refusal(rig: &Cluster) {
    let mut child = Command::new(BIN)
        .args(["serve", "recover"])
        .arg(rig.root.join("20/1"))
        .arg("1")
        .arg((rig.base + 128).to_string())
        .arg(rig.tls())
        .arg(rig.root.join("profile"))
        .arg("20")
        .arg(rig.root.join("access"))
        .arg(if rig.quic { "quic" } else { "tcp" })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("incompatible profile entered service");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let out = child.wait_with_output().unwrap();
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("profile binding mismatch"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
fn history(quic: bool) {
    let mut rig = Cluster::with_retirement(quic, true);
    initialize(&rig);
    assert!(!retire(&rig, "700").status.success());
    rig.operate("start");
    let original = proof(&rig);
    rejected_proofs(&rig, &original);
    lost_wait(&mut rig, &encoded(&original));
    require_success(retire(&rig, "700"));
    let status = rig.ok(20, &["retirement-status"]);
    assert!(
        status.contains("retirement=retired") && status.contains("release=700"),
        "{status}"
    );
    let index = status
        .split_whitespace()
        .find_map(|w| w.strip_prefix("index="))
        .unwrap()
        .parse()
        .unwrap();
    assert!(!retire(&rig, "701").status.success());
    let mut conflict = original.clone();
    conflict.release.release = source_fixture::op(701);
    assert!(!rig
        .request(0, 3, 20, &["retire-group", &encoded(&conflict)])
        .status
        .success());
    assert!(!rig
        .request(0, 3, 20, &["transfer-export", "21"])
        .status
        .success());
    assert!(!rig.request(0, 3, 20, &["read", "1"]).status.success());
    assert!(!rig
        .request(0, 3, 20, &["add", "900", "1", "1"])
        .status
        .success());
    if quic {
        checkpoint(&rig, index);
    }
    rig.crash();
    rejected_binding(&rig);
    rig.start("recover");
    let recovered = require_success(retire(&rig, "700"));
    assert!(recovered.contains(&format!("index={index}")), "{recovered}");
    // The exact original command is still a valid idempotent retry after restore.
    assert!(rig
        .ok(20, &["retire-group", &encoded(&original)])
        .contains("Retired"));
    rig.stop_group(1);
    assert!(require_success(retire(&rig, "700")).contains("retirement=retired"));
    rig.stop_group(20);
    for (g, key, op, value) in [(21, "1", "1", "7"), (22, "200", "2", "11")] {
        assert!(rig
            .ok(g, &["add", op, key, value])
            .contains("duplicate: true"));
        rig.ok(g, &["add", "901", key, "2"]);
        assert!(rig
            .ok(g, &["read", key])
            .contains(&format!("value={}", value.parse::<i64>().unwrap() + 2)));
        rig.stop_group(g);
    }
    let root = rig.root.clone();
    drop(rig);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn tcp_retirement_recovers_unread_proposal_and_original_release_from_wal() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn quic_retirement_recovers_unread_proposal_and_retired_checkpoint() {
    history(true);
}
