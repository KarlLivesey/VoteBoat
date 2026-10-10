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
fn input(current: &str, history: &str, nodes: &[usize]) -> String {
    let mut text=format!("voteboat-placement-v1\ngroup 1 1\nplacement 2 false\nsample 1 0 100 1 1024\ncurrent {current}\nhistory {history}\n");
    for &id in nodes {
        let store = lifecycle_store(id);
        text.push_str(&format!(
            "replica {id} {} {} {id} true 1048576 1 100\n",
            store.id.get(),
            store.incarnation.get()
        ));
    }
    text
}
fn generate(c: &Cluster, text: &str, args: &[&str]) -> Output {
    let path = c.root.join("placement.input");
    fs::write(&path, text).unwrap();
    run(Command::new(BIN).arg("placement-plan").arg(path).args(args))
}
fn plan(c: &Cluster, text: &str, args: &[&str]) -> String {
    let out = generate(c, text, args);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}
#[test]
fn placement_cli_preserves_exact_targets_and_recursive_policy() {
    let c = Cluster::new();
    let text = input("3 - m:2 v:1 v:2", "1000", &[1, 2, 4]);
    let learner = plan(&c, &text, &["learner", "1001"]);
    assert!(learner.contains("replica 4 4 404 7\n"));
    assert!(learner.ends_with("learners 1001 3 4 4 m:2 v:1 v:2\n"));
    let replacement = plan(&c, &text, &["replace", "1", "1001", "1002", "retain"]);
    assert!(replacement.ends_with(
        "learners 1001 3 4 4 m:2 v:1 v:2\njoint 1002 4 5 6 1 m:2 v:4 v:2\nfinal 1002 5 6\n"
    ));
    let voters = plan(
        &c,
        &text,
        &["voters", "1002", "retire", "w:2", "3", "v:1", "2", "v:2"],
    );
    assert!(voters.ends_with("joint 1002 3 4 5 - w:2 3 v:1 2 v:2\nfinal 1002 4 5\n"));
    let recursive = text.replace("m:2 v:1 v:2", "w:2 4 m:1 v:1 7 v:2");
    assert!(
        plan(&c, &recursive, &["replace", "1", "1001", "1002", "retire"])
            .contains("w:2 4 m:1 v:4 7 v:2\n")
    );
    fs::remove_dir_all(&c.root).unwrap();
}
#[test]
fn placement_cli_refuses_expired_ambiguous_and_unsafe_inputs_without_output() {
    let c = Cluster::new();
    let text = input("3 - m:2 v:1 v:2", "1000", &[1, 2, 4]);
    for (text, args, reason) in [
        (
            text.replace("sample 1 0 100 1", "sample 1 0 100 100"),
            vec!["learner", "1001"],
            "Expired",
        ),
        (
            text.replace("1048576 1 100", "0 0 100"),
            vec!["learner", "1001"],
            "NoCandidate",
        ),
        (
            format!("{text}replica 4 404 7 4 true 1048576 1 100\n"),
            vec!["learner", "1001"],
            "InvalidSample",
        ),
        (text.clone(), vec!["learner", "1000"], "ReusedOperation"),
        (
            text.clone(),
            vec!["replace", "1", "1001", "1001", "retain"],
            "ReusedOperation",
        ),
        (
            text.clone(),
            vec!["voters", "1001", "retire", "m:2", "v:1", "v:4"],
            "UnpreparedVoter",
        ),
        (
            text.clone(),
            vec!["voters", "1001", "retire", "m:1", "v:1"],
            "TooFewVotingDomains",
        ),
        (
            text.replace("group 1 1", "group 2 1"),
            vec!["learner", "1001"],
            "requires group1",
        ),
        ("x".repeat(65537), vec!["learner", "1001"], "exceeds64KiB"),
    ] {
        let out = generate(&c, &text, &args);
        assert!(!out.status.success());
        assert!(out.stdout.is_empty());
        assert!(
            String::from_utf8_lossy(&out.stderr).contains(reason),
            "expected {reason}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    fs::remove_dir_all(&c.root).unwrap();
}
#[test]
fn exact_planned_store_is_checked_before_opening_runtime_files() {
    let mut c = Cluster::new();
    let text = input("1 - m:3 v:1 v:2 v:3", "-", &[1, 2, 3]);
    let valid = plan(
        &c,
        &text,
        &["voters", "1000", "retire", "m:2", "v:1", "v:2"],
    );
    let path = c.root.join("wrong.plan");
    fs::write(&path, valid.replace("replica 1 1 1 1", "replica 1 1 999 1")).unwrap();
    let root = c.root.join("must-not-create");
    let tls = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    c.listeners.clear();
    c.udp_sockets.clear();
    let out = run(Command::new(BIN)
        .args(["serve", "recover-member"])
        .arg(&root)
        .arg("1")
        .arg(c.base.to_string())
        .arg(tls)
        .arg("--admin-plan")
        .arg(path));
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("differs from exact planned store"));
    assert!(!root.exists());
    fs::remove_dir_all(&c.root).unwrap();
}
fn prepare(quic: bool) -> Cluster {
    let mut c = Cluster::new();
    c.quic = quic;
    c.children.push(None);
    for id in 1..=3 {
        c.start(id, "create");
    }
    c.leader();
    assert!(c.routed(&["add", "700", "42"]).contains("Value(42)"));
    c.stop();
    let text = input("1 - m:3 v:1 v:2 v:3", "-", &[1, 2, 3]);
    let generated = plan(
        &c,
        &text,
        &["voters", "1000", "retire", "m:2", "v:1", "v:2"],
    );
    let path = c.root.join("placement.plan");
    fs::write(&path, generated).unwrap();
    c.admin_plan = Some(path);
    for id in 1..=3 {
        c.start(id, "recover-member");
    }
    finish_lifecycle_operation(&c, &[1, 2], "1000", "701");
    c.stop();
    configure_fourth_tls(&mut c);
    let deployment = c.root.join("placement.deployment");
    fs::write(&deployment,format!("voteboat-deployment-v1\n1 1 1 127.0.0.1:{} node1.voteboat.test\n2 2 1 127.0.0.1:{} node2.voteboat.test\n4 404 7 127.0.0.1:{} node3.voteboat.test\n",c.base+1,c.base+2,c.base+4)).unwrap();
    c.deployment = Some(deployment);
    let text = input("3 - m:2 v:1 v:2", "1000", &[1, 2, 4]);
    let replacement = plan(&c, &text, &["replace", "1", "1001", "1002", "retain"]);
    fs::write(c.admin_plan.as_ref().unwrap(), replacement).unwrap();
    c
}
fn history(quic: bool) {
    let mut c = prepare(quic);
    for id in [1, 2] {
        c.start(id, "recover-member");
    }
    finish_lifecycle_operation(&c, &[1, 2], "1001", "702");
    // No fourth process exists yet: generated future joint cannot promote it.
    for id in [1, 2] {
        let status = c.ok(id, &["configuration-status", "1002"]);
        assert!(status.contains("inconclusive_local_absence"), "{status}");
        c.ok(id, &["checkpoint"]);
    }
    c.stop();
    inspect_lifecycle(&c, &[1, 2], 4, &[1, 2], &[4], &[1000, 1001], 42);
    enroll_checkpointed_fourth(&c);
    for id in [1, 2, 4] {
        c.start(id, "recover-member");
    }
    finish_lifecycle_operation(&c, &[2, 4], "1002", "703");
    for id in [1, 2, 4] {
        c.ok(id, &["checkpoint"]);
    }
    c.stop();
    inspect_lifecycle(&c, &[2, 4], 6, &[2, 4], &[1], &[1000, 1001, 1002], 42);
    // Same saved artifact after checkpoint recovery cannot add new membership entries.
    for id in [1, 2, 4] {
        c.start(id, "recover-member");
    }
    let leader = c.leader();
    assert!(c
        .ok(leader, &["add", "700", "42"])
        .contains("duplicate=true"));
    finish_lifecycle_operation(&c, &[2, 4], "1002", "704");
    c.stop();
    inspect_lifecycle(&c, &[2, 4], 6, &[2, 4], &[1], &[1000, 1001, 1002], 42);
    fs::remove_dir_all(&c.root).unwrap();
}
#[test]
fn generated_placement_executes_and_recovers_tcp() {
    history(false);
}
#[cfg(feature = "quic")]
#[test]
fn generated_placement_executes_and_recovers_quic() {
    history(true);
}
