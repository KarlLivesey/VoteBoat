// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

const HEADER: &str = "voteboat-counter-group-drain-v1\noperation 19701\nsource 3 3 1\n";
fn setup() -> Cluster {
    let mut c = group_admin::setup(false);
    c.leadership_maintenance = true;
    c.node_drain = true;
    let path = c.root.join("group.drain");
    c.group_drain_plan = Some((3, path));
    fs::write(c.root.join("seven.drain"), "voteboat-counter-drain-v1\noperation 19701\nsource 3 3 1\nhandoff 1 1 1\noriginal 9 - m:3 v:1 v:2 v:3\njoint 7001 9 10 11 3 m:2 v:1 v:2\nfinal 7001 10 11\n").unwrap();
    c
}
fn refused(c: &mut Cluster, manifest: &str, expected: &str) {
    fs::write(&c.group_drain_plan.as_ref().unwrap().1, manifest).unwrap();
    c.start(3, "create");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(code) = c.children[2].as_mut().unwrap().try_wait().unwrap() {
            assert!(!code.success());
            c.children[2] = None;
            break;
        }
        assert!(Instant::now() < deadline, "{}", c.service_log(3));
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(c.service_log(3).contains(expected), "{}", c.service_log(3));
    assert!(!c.root.join("3").exists());
}

#[test]
fn group_drain_rejects_invalid_identity_scope_coverage_and_input_before_resources() {
    let mut c = setup();
    for manifest in [
        "bad".to_owned(),
        format!("{HEADER}voter 7 3 seven.drain\nvoter 7 3 seven.drain\n"),
        format!("{HEADER}voter 7 4 seven.drain\n"),
        format!("{HEADER}voter 7 3 absent.drain\n"),
        format!("{HEADER}voter 7 3 seven.drain\n"), // Incomplete local inventory.
        format!("{HEADER}voter 7 3 seven.drain\n").replace("source 3 3 1", "source 3 2 1"),
        format!("{HEADER}voter 7 3 seven.drain\n").replace("operation 19701", "operation 19702"),
        "x".repeat(65537),
    ] {
        refused(&mut c, &manifest, "Error:");
    }
    let rows = (1..=257)
        .map(|i| format!("retained {i} 1 9 3 m:2 v:1 v:2\n"))
        .collect::<String>();
    refused(
        &mut c,
        &format!("{HEADER}{rows}"),
        "exceeds 256 assignments",
    );
    fs::write(c.root.join("seven.drain"), "x".repeat(65537)).unwrap();
    refused(
        &mut c,
        &format!("{HEADER}voter 7 3 seven.drain\n"),
        "Error:",
    );
}

#[test]
fn group_drain_bounds_aggregate_referenced_input_before_resources() {
    let mut c = setup();
    let admin_rows = (1..=20)
        .map(|i| format!("group {i} 1 seven.plan\n"))
        .collect::<String>();
    fs::write(
        c.group_admin_plans.as_ref().unwrap(),
        format!("voteboat-counter-group-admin-v1\n{admin_rows}"),
    )
    .unwrap();
    let path = c.root.join("seven.drain");
    let padded = fs::read_to_string(&path).unwrap().replace(
        "operation 19701",
        &format!("operation {}19701", " ".repeat(60000)),
    );
    fs::write(path, padded).unwrap();
    let rows = (1..=20)
        .map(|i| format!("voter {i} 1 seven.drain\n"))
        .collect::<String>();
    refused(
        &mut c,
        &format!("{HEADER}{rows}"),
        "group drain aggregate input limit",
    );
}
