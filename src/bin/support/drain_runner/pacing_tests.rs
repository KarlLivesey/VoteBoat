// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{recovery_tests::*, *};
use std::sync::mpsc;

fn delayed_ready(succeeds: bool) {
    let (mut runner, listener, access, root) = fixture();
    runner.remaining = if succeeds { 16 } else { 4 };
    let initial_requests = runner.remaining;
    runner.deadline = Instant::now() + Duration::from_secs(4);
    let deadline = runner.deadline;
    let pending = format!("OK sequence=1 operation=2 phase=Active multi=true membership_change=true groups=1 ready=false plan_digest={}\n", "ab".repeat(32));
    let initial = pending.clone();
    let (stop, stopped) = mpsc::channel();
    let server = std::thread::spawn(move || {
        let start = Instant::now();
        let mut commands = Vec::new();
        while let Some(mut channel) = accept(&listener, &stopped) {
            let command = request(&mut channel, &access, start);
            let ready = start.elapsed() >= Duration::from_secs(2);
            let reply = match command.as_str() {
                "drain-group 1 2 0\n" => format!("OK sequence=1 operation=2 offset=0 groups=1 group=1 incarnation=1 configuration=1 done={ready} source=1 source_store=1 source_incarnation=1 kind=retained\n"),
                "resume-drain 1 2\n" | "drain-status 1 2\n" => pending.replace("ready=false", &format!("ready={ready}")),
                "drain-stop 1 2\n" => {
                    assert!(ready, "stop before actual source readiness");
                    "OK sequence=1 operation=2 stopping=true multi=true\n".into()
                }
                _ => panic!("changed original request {command:?}"),
            };
            respond(&mut channel, &access, start, &reply);
            let done = command == "drain-stop 1 2\n";
            commands.push(command);
            if done {
                break;
            }
        }
        commands
    });
    let result = runner.execute_planned_single(&initial);
    let _ = stop.send(());
    let commands = server.join().unwrap();
    std::fs::remove_dir_all(root).unwrap();
    assert_eq!(runner.deadline, deadline);
    assert_eq!(commands.len(), initial_requests - runner.remaining);
    assert_eq!(commands[0], "drain-group 1 2 0\n");
    assert_eq!(commands[1], "resume-drain 1 2\n");
    if succeeds {
        result.unwrap();
        assert!(
            runner.remaining > 0,
            "observations exhausted the whole budget"
        );
        assert_eq!(commands.last().unwrap(), "drain-stop 1 2\n");
    } else {
        assert_eq!(
            result.unwrap_err().to_string(),
            "UNKNOWN drain runner budget expired; rerun the same sequence and operation"
        );
        assert_eq!(runner.remaining, 0);
        assert!(!commands.iter().any(|c| c.starts_with("drain-stop ")));
    }
}

#[test]
fn pending_rounds_preserve_request_budget_until_source_is_actually_ready() {
    delayed_ready(true);
}

#[test]
fn unready_source_cannot_replenish_requests_or_request_stop() {
    delayed_ready(false);
}
