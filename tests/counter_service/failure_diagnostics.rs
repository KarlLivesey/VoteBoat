// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::{fmt::Write, process::Stdio};

fn status(args: &[&str]) -> Vec<String> {
    match args {
        ["group", group, incarnation, ..] => ["group", group, incarnation, "status"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        _ => vec!["status".into()],
    }
}
fn collect(
    deadline: Instant,
    nodes: &[usize],
    args: &[&str],
    mut probe: impl FnMut(usize, &[String], Instant) -> String,
    mut now: impl FnMut() -> Instant,
) -> String {
    let mut text = format!(
        "diagnostics nodes={} omitted={} evidence=local_volatile\n",
        nodes.len().min(8),
        nodes.len().saturating_sub(8)
    );
    let commands = [status(args), vec!["metrics".into()], vec!["timings".into()]];
    for node in nodes.iter().take(8) {
        for command in &commands {
            if now() >= deadline {
                text.push_str("diagnostic deadline reached; original request remains failed\n");
                return text;
            }
            let reply = probe(*node, command, deadline);
            writeln!(text, "node={node} command={} {reply}", command.join(" ")).unwrap();
        }
    }
    text
}
fn probe(c: &Cluster, node: usize, words: &[String], deadline: Instant) -> String {
    let words = words.iter().map(String::as_str).collect::<Vec<_>>();
    let mut command = c.client(&node.to_string(), &words);
    let gate = loop {
        if Instant::now() >= deadline {
            return "diagnostic fixture gate deadline reached; no caller spawned".into();
        }
        match STORE_SPAWN.try_lock() {
            Ok(gate) => break gate,
            Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                std::thread::park_timeout(Duration::from_millis(1));
            }
        }
    };
    if Instant::now() >= deadline {
        return "diagnostic fixture gate deadline reached; no caller spawned".into();
    }
    let child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    drop(gate);
    let Ok(child) = child else {
        return "diagnostic spawn failed".into();
    };
    complete(child, deadline)
}
fn complete(mut child: Child, deadline: Instant) -> String {
    let stop_reason = loop {
        match child.try_wait() {
            Ok(Some(_)) => break None,
            Ok(None) if Instant::now() < deadline => {
                std::thread::park_timeout(Duration::from_millis(1));
            }
            Ok(None) => {
                let _ = child.kill();
                break Some("diagnostic deadline reached; waiting caller terminated");
            }
            Err(_) => {
                let _ = child.kill();
                break Some("diagnostic poll failed; waiting caller terminated");
            }
        }
    };
    let Ok(output) = child.wait_with_output() else {
        return "diagnostic wait failed".into();
    };
    if let Some(reason) = stop_reason {
        return reason.into();
    }
    let out = &output.stdout[..output.stdout.len().min(4096)];
    let error = &output.stderr[..output.stderr.len().min(4096)];
    format!(
        "exit={:?} stdout={:?} stderr={:?}",
        output.status.code(),
        String::from_utf8_lossy(out),
        String::from_utf8_lossy(error)
    )
}
pub(super) fn snapshot(c: &Cluster, args: &[&str]) -> String {
    let deadline = Instant::now() + Duration::from_secs(3);
    let nodes = c
        .children
        .iter()
        .enumerate()
        .filter_map(|(i, child)| child.as_ref().map(|_| i + 1))
        .collect::<Vec<_>>();
    let text = collect(
        deadline,
        &nodes,
        args,
        |node, words, deadline| probe(c, node, words, deadline),
        Instant::now,
    );
    let _ = fs::write(c.root.join("routing-failure.log"), &text);
    text
}
pub(super) fn check_peer_metrics(metrics: &BTreeMap<String, u64>) {
    assert_eq!(metrics["peer_driver"], 1);
    assert_eq!(metrics["peer_authorized"], 2);
    assert_eq!(metrics["peer_details"], 2);
    assert_eq!(metrics["peer_omitted"], 0);
    assert!(metrics["peer_bound"] > 0);
    assert!(metrics["peer_bound"] <= metrics["peer_connections"]);
    assert_eq!(metrics["peer_send_failed"], 0);
    assert_eq!(metrics["peer_receive_failed"], 0);
    for kind in ["batches", "messages", "bytes"] {
        let sum = metrics
            .iter()
            .filter(|(key, _)| {
                key.starts_with("peer_")
                    && key.ends_with(&format!("_{kind}"))
                    && key
                        .split('_')
                        .nth(1)
                        .is_some_and(|id| id.parse::<u64>().is_ok())
            })
            .map(|(_, value)| value)
            .sum::<u64>();
        assert_eq!(metrics[&format!("outbound_{kind}")], sum);
    }
}

#[test]
fn failure_diagnostics_preserve_scope_and_never_reissue_the_failed_write() {
    let start = Instant::now();
    let mut calls = Vec::new();
    let text = collect(
        start + Duration::from_secs(3),
        &[1, 2, 3],
        &["group", "7", "3", "add", "43", "5"],
        |node, words, _| {
            calls.push((node, words.join(" ")));
            "observed".into()
        },
        || start,
    );
    assert_eq!(calls.len(), 9);
    let (chunks, remainder) = calls.as_chunks::<3>();
    assert!(remainder.is_empty());
    for chunk in chunks {
        assert_eq!(chunk[0].1, "group 7 3 status");
        assert_eq!(chunk[1].1, "metrics");
        assert_eq!(chunk[2].1, "timings");
        assert!(chunk.iter().all(|c| c.0 == chunk[0].0));
    }
    assert!(text.contains("evidence=local_volatile"));
    assert!(!text.contains("add 43"));
}
#[test]
fn failure_diagnostics_stop_at_shared_deadline_and_bound_node_count() {
    let start = Instant::now();
    let deadline = start + Duration::from_secs(3);
    let now = std::cell::Cell::new(start);
    let mut calls = 0;
    let text = collect(
        deadline,
        &[1, 2, 3],
        &["read"],
        |_, _, limit| {
            calls += 1;
            assert_eq!(limit, deadline);
            now.set(deadline);
            "deadline".into()
        },
        || now.get(),
    );
    assert_eq!(calls, 1);
    assert!(text.contains("original request remains failed"));
    let nodes = (1..=16).collect::<Vec<_>>();
    calls = 0;
    let text = collect(
        deadline,
        &nodes,
        &["read"],
        |_, _, _| {
            calls += 1;
            "observed".into()
        },
        || start,
    );
    assert_eq!(calls, 24);
    assert!(text.contains("nodes=8 omitted=8"));
}
#[test]
fn failure_diagnostics_terminate_a_waiting_native_caller() {
    let c = Cluster::new();
    // This checks accepted-child cleanup; lock expiry is a separate phase.
    let child = {
        let _gate = fixture_gate();
        c.client("1", &["status"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let start = Instant::now();
    let result = complete(child, start + Duration::from_millis(50));
    assert!(result.contains("waiting caller terminated"), "{result}");
    assert!(start.elapsed() < Duration::from_secs(2));
    fs::remove_dir_all(&c.root).unwrap();
}
#[test]
fn failure_diagnostics_stop_at_fixture_gate_deadline_without_spawning() {
    let c = Cluster::new();
    let gate = fixture_gate();
    let start = Instant::now();
    let result = probe(&c, 1, &["status".into()], start + Duration::from_millis(50));
    assert!(result.contains("no caller spawned"), "{result}");
    assert!(start.elapsed() < Duration::from_secs(2));
    drop(gate);
    fs::remove_dir_all(&c.root).unwrap();
}
