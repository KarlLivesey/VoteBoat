// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::io::{Read, Seek, SeekFrom};
fn tail(path: &std::path::Path) -> String {
    let Ok(mut file) = fs::File::open(path) else {
        return "log unavailable".into();
    };
    let mut bytes = Vec::new();
    if let Ok(length) = file.seek(SeekFrom::End(0)) {
        let _ = file.seek(SeekFrom::Start(length.saturating_sub(4096)));
        let _ = file.take(4096).read_to_end(&mut bytes);
    }
    String::from_utf8_lossy(&bytes).into_owned()
}
fn status(c: &Cluster, node: usize, deadline: Instant) -> String {
    let gate = loop {
        if Instant::now() >= deadline {
            return "diagnostic deadline reached before spawn".into();
        }
        match SPAWNS.try_lock() {
            Ok(gate) => break gate,
            Err(std::sync::TryLockError::Poisoned(error)) => break error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                std::thread::park_timeout(Duration::from_millis(1))
            }
        }
    };
    if Instant::now() >= deadline {
        return "diagnostic deadline reached before spawn".into();
    }
    let child = c
        .client(node, 3, &["status"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();
    drop(gate);
    let Ok(mut child) = child else {
        return "diagnostic spawn failed".into();
    };
    let reason = loop {
        match child.try_wait() {
            Ok(Some(_)) => break None,
            Ok(None) if Instant::now() < deadline => {
                std::thread::park_timeout(Duration::from_millis(1))
            }
            _ => {
                let _ = child.kill();
                break Some("diagnostic deadline/poll failure; child terminated");
            }
        }
    };
    let output = child.wait_with_output();
    if let Some(reason) = reason {
        return reason.into();
    }
    match output {
        Ok(out) => format!(
            "exit={:?} stdout={:?} stderr={:?}",
            out.status.code(),
            String::from_utf8_lossy(&out.stdout[..out.stdout.len().min(4096)]),
            String::from_utf8_lossy(&out.stderr[..out.stderr.len().min(4096)])
        ),
        Err(_) => "diagnostic wait failed".into(),
    }
}
pub(super) fn snapshot(c: &Cluster, phase: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(3);
    let mut text =
        format!("lookup diagnostics phase={phase} source=auto evidence=sequential_local\n");
    for node in 1..=3 {
        let probe = if c.children[node - 1].is_some() {
            status(c, node, deadline)
        } else {
            "node stopped".into()
        };
        text.push_str(&format!(
            "node={node} status={probe} log_tail={:?}\n",
            tail(&c.root.join(format!("{node}.log")))
        ));
    }
    text
}
