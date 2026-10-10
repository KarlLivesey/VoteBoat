// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::service_access::{self, Access, ActiveAccess, Channel};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
};
use voteboat::{identity::*, runtime::MonoTime};

#[test]
fn uncertain_configuration_attempts_require_observation() {
    for reason in [
        "request deadline expired",
        "reply deadline expired",
        "connection closed during request",
        "connection closed without a complete reply",
    ] {
        assert_eq!(
            configuration_attempt(Attempt::Interrupted(reason), 3).unwrap(),
            ConfigurationReply::ObserveSource
        );
    }
    assert_eq!(
        configuration_attempt(Attempt::Unavailable, 3).unwrap(),
        ConfigurationReply::AnotherPeer
    );
}

#[test]
fn invalid_configuration_transport_is_not_hidden_by_retry() {
    for reason in [
        "authentication failed",
        "authentication deadline expired",
        "authentication setup failed",
        "authenticated read failed",
        "invalid reply encoding",
        "multiple reply lines",
        "reply exceeds limit",
    ] {
        assert!(configuration_attempt(Attempt::Interrupted(reason), 3).is_err());
    }
    for reply in ["ERR Unauthorized\n", "OK operation=4 action=completed\n"] {
        assert!(configuration_attempt(Attempt::Reply(reply.into()), 3).is_err());
    }
}

pub(super) fn accept(listener: &TcpListener, stop: &mpsc::Receiver<()>) -> Option<Channel> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if stop.try_recv().is_ok() {
            return None;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(true).unwrap();
                return Some(Channel::server(stream, true));
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => (),
            Err(error) => panic!("accept: {error}"),
        }
        assert!(Instant::now() < deadline, "missing next runner request");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn poll(channel: &mut Channel, access: &ActiveAccess, start: Instant) -> bool {
    channel
        .poll(
            Some(access),
            service_access::server_local(1, StoreSession::new(1).unwrap()),
            SecureSessionGeneration::new(1).unwrap(),
            MonoTime(start.elapsed().as_millis() as u64),
        )
        .unwrap()
}
pub(super) fn request(channel: &mut Channel, access: &ActiveAccess, start: Instant) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut command = Vec::new();
    loop {
        if !poll(channel, access, start) {
            assert!(Instant::now() < deadline, "authentication deadline");
            std::thread::park_timeout(Duration::from_millis(1));
            continue;
        }
        let mut bytes = [0; 256];
        match channel.read(&mut bytes) {
            Ok(0) => panic!("runner closed before its request"),
            Ok(n) => command.extend_from_slice(&bytes[..n]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => (),
            Err(error) => panic!("request: {error}"),
        }
        if command.ends_with(b"\n") {
            return String::from_utf8(command).unwrap();
        }
        assert!(command.len() < 256);
        assert!(Instant::now() < deadline, "request deadline");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
pub(super) fn respond(channel: &mut Channel, access: &ActiveAccess, start: Instant, text: &str) {
    channel.write_all(text.as_bytes()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !channel.is_flushed() {
        poll(channel, access, start);
        assert!(Instant::now() < deadline, "response deadline");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
fn script(listener: TcpListener, access: ActiveAccess, stop: mpsc::Receiver<()>) -> Vec<String> {
    let start = Instant::now();
    let pending = format!("OK sequence=1 operation=2 phase=Active membership_change=true configuration_operation=3 plan_digest={} ready=false\n", "ab".repeat(32));
    let ready = pending.replace("ready=false", "ready=true");
    let steps = [
        ("drain-node 1 2\n", Some(pending.as_str())),
        ("resume-drain 1 2\n", Some(pending.as_str())),
        ("configure 3\n", None),
        ("drain-status 1 2\n", Some(pending.as_str())),
        ("configure 3\n", Some("OK operation=3 action=completed\n")),
        ("drain-status 1 2\n", Some(ready.as_str())),
        (
            "drain-stop 1 2\n",
            Some("OK sequence=1 operation=2 stopping=true\n"),
        ),
    ];
    let mut commands = Vec::new();
    let mut held = Vec::new();
    for (expected, reply) in steps {
        let Some(mut channel) = accept(&listener, &stop) else {
            break;
        };
        let command = request(&mut channel, &access, start);
        assert_eq!(command, expected);
        commands.push(command);
        if let Some(reply) = reply {
            respond(&mut channel, &access, start, reply);
        } else {
            // Keep the authenticated request open beyond its attempt deadline.
            // This represents an unknown outcome, never a committed receipt.
            held.push(channel);
        }
    }
    commands
}

pub(super) fn fixture() -> (Runner, TcpListener, ActiveAccess, std::path::PathBuf) {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let root = std::env::temp_dir().join(format!(
        "voteboat-drain-recovery-{}-{}",
        std::process::id(),
        listener.local_addr().unwrap().port()
    ));
    std::fs::create_dir(&root).unwrap();
    let policy = root.join("access");
    std::fs::write(&policy, "voteboat-service-access-v1 1\n3 admin 1 1\n").unwrap();
    let tls = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let access = Access::load(&policy, &tls, 1).unwrap();
    let active = ActiveAccess::new(access.policy.generation(), access);
    let endpoint = Endpoint {
        node: 1,
        address: listener.local_addr().unwrap(),
        server_name: "node1.voteboat.test".into(),
    };
    let args = [
        "1",
        "2",
        "--service-tls",
        tls.to_str().unwrap(),
        "--principal",
        "3",
    ]
    .map(str::to_owned);
    let mut runner = configured(10000, 3, &args).unwrap();
    runner.auth = ClientAccess::load(&tls, 3, std::slice::from_ref(&endpoint)).unwrap();
    runner.endpoints = vec![endpoint];
    runner.source = 0;
    (runner, listener, active, root)
}

fn history(request_limit: Option<usize>) {
    let (mut runner, listener, active, root) = fixture();
    if let Some(limit) = request_limit {
        runner.remaining = limit;
    }
    let deadline = runner.deadline;
    let remaining = runner.remaining;
    let (stop, stopped) = mpsc::channel();
    let server = std::thread::spawn(move || script(listener, active, stopped));
    let result = runner.execute();
    let _ = stop.send(());
    let commands = server.join().unwrap();
    std::fs::remove_dir_all(root).unwrap();
    if request_limit.is_some() {
        assert_eq!(
            result.unwrap_err().to_string(),
            "UNKNOWN drain runner budget expired; rerun the same sequence and operation"
        );
        assert_eq!(commands.len(), 3);
        assert_eq!(runner.remaining, 0);
    } else {
        assert!(result.is_ok(), "{result:?}; commands={commands:?}");
        assert_eq!(commands.len(), 7);
        assert_eq!(runner.remaining, remaining - 7);
    }
    assert_eq!(runner.deadline, deadline);
    assert!(Instant::now() < deadline);
}

#[test]
fn configuration_reply_timeout_observes_original_journal_before_retry_and_stop() {
    history(None);
}

#[test]
fn configuration_reply_timeout_cannot_replenish_request_budget_or_stop_source() {
    history(Some(3));
}
