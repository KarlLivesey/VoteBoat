// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{tests::stalled_authority, *};
use crate::service_access::{self, Access, ActiveAccess};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::mpsc,
};

fn accept(listener: &TcpListener, stop: &mpsc::Receiver<()>) -> Option<Channel> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if stop.try_recv().is_ok() {
            return None;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                stream.set_nonblocking(true).unwrap();
                return Some(Channel::server(stream, true));
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
            Err(e) => panic!("accept: {e}"),
        }
        assert!(Instant::now() < deadline, "missing route probe");
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

fn request(channel: &mut Channel, access: &ActiveAccess, start: Instant) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut bytes = Vec::new();
    loop {
        if poll(channel, access, start) {
            let mut part = [0; 64];
            match channel.read(&mut part) {
                Ok(0) => panic!("probe closed before request"),
                Ok(n) => bytes.extend_from_slice(&part[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                Err(e) => panic!("request: {e}"),
            }
        }
        if bytes.ends_with(b"\n") {
            return String::from_utf8(bytes).unwrap();
        }
        assert!(bytes.len() < 256);
        assert!(Instant::now() < deadline, "missing authenticated request");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

fn respond(channel: &mut Channel, access: &ActiveAccess, start: Instant, text: &str) {
    channel.write_all(text.as_bytes()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !channel.is_flushed() {
        poll(channel, access, start);
        assert!(Instant::now() < deadline, "unflushed reply");
        std::thread::park_timeout(Duration::from_millis(1));
    }
}

fn serve(
    listener: TcpListener,
    access: ActiveAccess,
    stop: mpsc::Receiver<()>,
    status_stalled: bool,
    terminal: Option<&'static str>,
) -> Vec<String> {
    let mut steps = vec![("status\n", None)];
    if !status_stalled {
        steps[0].1 = Some("OK role=Leader\n");
        steps.push(("manifest-session\n", None));
    }
    if let Some(reply) = terminal {
        steps = vec![
            ("status\n", Some("OK role=Leader\n")),
            ("manifest-session\n", Some(reply)),
        ];
    } else {
        steps.extend([
            ("status\n", Some("OK role=Leader\n")),
            ("manifest-session\n", Some(ACK)),
        ]);
    }
    let start = Instant::now();
    let mut commands = Vec::new();
    let mut held = Vec::new();
    for (expected, reply) in steps {
        let Some(mut channel) = accept(&listener, &stop) else {
            return commands;
        };
        let text = request(&mut channel, &access, start);
        assert_eq!(text, expected);
        commands.push(text);
        if let Some(reply) = reply {
            respond(&mut channel, &access, start, reply);
        }
        if reply.is_none() || reply == Some(ACK) {
            held.push(channel);
        }
    }
    let _ = stop.recv_timeout(Duration::from_secs(5));
    commands
}

fn history(status_stalled: bool, terminal: Option<&'static str>) {
    let (mut discovery, _, listener, request) = stalled_authority();
    let root = std::env::temp_dir().join(format!(
        "voteboat-route-probe-{}-{}",
        std::process::id(),
        listener.local_addr().unwrap().port()
    ));
    std::fs::create_dir(&root).unwrap();
    let policy = root.join("access");
    std::fs::write(&policy, "voteboat-service-access-v1 1\n1 reader 42 1\n").unwrap();
    let tls = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let access = Access::load(&policy, &tls, 1).unwrap();
    let access = ActiveAccess::new(access.policy.generation(), access);
    let (stop, stopped) = mpsc::channel();
    let server =
        std::thread::spawn(move || serve(listener, access, stopped, status_stalled, terminal));
    let deadline = discovery.deadline;
    assert!(matches!(
        discovery.lookup(request, discovery.now()),
        Err(ManifestDiscoveryError::Unavailable)
    ));
    let result = discovery.poll();
    let first = (
        discovery.attempts,
        discovery.generation,
        discovery.active.is_none(),
    );
    let no_observation = discovery.observations.is_empty();
    if result.is_ok() {
        while discovery.active.is_none() && Instant::now() < deadline {
            discovery.poll().unwrap();
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    let admitted = discovery.active.is_some();
    let attempts = discovery.attempts;
    let generation = discovery.generation;
    discovery.close();
    let _ = stop.send(());
    let commands = server.join().unwrap();
    std::fs::remove_dir_all(root).unwrap();
    if let Some(reply) = terminal {
        assert!(result.unwrap_err().to_string().contains(reply.trim()));
        assert_eq!(first, (1, 0, true));
        assert!(!admitted);
        assert!(no_observation);
        assert_eq!(commands, ["status\n", "manifest-session\n"]);
        assert_eq!(discovery.deadline, deadline);
        assert_eq!(discovery.request, Some(request));
        return;
    }
    assert!(result.is_ok(), "{result:?}; commands={commands:?}");
    assert_eq!(first, (1, 0, true));
    assert!(no_observation);
    assert!(admitted);
    assert_eq!((attempts, generation), (2, 1));
    assert_eq!(discovery.deadline, deadline);
    assert_eq!(discovery.request, Some(request));
    assert!(discovery.observations.is_empty());
    assert_eq!(commands.len(), if status_stalled { 3 } else { 4 });
    assert!(Instant::now() < deadline);
}

#[test]
fn timed_out_status_rotates_without_renewing_lookup_budget() {
    history(true, None);
}

#[test]
fn timed_out_manifest_upgrade_rotates_without_admitting_a_source() {
    history(false, None);
}

#[test]
fn refused_or_wrong_version_upgrade_does_not_rotate_or_hide_the_reply() {
    for reply in [
        "ERR AUTHORIZATION\n",
        "OK manifest-v2\n",
        "OK manifest-v1 extra\n",
    ] {
        history(false, Some(reply));
    }
}

#[test]
fn exhausted_connection_budget_cannot_open_another_probe() {
    let (mut discovery, _, listener, request) = stalled_authority();
    assert!(matches!(
        discovery.lookup(request, discovery.now()),
        Err(ManifestDiscoveryError::Unavailable)
    ));
    let deadline = discovery.deadline;
    discovery.attempts = 128;
    assert_eq!(
        discovery.poll().unwrap_err().to_string(),
        "recursive lookup connection budget exhausted"
    );
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(discovery.attempts, 128);
    assert_eq!(discovery.deadline, deadline);
    assert_eq!(discovery.generation, 0);
    assert!(discovery.active.is_none());
    assert!(discovery.observations.is_empty());
}

#[test]
fn interrupted_read_classification_keeps_invalid_and_incomplete_replies_terminal() {
    for reason in [
        "authentication deadline expired",
        "request deadline expired",
        "reply deadline expired",
        "connection closed during request",
        "connection closed without a complete reply",
    ] {
        assert!(repeat_observation(reason));
    }
    for reason in [
        "authentication failed",
        "authentication setup failed",
        "authenticated read failed",
        "invalid reply encoding",
        "multiple reply lines",
        "reply exceeds limit",
        "connection closed with an incomplete reply",
    ] {
        assert!(!repeat_observation(reason));
    }
}
