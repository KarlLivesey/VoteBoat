// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use std::io::{Read, Write};

// A bounded peer that either answers one command or waits for the caller to
// close. None also models a stalled TLS handshake after its principal selector.
fn peer(
    listener: TcpListener,
    reply: Option<&'static [u8]>,
) -> std::thread::JoinHandle<Option<Vec<u8>>> {
    std::thread::spawn(move || {
        listener.set_nonblocking(true).unwrap();
        let end = Instant::now() + Duration::from_secs(12);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                Err(e) => panic!("{e}"),
            }
            if Instant::now() >= end {
                return None;
            }
            std::thread::park_timeout(Duration::from_millis(1));
        };
        stream.set_nonblocking(false).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(12)))
            .unwrap();
        let mut line = Vec::new();
        loop {
            let mut byte = [0];
            assert_eq!(stream.read(&mut byte).unwrap(), 1);
            line.push(byte[0]);
            assert!(line.len() <= 256);
            if byte[0] == b'\n' {
                break;
            }
        }
        if let Some(reply) = reply {
            stream.write_all(reply).unwrap();
        } else {
            let mut buffer = [0; 4096];
            let mut received = 0;
            loop {
                let count = stream.read(&mut buffer).unwrap();
                if count == 0 {
                    break;
                }
                received += count;
                assert!(received <= 65536, "unexpected traffic while stalled");
            }
        }
        Some(line)
    })
}

#[test]
fn automatic_read_escapes_silent_replica_with_original_scope() {
    for command in [vec!["read"], vec!["group", "7", "3", "read"]] {
        let mut c = Cluster::new();
        let first = peer(c.take_listener(101), None);
        let second = peer(c.take_listener(102), Some(b"OK value=7\n"));
        let third = c.take_listener(103);
        third.set_nonblocking(true).unwrap();
        let start = Instant::now();
        let output = c.target("auto", &command);
        let first = first.join().unwrap();
        let second = second.join().unwrap();
        assert!(
            output.status.success(),
            "{} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let expected = format!("{}\n", command.join(" ")).into_bytes();
        assert_eq!(first, Some(expected.clone()));
        assert_eq!(second, Some(expected));
        assert_eq!(output.stdout, b"OK value=7\n");
        assert!(start.elapsed() < Duration::from_secs(8));
        assert_eq!(
            third.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        fs::remove_dir_all(c.root.clone()).unwrap();
    }
}

#[test]
fn automatic_read_repeats_an_empty_disconnect_but_not_a_partial_reply() {
    for reply in [
        None,
        Some(b"OK value=7".as_slice()),
        Some(b"\xff\n".as_slice()),
        Some(b"OK value=7\nOK value=8\n".as_slice()),
    ] {
        let mut c = Cluster::new();
        let first = reply_peer(c.take_listener(101), reply);
        let second = c.take_listener(102);
        if reply.is_none() {
            let second = reply_peer(second, Some(b"OK value=8\n"));
            assert_eq!(c.routed(&["read"]), "OK value=8\n");
            assert_eq!(second.join().unwrap(), b"read\n");
        } else {
            second.set_nonblocking(true).unwrap();
            assert!(!c.target("auto", &["read"]).status.success());
            assert_eq!(
                second.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
        assert_eq!(first.join().unwrap(), b"read\n");
        fs::remove_dir_all(c.root.clone()).unwrap();
    }
}

#[test]
fn automatic_read_stalls_cannot_renew_the_absolute_deadline() {
    let mut c = Cluster::new();
    let peers = (101..=103)
        .map(|port| peer(c.take_listener(port), None))
        .collect::<Vec<_>>();
    let start = Instant::now();
    let output = c.target("auto", &["read"]);
    assert!(!output.status.success());
    assert!(start.elapsed() < Duration::from_millis(11500));
    for peer in peers {
        assert_eq!(peer.join().unwrap(), Some(b"read\n".to_vec()));
    }
    fs::remove_dir_all(c.root.clone()).unwrap();
}

#[test]
fn automatic_authenticated_read_escapes_stalled_handshake_tcp() {
    authenticated(false);
}

#[cfg(feature = "quic")]
#[test]
fn automatic_authenticated_read_escapes_stalled_handshake_quic() {
    authenticated(true);
}

fn authenticated(quic: bool) {
    let mut c = Cluster::new();
    c.quic = quic;
    let access = c.root.join("access.txt");
    fs::write(&access, "voteboat-service-access-v1 1\n3 admin 1 1\n").unwrap();
    c.command_access = Some(access);
    c.command_principal = Some(3);
    let listener = c.take_listener(101);
    for node in [2, 3] {
        c.start(node, "create");
    }
    let leader = c.leader();
    assert!(c.ok(leader, &["add", "19901", "7"]).contains("Value(7)"));
    let first = peer(listener, None);
    let start = Instant::now();
    let output = c.target("auto", &["read"]);
    assert_eq!(first.join().unwrap(), Some(b"3\n".to_vec()));
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"OK value=7\n");
    assert!(start.elapsed() < Duration::from_secs(8));
    c.stop();
    fs::remove_dir_all(c.root.clone()).unwrap();
}
