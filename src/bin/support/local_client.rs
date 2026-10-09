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
//! Bounded local routing. Only an explicit NotLeader proves a sent write was not proposed.
use super::{
    service_access::{Channel, ClientAccess},
    setup::Failure,
};
use std::{
    io::{Read, Write},
    net::{Ipv4Addr, TcpStream},
    time::{Duration, Instant},
};
use voteboat::runtime::MonoTime;
const NOT_LEADER: &str = "ERR NOT_LEADER\n";
enum Attempt {
    Unavailable,
    Interrupted(&'static str),
    Reply(String),
}
fn exchange(
    base: u16,
    id: u64,
    text: &[u8],
    deadline: Instant,
    auth: Option<&ClientAccess>,
    start: Instant,
) -> Attempt {
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        return Attempt::Unavailable;
    };
    let mut stream = match TcpStream::connect_timeout(
        &(Ipv4Addr::LOCALHOST, base + 100 + id as u16).into(),
        remaining.min(Duration::from_secs(2)),
    ) {
        Ok(stream) => stream,
        Err(_) => return Attempt::Unavailable,
    };
    if stream.set_nonblocking(true).is_err() {
        return Attempt::Interrupted("could not configure connected socket");
    }
    if let Some(auth) = auth {
        let selector = format!("{}\n", auth.principal.get());
        let mut selected = 0;
        while selected < selector.len() {
            if Instant::now() >= deadline {
                return Attempt::Interrupted("authentication deadline expired");
            }
            match stream.write(&selector.as_bytes()[selected..]) {
                Ok(0) => return Attempt::Interrupted("closed authentication selector"),
                Ok(n) => selected += n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::park_timeout(Duration::from_millis(1))
                }
                Err(_) => return Attempt::Interrupted("authentication selector failed"),
            }
        }
    }
    let timestamp = || MonoTime(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
    let mut stream = if let Some(auth) = auth {
        match Channel::client(stream, auth, id, timestamp()) {
            Ok(stream) => stream,
            Err(_) => return Attempt::Interrupted("authentication setup failed"),
        }
    } else {
        Channel::Plain(stream)
    };
    loop {
        if Instant::now() >= deadline {
            return Attempt::Interrupted("authentication deadline expired");
        }
        match stream.poll_client(timestamp()) {
            Ok(true) => break,
            Ok(false) => std::thread::park_timeout(Duration::from_millis(1)),
            Err(_) => return Attempt::Interrupted("authentication failed"),
        }
    }
    let mut sent = 0;
    while sent < text.len() {
        if Instant::now() >= deadline {
            return Attempt::Interrupted("request deadline expired");
        }
        if stream.poll_client(timestamp()).is_err() {
            return Attempt::Interrupted("authenticated write failed");
        }
        match stream.write(&text[sent..]) {
            Ok(0) => return Attempt::Interrupted("connection closed during request"),
            Ok(n) => sent += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::park_timeout(Duration::from_millis(1))
            }
            Err(_) => return Attempt::Interrupted("request write failed"),
        }
    }
    let mut bytes = [0u8; 4096];
    let mut used = 0;
    loop {
        if Instant::now() >= deadline {
            return Attempt::Interrupted("reply deadline expired");
        }
        if stream.poll_client(timestamp()).is_err() {
            return Attempt::Interrupted("authenticated read failed");
        }
        match stream.read(&mut bytes[used..]) {
            Ok(0) => return Attempt::Interrupted("connection closed without a complete reply"),
            Ok(n) => {
                used += n;
                if let Some(end) = bytes[..used].iter().position(|b| *b == b'\n') {
                    if end + 1 != used {
                        return Attempt::Interrupted("multiple reply lines");
                    }
                    return match std::str::from_utf8(&bytes[..used]) {
                        Ok(text) => Attempt::Reply(text.to_owned()),
                        Err(_) => Attempt::Interrupted("invalid reply encoding"),
                    };
                }
                if used == bytes.len() {
                    return Attempt::Interrupted("reply exceeds limit");
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::park_timeout(Duration::from_millis(1))
            }
            Err(_) => return Attempt::Interrupted("reply read failed"),
        }
    }
}
fn terminal(response: String) -> Result<(), Failure> {
    print!("{response}");
    if response.starts_with("OK ") {
        Ok(())
    } else {
        Err(
            "request unsuccessful; preserve the operation ID and payload when retrying a write"
                .into(),
        )
    }
}
fn interrupted(command: &[String], reason: &str) -> Result<(), Failure> {
    if command.first().is_some_and(|c| c == "add") {
        println!("UNKNOWN {reason}; retry the same operation ID and delta");
    } else if command.first().is_some_and(|c| c == "configure") {
        println!("UNKNOWN {reason}; retry the same configuration operation ID");
    } else {
        println!("ERR {reason}");
    }
    Err("request interrupted after connection; automatic routing stopped".into())
}
pub fn run(base: u16, id: Option<u64>, input: &[String]) -> Result<(), Failure> {
    let mut command = input.to_vec();
    let mut tls_directory = None;
    let mut principal = None;
    while command.len() >= 2 {
        let flag = command[command.len() - 2].as_str();
        if !matches!(flag, "--service-tls" | "--principal") {
            break;
        }
        let value = command.pop().unwrap();
        match command.pop().unwrap().as_str() {
            "--service-tls" if tls_directory.is_none() => {
                tls_directory = Some(std::path::PathBuf::from(value))
            }
            "--principal" if principal.is_none() => principal = Some(value.parse::<u64>()?),
            _ => return Err("duplicate service client option".into()),
        }
    }
    let targets = id.map_or_else(|| vec![1, 2, 3], |id| vec![id]);
    let auth = match (tls_directory.as_deref(), principal) {
        (None, None) => None,
        (Some(directory), Some(principal)) => {
            Some(ClientAccess::load(directory, principal, &targets)?)
        }
        _ => return Err("select --service-tls and --principal together".into()),
    };
    let command = command.as_slice();
    if id.is_none() {
        match command {
            [cmd] if cmd == "read" => (),
            [cmd, operation, delta]
                if cmd == "add"
                    && operation.parse::<u128>().is_ok_and(|n| n != 0)
                    && delta.parse::<i64>().is_ok() => {}
            _ => return Err("auto accepts only read or add NONZERO_OPERATION_ID DELTA".into()),
        }
    }
    let bytes = command.iter().try_fold(0usize, |bytes, part| {
        bytes.checked_add(part.len())?.checked_add(1)
    });
    if bytes.is_none_or(|n| n > 256) {
        return Err("command too long".into());
    }
    let text = format!("{}\n", command.join(" "));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(10);
    if let Some(id) = id {
        return match exchange(base, id, text.as_bytes(), deadline, auth.as_ref(), start) {
            Attempt::Unavailable => Err("node unavailable before connection".into()),
            Attempt::Interrupted(reason) => interrupted(command, reason),
            Attempt::Reply(response) => terminal(response),
        };
    }
    // Each attempt owns one socket and the same bounded original command. No leader cache.
    for _ in 0..100 {
        for id in 1..=3 {
            if Instant::now() >= deadline {
                return Err("no eligible local leader found within the routing deadline".into());
            }
            match exchange(base, id, text.as_bytes(), deadline, auth.as_ref(), start) {
                Attempt::Unavailable => (),
                Attempt::Reply(response) if response == NOT_LEADER => (),
                Attempt::Reply(response) => return terminal(response),
                Attempt::Interrupted(reason) => return interrupted(command, reason),
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        std::thread::park_timeout(remaining.min(Duration::from_millis(50)));
    }
    Err("no eligible local leader found within the routing attempt limit".into())
}
