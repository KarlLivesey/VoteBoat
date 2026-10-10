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
//! Bounded authenticated command exchange; callers decide retry semantics.
use super::{
    command_endpoints::Endpoint,
    service_access::{Channel, ClientAccess},
};
use std::{
    io::{Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};
use voteboat::runtime::MonoTime;
pub enum Attempt {
    Unavailable,
    Interrupted(&'static str),
    Reply(String),
}
pub fn connect(
    target: &Endpoint,
    deadline: Instant,
    auth: Option<&ClientAccess>,
    start: Instant,
) -> Result<Channel, Attempt> {
    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
        return Err(Attempt::Unavailable);
    };
    let mut stream =
        match TcpStream::connect_timeout(&target.address, remaining.min(Duration::from_secs(2))) {
            Ok(stream) => stream,
            Err(_) => return Err(Attempt::Unavailable),
        };
    if stream.set_nonblocking(true).is_err() {
        return Err(Attempt::Interrupted("could not configure connected socket"));
    }
    if let Some(auth) = auth {
        let selector = format!("{}\n", auth.principal.get());
        let mut selected = 0;
        while selected < selector.len() {
            if Instant::now() >= deadline {
                return Err(Attempt::Interrupted("authentication deadline expired"));
            }
            match stream.write(&selector.as_bytes()[selected..]) {
                Ok(0) => return Err(Attempt::Interrupted("closed authentication selector")),
                Ok(n) => selected += n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::park_timeout(Duration::from_millis(1))
                }
                Err(_) => return Err(Attempt::Interrupted("authentication selector failed")),
            }
        }
    }
    let timestamp = || MonoTime(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
    let mut stream = if let Some(auth) = auth {
        match Channel::client(stream, auth, target.node, timestamp()) {
            Ok(stream) => stream,
            Err(_) => return Err(Attempt::Interrupted("authentication setup failed")),
        }
    } else {
        Channel::Plain(stream)
    };
    loop {
        if Instant::now() >= deadline {
            return Err(Attempt::Interrupted("authentication deadline expired"));
        }
        match stream.poll_client(timestamp()) {
            Ok(true) => break,
            Ok(false) => std::thread::park_timeout(Duration::from_millis(1)),
            Err(_) => return Err(Attempt::Interrupted("authentication failed")),
        }
    }
    Ok(stream)
}
pub fn request(stream: &mut Channel, text: &[u8], deadline: Instant, start: Instant) -> Attempt {
    request_bounded(stream, text, deadline, start, 4096)
}
pub fn request_bounded(
    stream: &mut Channel,
    text: &[u8],
    deadline: Instant,
    start: Instant,
    max_reply: usize,
) -> Attempt {
    if max_reply == 0 || max_reply > 1024 * 1024 {
        return Attempt::Unavailable;
    }
    let timestamp = || MonoTime(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
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
    let mut bytes = vec![0u8; max_reply];
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
