// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use service_access::Channel;
use std::{
    io::{Read, Write},
    net::TcpStream,
};
pub(super) enum Phase {
    Dialing,
    Selector {
        stream: TcpStream,
        bytes: String,
        sent: usize,
    },
    Upgrade {
        channel: Channel,
        sent: usize,
        ack: [u8; 16],
        read: usize,
    },
}
pub(super) fn poll(
    attempt: &mut Attempt,
    dialer: &mut NativeTcpDialer,
    prepared: &Prepared,
    now: MonoTime,
    budget: SessionPollBudget,
    closed: bool,
) -> Result<Option<Box<dyn SecureSession>>, DiscoveryError> {
    let expired = closed || now >= attempt.deadline;
    if matches!(attempt.phase, Phase::Dialing) {
        let Some(done) = dialer.poll(1).pop() else {
            return Ok(None);
        };
        if done.connection != attempt.ticket {
            return Err(DiscoveryError::WrongBinding);
        }
        let stream = done.result.map_err(|_| DiscoveryError::Unavailable)?;
        if expired {
            return Err(DiscoveryError::Closed);
        }
        attempt.phase = Phase::Selector {
            stream,
            bytes: format!("{}\n", prepared.access.principal.get()),
            sent: 0,
        };
        return Ok(None);
    }
    if expired {
        return Err(DiscoveryError::Closed);
    }
    match &mut attempt.phase {
        Phase::Selector {
            stream,
            bytes,
            sent,
        } => {
            let count = (bytes.len() - *sent).min(budget.write_bytes);
            if count == 0 {
                return Ok(None);
            }
            match stream.write(&bytes.as_bytes()[*sent..*sent + count]) {
                Ok(0) => return Err(DiscoveryError::Unavailable),
                Ok(n) => *sent += n,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
                Err(_) => return Err(DiscoveryError::Unavailable),
            }
            if *sent == bytes.len() {
                let Phase::Selector { stream, .. } =
                    std::mem::replace(&mut attempt.phase, Phase::Dialing)
                else {
                    unreachable!()
                };
                let channel =
                    Channel::client(stream, &prepared.access, prepared.endpoint.node, now)
                        .map_err(|_| DiscoveryError::Unavailable)?;
                attempt.phase = Phase::Upgrade {
                    channel,
                    sent: 0,
                    ack: [0; 16],
                    read: 0,
                };
            }
            Ok(None)
        }
        Phase::Upgrade {
            channel,
            sent,
            ack,
            read,
        } => upgraded(channel, sent, ack, read, now, budget),
        Phase::Dialing => unreachable!(),
    }
}

fn upgraded(
    channel: &mut Channel,
    sent: &mut usize,
    ack: &mut [u8; 16],
    read: &mut usize,
    now: MonoTime,
    budget: SessionPollBudget,
) -> Result<Option<Box<dyn SecureSession>>, DiscoveryError> {
    if !channel
        .poll_client_with_budget(now, budget)
        .map_err(|_| DiscoveryError::Unavailable)?
    {
        return Ok(None);
    }
    let request = b"discover\n";
    if *sent < request.len() {
        let count = (request.len() - *sent).min(budget.write_bytes);
        if count == 0 {
            return Ok(None);
        }
        match channel.write(&request[*sent..*sent + count]) {
            Ok(0) => return Err(DiscoveryError::Unavailable),
            Ok(n) => *sent += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
            Err(_) => return Err(DiscoveryError::Unavailable),
        }
        return Ok(None);
    }
    let expected = super::super::command_discovery::ACK.as_bytes();
    let count = (expected.len() - *read).min(budget.read_bytes);
    if count == 0 {
        return Ok(None);
    }
    match channel.read(&mut ack[*read..*read + count]) {
        Ok(0) => return Err(DiscoveryError::Unavailable),
        Ok(n) => *read += n,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
        Err(_) => return Err(DiscoveryError::Unavailable),
    }
    if ack[..*read] != expected[..*read] {
        return Err(DiscoveryError::WrongBinding);
    }
    if *read == expected.len() {
        return channel
            .take_secure()
            .map(Some)
            .ok_or(DiscoveryError::WrongBinding);
    }
    Ok(None)
}
