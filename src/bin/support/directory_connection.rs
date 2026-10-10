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
//! One bounded command or manifest session, with exact request cancellation.
use super::{
    directory_owner::Owner,
    service_access::{ActiveAccess, Channel},
    setup::{checked, Failure},
};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};
use voteboat::{identity::*, runtime::*, secure::*};
pub const ACK: &str = "OK manifest-v1\n";
enum Phase {
    Input { bytes: [u8; 256], used: usize },
    Output { bytes: Vec<u8>, sent: usize },
    Pending(ClientTicket),
    Upgrade(usize),
    Remote,
}
pub struct Connection {
    channel: Channel,
    phase: Phase,
    deadline: Instant,
    pub generation: SecureSessionGeneration,
}
pub struct Commands<'a> {
    pub group: GroupIdentity,
    pub bootstrap: OperationId,
    pub records: &'a BTreeMap<OperationId, Vec<u8>>,
    pub quit: &'a mut bool,
}
impl Connection {
    pub fn new(stream: TcpStream, generation: SecureSessionGeneration) -> Result<Self, Failure> {
        stream.set_nonblocking(true)?;
        Ok(Self {
            channel: Channel::server(stream, true),
            phase: Phase::Input {
                bytes: [0; 256],
                used: 0,
            },
            deadline: Instant::now() + Duration::from_secs(10),
            generation,
        })
    }
    fn reply(&mut self, text: String) {
        self.phase = Phase::Output {
            bytes: format!("{text}\n").into_bytes(),
            sent: 0,
        };
    }
    pub fn poll(
        &mut self,
        owner: &mut Owner,
        access: &ActiveAccess,
        local: LocalIdentity,
        now: MonoTime,
        commands: &mut Commands<'_>,
    ) -> Result<bool, Failure> {
        if Instant::now() >= self.deadline {
            self.cancel(owner)?;
            return Ok(true);
        }
        if matches!(self.phase, Phase::Remote) {
            return Ok(!owner.remote());
        }
        match self.channel.poll(Some(access), local, self.generation, now) {
            Err(_) => {
                self.cancel(owner)?;
                return Ok(true);
            }
            Ok(false) => return Ok(false),
            Ok(true) => (),
        }
        match &mut self.phase {
            Phase::Input { bytes, used } => match self.channel.read(&mut bytes[*used..]) {
                Ok(0) => return Ok(true),
                Ok(n) => {
                    *used += n;
                    if bytes[..*used].contains(&b'\n') {
                        let input = std::str::from_utf8(&bytes[..*used])
                            .map_err(|_| "invalid UTF8".to_owned())
                            .map(str::to_owned);
                        let result = input.and_then(|s| {
                            self.execute(owner, access, now, commands, &s)
                                .map_err(|e| e.to_string())
                        });
                        if let Err(e) = result {
                            self.reply(format!("ERR {e}"));
                        }
                    } else if *used == bytes.len() {
                        self.reply("ERR command too long".into());
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                Err(_) => return Ok(true),
            },
            Phase::Output { bytes, sent } => {
                match self.channel.write(&bytes[*sent..]) {
                    Ok(0) if *sent != bytes.len() => return Ok(true),
                    Ok(n) => *sent += n,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                    Err(_) => return Ok(true),
                }
                if *sent == bytes.len() && self.channel.is_flushed() {
                    return Ok(true);
                }
            }
            Phase::Upgrade(sent) => {
                *sent += match self.channel.write(&ACK.as_bytes()[*sent..]) {
                    Ok(n) => n,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => 0,
                    Err(_) => return Ok(true),
                };
                if *sent == ACK.len() && self.channel.is_flushed() {
                    owner.upgrade(
                        self.channel
                            .take_secure()
                            .ok_or("missing authenticated session")?,
                        now,
                    )?;
                    self.phase = Phase::Remote;
                }
            }
            Phase::Pending(_) => {
                let mut byte = [0];
                match self.channel.read(&mut byte) {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                    _ => {
                        self.cancel(owner)?;
                        return Ok(true);
                    }
                }
            }
            Phase::Remote => unreachable!(),
        }
        Ok(false)
    }
    fn execute(
        &mut self,
        owner: &mut Owner,
        access: &ActiveAccess,
        now: MonoTime,
        c: &mut Commands<'_>,
        input: &str,
    ) -> Result<(), Failure> {
        if input.trim_end_matches('\n').contains('\n') {
            return Err("one command per connection".into());
        }
        self.channel.authorize(Some(access), c.group, input, now)?;
        let words = input.split_whitespace().collect::<Vec<_>>();
        match words.as_slice() {
            ["status"] => {
                let pending_reads = owner.node().local().reads.usage().requests;
                let core = owner
                    .node()
                    .local()
                    .owner
                    .core(c.group)
                    .ok_or("missing authority")?;
                self.reply(format!(
                    "OK role={:?} term={} committed={} checkpoint_index={} pending_reads={} evidence=local",
                    core.role(),
                    core.state().hard_state.term,
                    core.state().commit_index,core.state().base_index(),pending_reads
                ));
            }
            ["initialize"] => self.submit(owner, c, c.bootstrap)?,
            ["publish", operation] => {
                self.submit(owner, c, super::directory_plan::op(operation)?)?
            }
            ["manifest-session"] => self.phase = Phase::Upgrade(0),
            ["checkpoint"] => {
                checked(owner.node().control(c.group, NodeControl::Checkpoint))?;
                self.reply("OK checkpoint_admitted".into());
            }
            ["quit"] => {
                *c.quit = true;
                self.reply("OK shutting_down".into());
            }
            _ => return Err("unknown metadata command".into()),
        }
        Ok(())
    }
    fn submit(
        &mut self,
        owner: &mut Owner,
        c: &Commands<'_>,
        operation: OperationId,
    ) -> Result<(), Failure> {
        let bytes = c
            .records
            .get(&operation)
            .ok_or("operation not in trusted plan")?
            .clone();
        match owner.node().propose(ClientRequest {
            group: c.group,
            operation,
            bytes,
        }) {
            Ok(ticket) => {
                eprintln!("directory proposal admitted operation={}", operation.get());
                self.phase = Phase::Pending(ticket);
            }
            Err(rejected) => self.reply(format!("ERR {:?}", rejected.reason)),
        }
        Ok(())
    }
    pub fn cancel(&mut self, owner: &mut Owner) -> Result<(), Failure> {
        match self.phase {
            Phase::Pending(t) => {
                checked(owner.node().cancel_client(t))?;
            }
            Phase::Remote => owner.close_remote(),
            _ => (),
        }
        Ok(())
    }
    pub fn completions(owner: &mut Owner, connection: &mut Option<Self>) -> Result<(), Failure> {
        while let Some(output) = owner.node().poll_client() {
            let ticket = output.ticket();
            let outcome = checked(owner.node().complete_client(output).map_err(|r| r.reason))?;
            if let Some(c) = connection
                .as_mut()
                .filter(|c| matches!(c.phase,Phase::Pending(t) if t == ticket))
            {
                c.reply(match outcome {
                    ClientOutcome::Applied { receipt, .. } => format!(
                        "OK operation={} outcome={:?} duplicate={}",
                        receipt.operation.get(),
                        receipt.outcome,
                        receipt.duplicate
                    ),
                    other => format!("UNKNOWN {other:?}; retry original plan operation"),
                });
            }
        }
        Ok(())
    }
}
