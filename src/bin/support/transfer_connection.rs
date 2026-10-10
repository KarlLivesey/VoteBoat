// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    app::App,
    profile::{Binding, Profile},
    server::Node,
    service_access::{ActiveAccess, Channel},
    setup::{checked, Failure},
    wire,
};
use std::{
    fmt::Debug,
    io::{Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};
use voteboat::{identity::*, runtime::*, secure::*};
enum Phase {
    Input(Vec<u8>),
    Output(Vec<u8>, usize),
    Client(ClientTicket),
    Read(ReadInvocationTicket, Vec<String>),
}
pub struct Connection {
    channel: Channel,
    phase: Phase,
    generation: SecureSessionGeneration,
    deadline: Instant,
}
pub struct Context<'a> {
    pub profile: &'a Profile,
    pub binding: Binding,
    pub access: &'a ActiveAccess,
    pub local: LocalIdentity,
    pub now: MonoTime,
}
impl Connection {
    pub fn new(stream: TcpStream, generation: SecureSessionGeneration) -> Result<Self, Failure> {
        stream.set_nonblocking(true)?;
        Ok(Self {
            channel: Channel::server(stream, true),
            phase: Phase::Input(Vec::with_capacity(wire::COMMAND_BYTES)),
            generation,
            deadline: Instant::now() + Duration::from_secs(15),
        })
    }
    fn reply(&mut self, text: String) {
        self.phase = Phase::Output(format!("{text}\n").into_bytes(), 0);
    }
    pub fn poll<A: App>(
        &mut self,
        node: &mut Node<A>,
        context: &Context<'_>,
        quit: &mut bool,
        credentials: &mut super::credential_reload::Commands<'_>,
        peers: &mut Option<super::peer_credentials::Peers>,
    ) -> Result<bool, Failure>
    where
        A::Receipt: Debug,
    {
        if Instant::now() >= self.deadline {
            return Ok(true);
        }
        if !self.channel.poll(
            Some(context.access),
            context.local,
            self.generation,
            context.now,
        )? {
            return Ok(false);
        }
        match &mut self.phase {
            Phase::Input(input) => {
                let mut bytes = [0; 4096];
                match self.channel.read(&mut bytes) {
                    Ok(0) => return Ok(true),
                    Ok(n) => {
                        if input.len() + n > wire::COMMAND_BYTES {
                            return Err("command budget exceeded".into());
                        }
                        input.extend_from_slice(&bytes[..n]);
                        if input.contains(&b'\n') {
                            let text = String::from_utf8(input.clone())?;
                            if let Err(e) =
                                self.execute(node, context, quit, credentials, peers, &text)
                            {
                                self.reply(format!("ERR {e}"));
                            }
                        }
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                    Err(e) => return Err(e.into()),
                }
            }
            Phase::Output(bytes, sent) => {
                match self.channel.write(&bytes[*sent..]) {
                    Ok(0) if *sent < bytes.len() => return Ok(true),
                    Ok(n) => *sent += n,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                    Err(e) => return Err(e.into()),
                }
                if *sent == bytes.len() && self.channel.is_flushed() {
                    return Ok(true);
                }
            }
            Phase::Client(_) | Phase::Read(..) => {
                let mut byte = [0];
                match self.channel.read(&mut byte) {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                    _ => return Ok(true),
                }
            }
        }
        Ok(false)
    }
    fn execute<A: App>(
        &mut self,
        node: &mut Node<A>,
        context: &Context<'_>,
        quit: &mut bool,
        credentials: &mut super::credential_reload::Commands<'_>,
        peers: &mut Option<super::peer_credentials::Peers>,
        text: &str,
    ) -> Result<(), Failure> {
        if !text.ends_with('\n') || text.trim_end_matches('\n').contains('\n') {
            return Err("one command per connection".into());
        }
        let (p, b) = (context.profile, context.binding);
        self.channel
            .authorize(Some(context.access), b.group, text, context.now)?;
        if let Some(result) = super::peer_credentials::command(peers, text) {
            self.reply(result?);
            return Ok(());
        }
        let words = text.split_whitespace().collect::<Vec<_>>();
        match words.as_slice() {
            ["credential-status" | "reload-access", ..] => {
                self.reply(credentials.command(&words)?);
            }
            ["status"] => {
                let core = node.local().owner.core(b.group).ok_or("missing group")?;
                self.reply(format!(
                    "OK role={:?} committed={} checkpoint_index={} pending_reads={} evidence=local",
                    core.role(),
                    core.state().commit_index,
                    core.state().base_index(),
                    node.local().reads.usage().requests
                ));
            }
            ["quit"] => {
                *quit = true;
                self.reply("OK shutting_down".into());
            }
            ["checkpoint"] => {
                checked(node.control(b.group, NodeControl::Checkpoint))?;
                self.reply("OK checkpoint_admitted".into());
            }
            ["read", _] | ["transfer-read", _] | ["transfer-export", _] | ["retirement-status"] => {
                let query = node.local().applications[&b.group].query(p, b, &words)?;
                match node.read(b.group, query) {
                    Ok(t) => {
                        eprintln!("transfer_read accepted sequence={}", t.sequence);
                        self.phase = Phase::Read(t, words.iter().map(|s| (*s).to_owned()).collect())
                    }
                    Err(e) => self.reply(format!("ERR {:?}", e.reason)),
                }
            }
            _ => {
                let (operation, bytes) =
                    node.local().applications[&b.group].command(p, b, &words)?;
                match node.propose(ClientRequest {
                    group: b.group,
                    operation,
                    bytes,
                }) {
                    Ok(t) => {
                        eprintln!("transfer_proposal accepted sequence={}", t.sequence);
                        self.phase = Phase::Client(t);
                    }
                    Err(e) => self.reply(format!("ERR {:?}", e.reason)),
                }
            }
        }
        Ok(())
    }
    pub fn cancel<A: App>(&mut self, node: &mut Node<A>) -> Result<(), Failure> {
        match self.phase {
            Phase::Client(t) => checked(node.cancel_client(t))?,
            Phase::Read(t, _) => checked(node.cancel_read(t))?,
            _ => (),
        }
        if let Some(mut session) = self.channel.take_secure() {
            session.close();
        }
        Ok(())
    }
    pub fn completions<A: App>(
        node: &mut Node<A>,
        p: &Profile,
        b: Binding,
        mut connection: Option<&mut Self>,
    ) -> Result<(), Failure>
    where
        A::Receipt: Debug,
    {
        while let Some(output) = node.poll_client() {
            let ticket = output.ticket();
            let result = checked(node.complete_client(output).map_err(|r| r.reason))?;
            if let Some(c) = connection
                .as_deref_mut()
                .filter(|c| matches!(c.phase,Phase::Client(t) if t==ticket))
            {
                c.reply(match result {
                    ClientOutcome::Applied { receipt, .. } => format!("OK receipt={receipt:?}"),
                    other => format!("UNKNOWN {other:?}"),
                });
            }
        }
        while let Some(output) = node.poll_read() {
            let ticket = output.ticket();
            let result = checked(node.complete_read(output).map_err(|r| r.reason))?;
            if let Some(c) = connection.as_deref_mut() {
                if let Phase::Read(t, words) = &c.phase {
                    if *t == ticket {
                        let reply = match &result {
                            ReadOutcome::Read { result: Ok(_), .. } => node.local().applications
                                [&b.group]
                                .answer(p, b, words, &result)
                                .unwrap_or_else(|e| format!("ERR {e}")),
                            _ => "UNKNOWN read unavailable".into(),
                        };
                        c.reply(reply);
                    }
                }
            }
        }
        Ok(())
    }
}
