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
//! Three independent native TCP/TLS nodes, with a bounded local control endpoint.
#[path = "support/counter_setup.rs"]
mod setup;
use setup::{checked, group, Failure, Service};
use std::{
    io::{Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    path::Path,
    time::{Duration, Instant},
};
use voteboat::{identity::*, runtime::*};

const HELP: &str =
    "voteboat-counter serve create|recover DIRECTORY NODE BASE_PORT TLS_DIRECTORY [PEERS_FILE]\n\
voteboat-counter client BASE_PORT NODE status|read|add OPERATION_ID DELTA|checkpoint|quit\n\
Default peer ports are BASE+1..3; local command ports are BASE+101..103.\n\
TLS_DIRECTORY contains ca.der, node1..3.der and node1..3-key.der.\n\
Commands are local-only trusted-user controls. Peer traffic uses mutual TLS.\n\
PEERS_FILE lines: NODE SOCKET_ADDRESS TLS_SERVER_NAME.\n\
Use the same operation ID and delta when retrying an unknown write.";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Pending {
    Write(ClientTicket),
    Read(ReadInvocationTicket),
}
enum Phase {
    Input { bytes: [u8; 256], len: usize },
    Pending(Pending),
    Output { bytes: Vec<u8>, sent: usize },
}
struct Connection {
    stream: TcpStream,
    phase: Phase,
    deadline: Instant,
}
impl Connection {
    fn reply(&mut self, text: String) {
        self.phase = Phase::Output {
            bytes: format!("{text}\n").into_bytes(),
            sent: 0,
        };
    }
}
fn now(start: Instant) -> MonoTime {
    MonoTime(start.elapsed().as_millis().min(u64::MAX as u128) as u64)
}
fn ports(base: &str, id: &str) -> Result<(u16, u64), Failure> {
    let base: u16 = base.parse()?;
    let id: u64 = id.parse()?;
    if !(1..=3).contains(&id) || base == 0 || base > 65432 {
        return Err("invalid base port or node ID".into());
    }
    Ok((base, id))
}
fn command(service: &mut Service, command: &str, quit: &mut bool) -> Result<Phase, String> {
    let words = command.split_whitespace().collect::<Vec<_>>();
    let reply = match words.as_slice() {
        ["status"] => {
            let core = service.local().owner.core(group()).ok_or("missing group")?;
            format!(
                "OK role={:?} term={} committed={}",
                core.role(),
                core.state().hard_state.term,
                core.state().commit_index
            )
        }
        ["add", operation, delta] => {
            let operation = operation
                .parse::<u128>()
                .ok()
                .and_then(OperationId::new)
                .ok_or("invalid operation ID")?;
            let delta = delta.parse::<i64>().map_err(|_| "invalid delta")?;
            let ticket = service
                .propose(ClientRequest {
                    group: group(),
                    operation,
                    bytes: delta.to_le_bytes().to_vec(),
                })
                .map_err(|r| format!("{:?}", r.reason))?;
            return Ok(Phase::Pending(Pending::Write(ticket)));
        }
        ["read"] => {
            let ticket = service
                .read(group(), ())
                .map_err(|r| format!("{:?}", r.reason))?;
            return Ok(Phase::Pending(Pending::Read(ticket)));
        }
        ["checkpoint"] => {
            service
                .control(group(), NodeControl::Checkpoint)
                .map_err(|e| format!("{e:?}"))?;
            "OK checkpoint_admitted".into()
        }
        ["quit"] => {
            *quit = true;
            "OK shutting_down".into()
        }
        _ => {
            return Err("expected status, read, add OPERATION_ID DELTA, checkpoint or quit".into())
        }
    };
    Ok(Phase::Output {
        bytes: format!("{reply}\n").into_bytes(),
        sent: 0,
    })
}
fn outputs(service: &mut Service, connection: &mut Option<Connection>) -> Result<(), Failure> {
    while let Some(output) = service.poll_client() {
        let ticket = Pending::Write(output.ticket());
        let result = checked(service.complete_client(output).map_err(|r| r.reason))?;
        if let Some(c) = connection
            .as_mut()
            .filter(|c| matches!(c.phase, Phase::Pending(p) if p == ticket))
        {
            c.reply(match result {
                ClientOutcome::Applied { receipt, .. } => format!(
                    "OK outcome={:?} duplicate={}",
                    receipt.outcome, receipt.duplicate
                ),
                ClientOutcome::NotProposed(e) => format!("ERR not_proposed={e:?}"),
                ClientOutcome::Unknown(e) => {
                    format!("UNKNOWN {e:?}; retry the same operation ID and delta")
                }
            });
        }
    }
    while let Some(output) = service.poll_read() {
        let ticket = Pending::Read(output.ticket());
        let result = checked(service.complete_read(output).map_err(|r| r.reason))?;
        if let Some(c) = connection
            .as_mut()
            .filter(|c| matches!(c.phase, Phase::Pending(p) if p == ticket))
        {
            c.reply(match result {
                ReadOutcome::Read {
                    result: Ok(value), ..
                } => format!("OK value={value}"),
                other => format!("ERR {other:?}"),
            });
        }
    }
    Ok(())
}
fn serve(
    mode: &str,
    root: &Path,
    id: u64,
    base: u16,
    tls: &Path,
    endpoints: Option<&Path>,
) -> Result<(), Failure> {
    let create = match mode {
        "create" => true,
        "recover" => false,
        _ => return Err("expected create or recover".into()),
    };
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, base + 100 + id as u16))?;
    listener.set_nonblocking(true)?;
    let config = setup::configuration(root, id, base, tls, create, endpoints)?;
    let peer_address = config.listen;
    let mut service = setup::open(config)?;
    let start = Instant::now();
    println!(
        "ready node={id} peer={peer_address} command=127.0.0.1:{}",
        base + 100 + id as u16
    );
    let mut connection: Option<Connection> = None;
    let mut quit = false;
    let mut shutdown_started = None;
    loop {
        let progress = checked(service.poll(now(start), NodePollBudget::default()))?;
        if let Some(replica) = progress.replica {
            for step in replica.steps {
                // Client/read errors are reported through their exact output tickets.
                if let Some(error) = step.error {
                    eprintln!("event: {error:?}");
                }
            }
        }
        outputs(&mut service, &mut connection)?;
        if !quit && connection.is_none() {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true)?;
                    connection = Some(Connection {
                        stream,
                        phase: Phase::Input {
                            bytes: [0; 256],
                            len: 0,
                        },
                        deadline: Instant::now() + Duration::from_secs(5),
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                Err(e) => return Err(e.into()),
            }
        }
        let mut remove = false;
        if let Some(c) = &mut connection {
            if Instant::now() >= c.deadline {
                if let Phase::Pending(p) = c.phase {
                    match p {
                        Pending::Write(t) => {
                            checked(service.cancel_client(t))?;
                        }
                        Pending::Read(t) => {
                            checked(service.cancel_read(t))?;
                        }
                    }
                }
                remove = true;
            } else {
                match &mut c.phase {
                    Phase::Input { bytes, len } => match c.stream.read(&mut bytes[*len..]) {
                        Ok(0) => remove = true,
                        Ok(n) => {
                            *len += n;
                            if bytes[..*len].contains(&b'\n') {
                                let input = std::str::from_utf8(&bytes[..*len])
                                    .map_err(|_| "invalid UTF-8".to_string());
                                let result = input.and_then(|s| {
                                    if s.trim_end_matches('\n').contains('\n') {
                                        return Err("one command per connection".into());
                                    }
                                    command(&mut service, s, &mut quit)
                                });
                                match result {
                                    Ok(phase) => c.phase = phase,
                                    Err(e) => c.reply(format!("ERR {e}")),
                                }
                            } else if *len == bytes.len() {
                                c.reply("ERR command too long".into());
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                        Err(_) => remove = true,
                    },
                    Phase::Output { bytes, sent } => match c.stream.write(&bytes[*sent..]) {
                        Ok(0) => remove = true,
                        Ok(n) => {
                            *sent += n;
                            remove = *sent == bytes.len();
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                        Err(_) => remove = true,
                    },
                    Phase::Pending(_) => (),
                }
            }
        }
        if remove {
            connection = None;
        }
        if quit && connection.is_none() && shutdown_started.is_none() {
            service.begin_shutdown();
            shutdown_started = Some(Instant::now());
        }
        if service.is_drained() {
            break;
        }
        if shutdown_started.is_some_and(|t| t.elapsed() > Duration::from_secs(10)) {
            return Err("shutdown timed out; recover durable state on restart".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    setup::join(service)?;
    println!("stopped node={id} workers_joined=true");
    Ok(())
}
fn client(base: u16, id: u64, command: &[String]) -> Result<(), Failure> {
    let text = format!("{}\n", command.join(" "));
    if text.len() > 256 {
        return Err("command too long".into());
    }
    let mut stream = TcpStream::connect_timeout(
        &(Ipv4Addr::LOCALHOST, base + 100 + id as u16).into(),
        Duration::from_secs(2),
    )?;
    stream.set_read_timeout(Some(Duration::from_secs(7)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(text.as_bytes())?;
    let mut response = String::new();
    stream.take(4096).read_to_string(&mut response)?;
    print!("{response}");
    if !response.starts_with("OK ") {
        return Err("request unsuccessful; an interrupted write may still commit; retry its original operation ID and delta".into());
    }
    Ok(())
}
fn main() -> Result<(), Failure> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [help] if help == "--help" || help == "-h" => {
            println!("{HELP}");
            Ok(())
        }
        [serve_arg, mode, root, id, base, tls] if serve_arg == "serve" => {
            let (base, id) = ports(base, id)?;
            serve(mode, Path::new(root), id, base, Path::new(tls), None)
        }
        [serve_arg, mode, root, id, base, tls, endpoints] if serve_arg == "serve" => {
            let (base, id) = ports(base, id)?;
            serve(
                mode,
                Path::new(root),
                id,
                base,
                Path::new(tls),
                Some(Path::new(endpoints)),
            )
        }
        [client_arg, base, id, rest @ ..] if client_arg == "client" && !rest.is_empty() => {
            let (base, id) = ports(base, id)?;
            client(base, id, rest)
        }
        _ => Err(HELP.into()),
    }
}
