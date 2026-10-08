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
//! Native TCP/TLS or optional QUIC counter service, with bounded local controls.
#[path = "support/counter_admin.rs"]
mod administration;
#[path = "support/local_client.rs"]
mod local_client;
#[path = "support/counter_setup.rs"]
mod setup;
use setup::{checked, group, Failure, Service};
use std::{
    io::{Read, Write},
    net::{Ipv4Addr, TcpListener, TcpStream},
    path::Path,
    time::{Duration, Instant},
};
use voteboat::membership::ConfigurationResumeAction;
use voteboat::{identity::*, native::connect::NativePeerProtocol, raft::RaftError, runtime::*};

const HELP: &str =
    "voteboat-counter serve create|recover|recover-member DIRECTORY NODE BASE_PORT TLS_DIRECTORY [PEERS_FILE | --deployment FILE] [--transport tcp|quic] [--admin-plan FILE]\n\
voteboat-counter enroll create|recover DIRECTORY NODE BASE_PORT TLS_DIRECTORY SOURCE_DIRECTORY SOURCE_NODE [PEERS_FILE | --deployment FILE]\n\
voteboat-counter client BASE_PORT NODE status|configuration-status OPERATION_ID|read|add OPERATION_ID DELTA|checkpoint|quit\n\
voteboat-counter client BASE_PORT auto read|add OPERATION_ID DELTA\n\
Default peer ports are BASE+1..3; local command ports are BASE+101..103.\n\
TLS_DIRECTORY contains ca.der, node1..3.der and node1..3-key.der.\n\
Commands are local-only trusted-user controls. Peer traffic uses mutual TLS.\n\
recover-member explicitly verifies existing membership journals and selects wire format 6 on all peers.\n\
enroll is an offline trusted handoff; stop source and destination before use and preserve files on failure.\n\
QUIC requires a build with --features quic; TCP is the default.\n\
PEERS_FILE lines: NODE SOCKET_ADDRESS TLS_SERVER_NAME.\n\
recover-member accepts --deployment FILE instead of PEERS_FILE; trailing named options may appear in any order.\n\
--admin-plan FILE is trusted startup input for recover-member; see docs/COUNTER_SERVICE.md for grammar and restart rules.\n\
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
    if !(1..=setup::MAX_NODE).contains(&id)
        || base == 0
        || u64::from(base) + 100 + id > u64::from(u16::MAX)
    {
        return Err("invalid base port or node ID".into());
    }
    Ok((base, id))
}
fn command(service: &mut Service, command: &str, quit: &mut bool) -> Result<Phase, String> {
    let words = command.split_whitespace().collect::<Vec<_>>();
    let reply = match words.as_slice() {
        ["configuration-status", operation] => {
            let operation = operation.parse::<u128>().ok().and_then(OperationId::new)
                .ok_or("invalid operation ID")?;
            let status = service.configuration_status(group(), operation)
                .map_err(|e| format!("{e:?}"))?;
            let action = match status.resume_action() {
                ConfigurationResumeAction::Completed => "completed",
                ConfigurationResumeAction::WaitForCommit => "wait_for_commit",
                ConfigurationResumeAction::Finalize(_) => "finalize_requires_authorization",
                ConfigurationResumeAction::NotFoundLocally => "inconclusive_local_absence",
            };
            format!("OK evidence=local_durable operation={} committed_prefix={} durable_last={} committed={:?} accepted={:?} action={}",
                operation.get(), status.committed_index, status.durable_last_index,
                status.committed, status.accepted, action)
        }
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
                .map_err(|r| match r.reason {
                    ClientError::Consensus(RaftError::NotLeader) => "NOT_LEADER".into(),
                    other => format!("{other:?}"),
                })?;
            return Ok(Phase::Pending(Pending::Write(ticket)));
        }
        ["read"] => {
            let ticket = service.read(group(), ()).map_err(|r| match r.reason {
                ReadInvocationError::Consensus(RaftError::NotLeader) => "NOT_LEADER".into(),
                other => format!("{other:?}"),
            })?;
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
            return Err("expected status, configuration-status OPERATION_ID, read, add OPERATION_ID DELTA, checkpoint or quit".into())
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
                ClientOutcome::NotProposed(RaftError::NotLeader) => "ERR NOT_LEADER".into(),
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
                ReadOutcome::NotRead(RaftError::NotLeader) => "ERR NOT_LEADER".into(),
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
    input: setup::PeerInput<'_>,
    options: (NativePeerProtocol, Option<&Path>),
) -> Result<(), Failure> {
    let (protocol, plan_path) = options;
    let create = match mode {
        "create" => true,
        "recover" | "recover-member" => false,
        _ => return Err("expected create, recover or recover-member".into()),
    };
    if matches!(input, setup::PeerInput::Deployment(_)) && mode != "recover-member" {
        return Err(
            "explicit deployment requires recover-member; enrollment uses enroll create|recover"
                .into(),
        );
    }
    if plan_path.is_some() && mode != "recover-member" {
        return Err("administration plan requires recover-member".into());
    }
    let config = setup::configuration(root, id, base, tls, create, input)?;
    let mut administration = plan_path
        .map(|path| administration::Administration::load(path, &config.provisioned_stores))
        .transpose()?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, base + 100 + id as u16))?;
    listener.set_nonblocking(true)?;
    let peer_address = config.startup.listen;
    let mut service = setup::open(config, protocol, mode == "recover-member")?;
    let start = Instant::now();
    println!(
        "ready node={id} peer={peer_address} transport={protocol:?} command=127.0.0.1:{}",
        base + 100 + id as u16
    );
    let mut connection: Option<Connection> = None;
    let mut quit = false;
    let mut shutdown_started = None;
    loop {
        let progress = if let Some(admin) = administration.as_ref() {
            checked(service.poll_with_configuration_authorization(
                now(start),
                NodePollBudget::default(),
                |core, proposal| {
                    admin
                        .plan
                        .authorize(core.state().bootstrap.group, core.membership(), proposal)
                },
            ))?
        } else {
            checked(service.poll(now(start), NodePollBudget::default()))?
        };
        if let Some(replica) = progress.replica {
            for step in replica.steps {
                // Client/read errors are reported through their exact output tickets.
                if let Some(error) = step.error {
                    eprintln!("event: {error:?}");
                }
            }
        }
        outputs(&mut service, &mut connection)?;
        if let Some(admin) = administration.as_mut() {
            admin.tick(&mut service, quit)?;
        }
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
fn main() -> Result<(), Failure> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let mut protocol = NativePeerProtocol::TcpTls;
    let mut transport_selected = false;
    let mut deployment = None;
    let mut admin_plan = None;
    while args.first().is_some_and(|a| a == "serve" || a == "enroll") && args.len() >= 3 {
        let flag = args[args.len() - 2].as_str();
        if !matches!(flag, "--transport" | "--deployment" | "--admin-plan") {
            break;
        }
        let value = args.pop().unwrap();
        match args.pop().unwrap().as_str() {
            "--transport" if !transport_selected && args[0] == "serve" => {
                transport_selected = true;
                protocol = match value.as_str() {
                    "tcp" => NativePeerProtocol::TcpTls,
                    #[cfg(feature = "quic")]
                    "quic" => NativePeerProtocol::Quic,
                    #[cfg(not(feature = "quic"))]
                    "quic" => {
                        return Err("QUIC support requires building with --features quic".into())
                    }
                    _ => return Err("expected --transport tcp or --transport quic".into()),
                };
            }
            "--deployment" if deployment.is_none() => {
                deployment = Some(std::path::PathBuf::from(value))
            }
            "--admin-plan" if admin_plan.is_none() && args[0] == "serve" => {
                admin_plan = Some(std::path::PathBuf::from(value))
            }
            _ => return Err("duplicate or unsupported startup option".into()),
        }
    }
    match args.as_slice() {
        [enroll_arg, mode, root, id, base, tls, source, source_id, rest @ ..]
            if enroll_arg == "enroll" && rest.len() <= 1 =>
        {
            let (base, id) = ports(base, id)?;
            let create = match mode.as_str() {
                "create" => true,
                "recover" => false,
                _ => return Err("expected enrollment create or recover".into()),
            };
            let input = match (deployment.as_deref(), rest.first()) {
                (Some(path), None) => setup::PeerInput::Deployment(path),
                (None, legacy) => setup::PeerInput::Legacy(legacy.map(Path::new)),
                _ => return Err("select either PEERS_FILE or --deployment".into()),
            };
            let config =
                setup::configuration(Path::new(root), id, base, Path::new(tls), create, input)?;
            let (index, term) = setup::enroll(config, Path::new(source), source_id.parse()?)?;
            println!("OK enrolled node={id} checkpoint={index} term={term} evidence=trusted_local_source");
            Ok(())
        }
        [help] if help == "--help" || help == "-h" => {
            println!("{HELP}");
            Ok(())
        }
        [serve_arg, mode, root, id, base, tls, rest @ ..]
            if serve_arg == "serve" && rest.len() <= 1 =>
        {
            let (base, id) = ports(base, id)?;
            let input = match (deployment.as_deref(), rest.first()) {
                (Some(path), None) => setup::PeerInput::Deployment(path),
                (None, legacy) => setup::PeerInput::Legacy(legacy.map(Path::new)),
                _ => return Err("select either PEERS_FILE or --deployment".into()),
            };
            serve(
                mode,
                Path::new(root),
                id,
                base,
                Path::new(tls),
                input,
                (protocol, admin_plan.as_deref()),
            )
        }
        [client_arg, base, id, rest @ ..] if client_arg == "client" && !rest.is_empty() => {
            if id == "auto" {
                let (base, _) = ports(base, "1")?;
                local_client::run(base, None, rest)
            } else {
                let (base, id) = ports(base, id)?;
                local_client::run(base, Some(id), rest)
            }
        }
        _ => Err(HELP.into()),
    }
}
