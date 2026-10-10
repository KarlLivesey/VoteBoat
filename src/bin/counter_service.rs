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
#[path = "support/service_access.rs"]
mod service_access;
#[path = "support/counter_setup.rs"]
mod setup;
use setup::{checked, group, Failure, Service};
use std::{
    io::{Read, Write},
    net::{Ipv4Addr, TcpListener},
    path::Path,
    time::{Duration, Instant},
};
use voteboat::membership::ConfigurationResumeAction;
use voteboat::{identity::*, native::connect::NativePeerProtocol, raft::RaftError, runtime::*};
use voteboat::{native::observability::NativeCounterObserver, observability::*};

const HELP: &str =
    "voteboat-counter serve create|recover|recover-member DIRECTORY NODE BASE_PORT TLS_DIRECTORY [PEERS_FILE | --deployment FILE] [--transport tcp|quic] [--admin-plan FILE | --remote-admin-plan FILE | --remote-admin-policy FILE] [--service-access FILE]\n\
voteboat-counter enroll create|recover DIRECTORY NODE BASE_PORT TLS_DIRECTORY SOURCE_DIRECTORY SOURCE_NODE [PEERS_FILE | --deployment FILE]\n\
voteboat-counter client BASE_PORT NODE status|metrics|configuration-status OPERATION_ID|configure OPERATION_ID|read|add OPERATION_ID DELTA|checkpoint|quit\n\
voteboat-counter client BASE_PORT auto read|add OPERATION_ID DELTA\n\
Default peer ports are BASE+1..3; local command ports are BASE+101..103.\n\
TLS_DIRECTORY contains ca.der, node1..3.der and node1..3-key.der.\n\
Commands are local-only trusted-user controls. Peer traffic uses mutual TLS.\n\
recover-member explicitly verifies existing membership journals and selects wire format 7 on all peers.\n\
enroll is an offline trusted handoff; stop source and destination before use and preserve files on failure.\n\
QUIC requires a build with --features quic; TCP is the default.\n\
PEERS_FILE lines: NODE SOCKET_ADDRESS TLS_SERVER_NAME.\n\
recover-member accepts --deployment FILE instead of PEERS_FILE; trailing named options may appear in any order.\n\
--admin-plan FILE is trusted startup input for recover-member; see docs/COUNTER_SERVICE.md for grammar and restart rules.\n\
Optional authenticated commands: serve --service-access FILE; client ... --service-tls TLS_DIRECTORY --principal ID.\n\
Use the same operation ID and delta when retrying an unknown write.";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Pending {
    Write(ClientTicket),
    Read(ReadInvocationTicket),
    Configure(OperationId),
}
enum Phase {
    Input { bytes: [u8; 256], len: usize },
    Pending(Pending),
    Output { bytes: Vec<u8>, sent: usize },
}
struct Connection {
    stream: service_access::Channel,
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
fn command(
    service: &mut Service,
    observer: &impl Observer,
    command: &str,
    administration: &mut Option<administration::Administration>,
    quit: &mut bool,
) -> Result<Phase, String> {
    let words = command.split_whitespace().collect::<Vec<_>>();
    let reply = match words.as_slice() {
        ["metrics"] => {
            let snapshot = observer.snapshot_counters();
            let c = snapshot.counters;
            format!("OK evidence=local_volatile store_session={} polls={} failed_polls={} owner_steps={} step_errors={} worker_events={} snapshot_events={} snapshot_installs={} persistence_batches={} applications={} peer_sends={} peer_received={} ingress_blocked={} connection_failures={}", snapshot.owner.store.session.get(), c.polls, c.failed_polls, c.owner_steps, c.step_errors, c.worker_events, c.snapshot_events, c.snapshot_installs, c.persistence_batches, c.applications, c.peer_sends, c.peer_received, c.ingress_blocked, c.connection_failures)
        }
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
        ["configure-record", record @ ..] => {
            if service.local().owner.core(group()).is_none_or(|core| core.role() != voteboat::raft::Role::Leader) {
                return Err("NOT_LEADER".into());
            }
            let submission = administration.as_mut().ok_or("client-supplied targets disabled")?
                .request_record(&record.join(" "), service)?;
            match submission {
                administration::RecordSubmission::Pending(operation) => return Ok(Phase::Pending(Pending::Configure(operation))),
                administration::RecordSubmission::Reply(reply) => reply,
            }
        }
        ["configure", operation] => {
            let operation = operation.parse::<u128>().ok().and_then(OperationId::new)
                .ok_or("invalid operation ID")?;
            if service.local().owner.core(group()).is_none_or(|core| core.role() != voteboat::raft::Role::Leader) {
                return Err("NOT_LEADER".into());
            }
            administration.as_mut().ok_or("remote administration disabled")?
                .request(operation)?;
            return Ok(Phase::Pending(Pending::Configure(operation)));
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
            return Err("expected status, metrics, configuration-status OPERATION_ID, read, add OPERATION_ID DELTA, checkpoint or quit".into())
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
    options: (
        NativePeerProtocol,
        Option<&Path>,
        Option<&Path>,
        administration::Mode,
    ),
) -> Result<(), Failure> {
    let (protocol, plan_path, access_path, admin_mode) = options;
    let remote = admin_mode != administration::Mode::Automatic;
    let create = checked_service_mode(
        mode,
        &input,
        remote,
        access_path.is_some(),
        plan_path.is_some(),
    )?;
    let config = setup::configuration(root, id, base, tls, create, input)?;
    let access = access_path
        .map(|path| service_access::Access::load(path, tls, config.startup.tls.clone()))
        .transpose()?;
    let mut administration = plan_path
        .map(|path| {
            administration::Administration::load(path, &config.provisioned_stores, admin_mode)
        })
        .transpose()?;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, base + 100 + id as u16))?;
    listener.set_nonblocking(true)?;
    let peer_address = config.startup.listen;
    let mut service = setup::open(config, protocol, mode == "recover-member")?;
    let owner = service.local().owner.identity();
    let mut observer = NativeCounterObserver::new(owner);
    let command_local = service_access::server_local(id, owner.store.session);
    let mut command_generation = 0u64;
    let start = Instant::now();
    println!(
        "ready node={id} peer={peer_address} transport={protocol:?} command=127.0.0.1:{}",
        base + 100 + id as u16
    );
    let mut connection: Option<Connection> = None;
    let mut quit = false;
    let mut shutdown_started = None;
    loop {
        let time = now(start);
        // Observe command closure/deadline and cancel its exact pending work
        // before this iteration can authorize queued membership execution.
        if !quit && connection.is_none() {
            connection = accept_connection(&listener, access.is_some(), &mut command_generation)?;
        }
        let remove_reason = connection.as_mut().and_then(|c| {
            c.poll(
                access.as_ref(),
                command_local,
                SecureSessionGeneration::new(command_generation).unwrap(),
                time,
                |s| command(&mut service, &observer, s, &mut administration, &mut quit),
            )
        });

        if let Some(remove_reason) = remove_reason {
            cancel_connection(
                &mut service,
                &mut administration,
                &connection,
                remove_reason,
            )?;
            connection = None;
        }

        poll_service(
            &mut service,
            &mut observer,
            administration.as_ref(),
            &connection,
            access.as_ref(),
            time,
            owner,
        )?;
        outputs(&mut service, &mut connection)?;
        if let Some(admin) = administration.as_mut() {
            admin.tick(&mut service, quit)?;
            if let Some(reply) = admin.take_reply() {
                if let Some(c) = connection
                    .as_mut()
                    .filter(|c| matches!(c.phase, Phase::Pending(Pending::Configure(_))))
                {
                    c.reply(reply);
                }
            }
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
    observer.close();
    setup::join(service)?;
    println!("stopped node={id} workers_joined=true");
    Ok(())
}
fn main() -> Result<(), Failure> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let StartupOptions {
        protocol,
        deployment,
        admin_plan,
        service_access,
        remote_admin,
    } = startup_options(&mut args)?;
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
                (
                    protocol,
                    admin_plan.as_deref(),
                    service_access.as_deref(),
                    remote_admin,
                ),
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

impl Connection {
    fn poll(
        &mut self,
        access: Option<&service_access::Access>,
        local: voteboat::secure::LocalIdentity,
        generation: SecureSessionGeneration,
        time: MonoTime,
        mut command: impl FnMut(&str) -> Result<Phase, String>,
    ) -> Option<&'static str> {
        let mut remove = false;
        let mut remove_reason = "channel";
        if Instant::now() >= self.deadline {
            remove_reason = "deadline";
            remove = true;
        } else {
            match self.stream.poll(access, local, generation, time) {
                Err(_) => remove = true,
                Ok(false) => (),
                Ok(true) => match &mut self.phase {
                    Phase::Input { bytes, len } => match self.stream.read(&mut bytes[*len..]) {
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
                                    self.stream.authorize(access, group(), s, time)?;
                                    command(s)
                                });
                                match result {
                                    Ok(phase) => self.phase = phase,
                                    Err(e) => self.reply(format!("ERR {e}")),
                                }
                            } else if *len == bytes.len() {
                                self.reply("ERR command too long".into());
                            }
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                        Err(_) => remove = true,
                    },
                    Phase::Output { bytes, sent } => {
                        if *sent < bytes.len() {
                            match self.stream.write(&bytes[*sent..]) {
                                Ok(0) => remove = true,
                                Ok(n) => *sent += n,
                                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                                Err(_) => remove = true,
                            }
                        }
                        remove |= *sent == bytes.len() && self.stream.is_flushed();
                    }
                    Phase::Pending(_) => (),
                },
            }
        }
        remove.then_some(remove_reason)
    }
}
fn cancel_connection(
    service: &mut Service,
    administration: &mut Option<administration::Administration>,
    connection: &Option<Connection>,
    remove_reason: &str,
) -> Result<(), Failure> {
    // Deadline and TLS/channel failure release the same pending host
    // ticket; cancellation cannot undo an already committed command.
    if let Some(Connection {
        phase: Phase::Pending(p),
        ..
    }) = connection
    {
        match *p {
            Pending::Write(t) => {
                checked(service.cancel_client(t))?;
            }
            Pending::Read(t) => {
                checked(service.cancel_read(t))?;
            }
            Pending::Configure(_) => {
                if let Some(admin) = administration.as_mut() {
                    admin.cancel_remote(service, remove_reason)?;
                }
            }
        }
    }
    Ok(())
}
struct StartupOptions {
    protocol: NativePeerProtocol,
    deployment: Option<std::path::PathBuf>,
    admin_plan: Option<std::path::PathBuf>,
    service_access: Option<std::path::PathBuf>,
    remote_admin: administration::Mode,
}
fn startup_options(args: &mut Vec<String>) -> Result<StartupOptions, Failure> {
    let mut protocol = NativePeerProtocol::TcpTls;
    let mut transport_selected = false;
    let mut deployment = None;
    let mut admin_plan = None;
    let mut service_access = None;
    let mut remote_admin = administration::Mode::Automatic;
    while args.first().is_some_and(|a| a == "serve" || a == "enroll") && args.len() >= 3 {
        let flag = args[args.len() - 2].as_str();
        if !matches!(
            flag,
            "--transport"
                | "--deployment"
                | "--admin-plan"
                | "--remote-admin-plan"
                | "--remote-admin-policy"
                | "--service-access"
        ) {
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
            flag @ ("--admin-plan" | "--remote-admin-plan" | "--remote-admin-policy")
                if admin_plan.is_none() && args[0] == "serve" =>
            {
                remote_admin = match flag {
                    "--remote-admin-plan" => administration::Mode::Provisioned,
                    "--remote-admin-policy" => administration::Mode::Targets,
                    _ => administration::Mode::Automatic,
                };
                admin_plan = Some(std::path::PathBuf::from(value))
            }
            "--service-access" if service_access.is_none() && args[0] == "serve" => {
                service_access = Some(std::path::PathBuf::from(value));
            }
            _ => return Err("duplicate or unsupported startup option".into()),
        }
    }
    Ok(StartupOptions {
        protocol,
        deployment,
        admin_plan,
        service_access,
        remote_admin,
    })
}

fn poll_service(
    service: &mut Service,
    observer: &mut NativeCounterObserver,
    administration: Option<&administration::Administration>,
    connection: &Option<Connection>,
    access: Option<&service_access::Access>,
    time: MonoTime,
    owner: RuntimeOwner,
) -> Result<(), Failure> {
    let result = if let Some(admin) = administration {
        service.poll_with_configuration_authorization(
                time,
                NodePollBudget::default(),
                |core, proposal| {
                    if admin.remote() {
                        let live = connection.as_ref().filter(|c| Instant::now() < c.deadline && matches!(c.phase,
                            Phase::Pending(Pending::Configure(operation)) if operation == proposal.record.operation))
                            .ok_or(voteboat::raft::ConfigurationProposalError::AuthenticationRequired)?;
                        live.stream.authorize(access, group(), "configure", time)
                            .map_err(|_| voteboat::raft::ConfigurationProposalError::AuthenticationRequired)?;
                    }
                    admin
                        .authorize(core.state().bootstrap.group, core.membership(), proposal)
                },
            )
    } else {
        service.poll(time, NodePollBudget::default())
    };
    // Diagnostics run after poll and cannot replace its original result.
    let _ = observer.record_bounded(NodeObservation::from_poll(
        owner,
        time,
        service.state(),
        &result,
    ));
    let progress = checked(result)?;
    if let Some(replica) = progress.replica {
        for step in replica.steps {
            // Client/read errors are reported through their exact output tickets.
            if let Some(error) = step.error {
                eprintln!("event: {error:?}");
            }
        }
    }
    Ok(())
}
fn accept_connection(
    listener: &TcpListener,
    authenticated: bool,
    generation: &mut u64,
) -> Result<Option<Connection>, Failure> {
    match listener.accept() {
        Ok((stream, _)) => {
            stream.set_nonblocking(true)?;
            *generation = generation
                .checked_add(1)
                .ok_or("command generation exhausted")?;
            Ok(Some(Connection {
                stream: service_access::Channel::server(stream, authenticated),
                phase: Phase::Input {
                    bytes: [0; 256],
                    len: 0,
                },
                deadline: Instant::now() + Duration::from_secs(5),
            }))
        }
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn checked_service_mode(
    mode: &str,
    input: &setup::PeerInput<'_>,
    remote: bool,
    has_access: bool,
    has_plan: bool,
) -> Result<bool, Failure> {
    if remote && (!has_access || !has_plan) {
        return Err("remote administration requires service access and provisioned plan".into());
    }
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
    if has_plan && mode != "recover-member" {
        return Err("administration plan requires recover-member".into());
    }
    Ok(create)
}
