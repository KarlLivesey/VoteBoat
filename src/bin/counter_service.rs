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
#[path = "support/command_client.rs"]
mod command_client;
#[path = "support/command_discovery.rs"]
mod command_discovery;
#[path = "support/command_endpoints.rs"]
mod command_endpoints;
#[path = "support/counter_application.rs"]
mod counter_application;
#[path = "support/credential_reload.rs"]
mod credential_reload;
#[path = "support/diagnostics.rs"]
mod diagnostics;
#[path = "support/leadership_commands.rs"]
mod leadership_commands;
#[path = "support/local_client.rs"]
mod local_client;
#[path = "support/placement_format.rs"]
mod placement_format;
#[path = "support/placement_input.rs"]
mod placement_input;
#[path = "support/placement_plan.rs"]
mod placement_plan;
#[path = "support/policy_input.rs"]
mod policy_input;
#[path = "support/quorum_diagnostics.rs"]
mod quorum_diagnostics;
#[path = "support/service_access.rs"]
mod service_access;
#[path = "support/service_setup.rs"]
mod service_setup;
#[path = "support/counter_setup.rs"]
mod setup;
use diagnostics::Diagnostics;
use setup::{checked, group, Failure, Service};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    path::Path,
    time::{Duration, Instant},
};
use voteboat::membership::ConfigurationResumeAction;
use voteboat::observability::*;
use voteboat::worker::PersistenceWorker;
use voteboat::{identity::*, native::connect::NativePeerProtocol, raft::RaftError, runtime::*};

const HELP: &str =
    "voteboat-counter serve create|recover|recover-member DIRECTORY NODE BASE_PORT TLS_DIRECTORY [PEERS_FILE | --deployment FILE] [--transport tcp|quic] [--admin-plan FILE | --remote-admin-plan FILE | --remote-admin-policy FILE] [--service-access FILE] [--command-listen ADDRESS] [--discovery-peers FILE] [--wal-reclaim-ms MS] [--checkpoint-entries N]\n\
voteboat-counter placement-plan INPUT learner OPERATION | replace RETIRING LEARNER_OPERATION VOTER_OPERATION retire|retain | voters OPERATION retire|retain POLICY\n\
voteboat-counter enroll create|recover DIRECTORY NODE BASE_PORT TLS_DIRECTORY SOURCE_DIRECTORY SOURCE_NODE [PEERS_FILE | --deployment FILE]\n\
voteboat-counter client BASE_PORT NODE status|metrics|timings|maintenance|configuration-status OPERATION_ID|configure OPERATION_ID|read|add OPERATION_ID DELTA|checkpoint|quit\n\
voteboat-counter client BASE_PORT auto read|add OPERATION_ID DELTA\n\
voteboat-counter client BASE_PORT NODE events SESSION AFTER LIMIT\n\
voteboat-counter client BASE_PORT NODE explain-quorum NODES_CSV_OR_DASH OFFSET COUNT\n\
Default peer ports are BASE+1..3; local command ports are BASE+101..103.\n\
TLS_DIRECTORY contains ca.der, node1..3.der and node1..3-key.der.\n\
Commands default to trusted loopback controls. --command-listen requires --service-access. Peer traffic uses mutual TLS.\n\
recover-member explicitly verifies existing membership journals and selects wire format 7 on all peers.\n\
enroll is an offline trusted handoff; stop source and destination before use and preserve files on failure.\n\
QUIC requires a build with --features quic; TCP is the default.\n\
PEERS_FILE lines: NODE SOCKET_ADDRESS TLS_SERVER_NAME.\n\
recover-member accepts --deployment FILE instead of PEERS_FILE; trailing named options may appear in any order.\n\
--admin-plan FILE is trusted startup input for recover-member; see docs/COUNTER_SERVICE.md for grammar and restart rules.\n\
Optional authenticated commands: serve --service-access FILE; client ... --service-tls TLS_DIRECTORY --principal ID.\n\
Endpoint discovery: client ... --discover-via NODE --command-peers FILE --service-tls TLS_DIRECTORY --principal ID.\n\
Remote command routing: client ... --command-peers FILE --service-tls TLS_DIRECTORY --principal ID.\n\
--wal-reclaim-ms enables physical reclamation; --checkpoint-entries enables automatic checkpoints.\n\
Use the same operation ID and delta when retrying an unknown write.\n\
Maintenance profile: serve ... --service-access FILE --leadership-maintenance enabled; all peers use schema2/wire8.\n\
Commands: move-leader OP CONFIG TARGET STORE INC; leadership-status OP; resume-leadership OP; cancel-leadership OP.\n\
Authenticated local credential reload: reload-access REQUEST EXPECTED NEXT; credential-status REQUEST.";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Pending {
    Write(ClientTicket),
    Read(ReadInvocationTicket),
    Configure(OperationId),
    CancelLeadership(voteboat::maintenance::LeadershipRecord),
}
enum Phase {
    Input {
        bytes: [u8; 256],
        len: usize,
    },
    Pending(Pending),
    DiscoveryAck {
        source: command_discovery::Source,
        sent: usize,
    },
    Discovery(Box<command_discovery::Server>),
    Output {
        bytes: Vec<u8>,
        sent: usize,
    },
}
struct Connection {
    stream: service_access::Channel,
    phase: Phase,
    deadline: Instant,
    started: Instant,
}
impl Connection {
    fn observe_close(&self, observer: &mut Diagnostics, reason: &str, time: MonoTime) {
        let kind = if reason == "complete" {
            TimingKind::ConnectionCompleted
        } else {
            TimingKind::ConnectionInterrupted
        };
        observer.record_timing(kind, self.started.elapsed(), time);
    }
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
fn maintenance_status(service: &Service) -> String {
    let status = service.wal_maintenance();
    let event = status.last_completion.as_ref();
    let reclaimed = event.and_then(|e| e.result.as_ref().ok());
    format!("OK enabled={} pending={} next_ms={:?} last_sequence={} before_bytes={} after_bytes={} admission_error={:?} completion_error={:?} checkpoint_enabled={} checkpoint_pending={} checkpoint_base={}",
        status.policy.is_some(), status.pending.is_some(), status.next_deadline.map(|d| d.0),
        event.map_or(0, |e| e.request.sequence), reclaimed.map_or(0, |r| r.before_bytes),
        reclaimed.map_or(0, |r| r.after_bytes), status.last_admission_error,
        event.and_then(|e| e.result.as_ref().err()), service.checkpoints().policy.is_some(),
        service.checkpoints().pending.len(), service.local().owner.core(group()).map_or(0, |c| c.state().base_index()))
}
fn command(
    service: &mut Service,
    observer: &Diagnostics,
    command: &str,
    administration: &mut Option<administration::Administration>,
    quit: &mut bool,
    credentials: &mut credential_reload::Commands<'_>,
    discovery: &Option<command_discovery::Source>,
) -> Result<Phase, String> {
    let words = command.split_whitespace().collect::<Vec<_>>();
    let reply = match words.as_slice() {
        ["discover"] => return Ok(Phase::DiscoveryAck { source: discovery.clone().ok_or("endpoint discovery disabled")?, sent: 0 }),
        ["credential-status" | "reload-access", ..] => credentials.command(&words)?,
        ["maintenance"] => maintenance_status(service),
        ["events", session, after, count] => observer.events(session, after, count)?,
        ["metrics"] => observer.metrics(),
        ["timings"] => observer.timings(),
        ["explain-quorum", voters, offset, count] => quorum_diagnostics::explain(service, voters, offset, count)?,
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
                    bytes: service.local().applications[&group()].data(delta)?,
                })
                .map_err(|r| match r.reason {
                    ClientError::Consensus(RaftError::NotLeader) => "NOT_LEADER".into(),
                    other => format!("{other:?}"),
                })?;
            return Ok(Phase::Pending(Pending::Write(ticket)));
        }
        ["read"] => {
            let ticket = service.read(group(), counter_application::Query::Data(())).map_err(|r| match r.reason {
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
            return Err("expected status, metrics, timings, explain-quorum NODES OFFSET COUNT, events SESSION AFTER LIMIT, maintenance, configuration-status OPERATION_ID, read, add OPERATION_ID DELTA, checkpoint or quit".into())
        }
    };
    Ok(Phase::Output {
        bytes: format!("{reply}\n").into_bytes(),
        sent: 0,
    })
}
fn outputs(
    service: &mut Service,
    connection: &mut Option<Connection>,
    leadership: &mut leadership_commands::Driver,
) -> Result<(), Failure> {
    while let Some(output) = service.poll_client() {
        let original_ticket = output.ticket();
        let ticket = Pending::Write(original_ticket);
        let result = checked(service.complete_client(output).map_err(|r| r.reason))?;
        leadership.complete(original_ticket, &result);
        if let Some(c) = connection
            .as_mut()
            .filter(|c| matches!(c.phase, Phase::Pending(p) if p == ticket))
        {
            c.reply(match result {
                ClientOutcome::Applied { receipt, .. } => leadership_commands::receipt(&receipt),
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
                } => leadership_commands::read(&value),
                ReadOutcome::NotRead(RaftError::NotLeader) => "ERR NOT_LEADER".into(),
                other => format!("ERR {other:?}"),
            });
        }
    }
    Ok(())
}
struct PreparedService {
    service: Service,
    credentials: credential_reload::Credentials,
    administration: Option<administration::Administration>,
    listener: TcpListener,
    peer_address: std::net::SocketAddr,
    discovery: Option<command_discovery::Source>,
}
fn prepare_service(
    mode: &str,
    root: &Path,
    id: u64,
    base: u16,
    tls: &Path,
    input: setup::PeerInput<'_>,
    options: &StartupOptions,
) -> Result<PreparedService, Failure> {
    let protocol = options.protocol;
    let plan_path = options.admin_plan.as_deref();
    let access_path = options.service_access.as_deref();
    let admin_mode = options.remote_admin;
    let command_address =
        command_endpoints::listener(options.command_listen, access_path.is_some(), base, id)?;
    let mut discovery =
        command_discovery::Source::load(options.discovery_peers.as_deref(), access_path.is_some())?;
    let remote = admin_mode != administration::Mode::Automatic;
    let create = checked_service_mode(
        mode,
        &input,
        remote,
        access_path.is_some(),
        plan_path.is_some(),
    )?;
    if options.leadership_maintenance && (access_path.is_none() || plan_path.is_some()) {
        return Err("leadership maintenance requires --service-access and a separate profile without an administration plan".into());
    }
    let config = setup::configuration(root, id, base, tls, create, input)?;
    let credentials = credential_reload::Credentials::load(
        root,
        access_path,
        tls,
        id,
        voteboat::secure::PeerIdentity {
            node: config.startup.node,
            store: config.startup.store,
        },
    )?;
    let administration = plan_path
        .map(|path| {
            administration::Administration::load(path, &config.provisioned_stores, admin_mode)
        })
        .transpose()?;
    let listener = TcpListener::bind(command_address)?;
    listener.set_nonblocking(true)?;
    let peer_address = config.startup.listen;
    let mut service = setup::open(
        config,
        protocol,
        mode == "recover-member",
        options.leadership_maintenance,
    )?;
    options.configure_maintenance(&mut service)?;
    if let Some(source) = discovery.as_mut() {
        source.bind_session(service.local().owner.identity().store.session.get());
    }
    Ok(PreparedService {
        service,
        credentials,
        administration,
        listener,
        peer_address,
        discovery,
    })
}
fn serve(
    mode: &str,
    root: &Path,
    id: u64,
    base: u16,
    tls: &Path,
    input: setup::PeerInput<'_>,
    options: &StartupOptions,
) -> Result<(), Failure> {
    let prepared = prepare_service(mode, root, id, base, tls, input, options)?;
    let mut service = prepared.service;
    // Declare after service: error exits join the credential worker before
    // releasing the service's exclusive data-directory ownership.
    let mut credentials = prepared.credentials;
    let mut administration = prepared.administration;
    let listener = prepared.listener;
    let peer_address = prepared.peer_address;
    let protocol = options.protocol;
    let owner = service.local().owner.identity();
    let mut observer = Diagnostics::new(owner).map_err(|e| format!("diagnostic setup: {e:?}"))?;
    let command_local = service_access::server_local(id, owner.store.session);
    let discovery = prepared.discovery;
    let mut command_generation = 0u64;
    let start = Instant::now();
    println!(
        "ready node={id} peer={peer_address} transport={protocol:?} command={}",
        listener.local_addr()?
    );
    let mut connection: Option<Connection> = None;
    let mut quit = false;
    let mut shutdown_started = None;
    let mut leadership = leadership_commands::Driver::default();
    loop {
        let time = now(start);
        if credentials.poll() {
            eprintln!("credential reload has uncertain durable state; stopping");
            quit = true;
        }
        // Observe command closure/deadline and cancel its exact pending work
        // before this iteration can authorize queued membership execution.
        if !quit && connection.is_none() {
            connection = accept_connection(
                &listener,
                credentials.access.is_some(),
                &mut command_generation,
            )?;
        }
        let (access, mut commands) = credentials.split();
        let remove_reason = connection.as_mut().and_then(|c| {
            c.poll(
                access,
                command_local,
                SecureSessionGeneration::new(command_generation).unwrap(),
                time,
                |s| {
                    if leadership_commands::is_command(s.split_whitespace().next()) {
                        return leadership
                            .command(&mut service, &s.split_whitespace().collect::<Vec<_>>());
                    }
                    command(
                        &mut service,
                        &observer,
                        s,
                        &mut administration,
                        &mut quit,
                        &mut commands,
                        &discovery,
                    )
                },
            )
        });

        if let Some(remove_reason) = remove_reason {
            finish_connection(
                &mut service,
                &mut administration,
                &connection,
                remove_reason,
                &mut observer,
                time,
            )?;
            connection = None;
        }

        poll_service(
            &mut service,
            &mut observer,
            administration.as_ref(),
            &connection,
            credentials.access.as_ref(),
            time,
            owner,
        )?;
        outputs(&mut service, &mut connection, &mut leadership)?;
        leadership.advance_cancel(&mut service, &mut connection);
        leadership.tick(&mut service, quit)?;
        advance_administration(&mut service, &mut administration, &mut connection, quit)?;
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
    credentials.finish()?;
    observer.close();
    setup::join(service)?;
    println!("stopped node={id} workers_joined=true");
    Ok(())
}
fn advance_administration(
    service: &mut Service,
    administration: &mut Option<administration::Administration>,
    connection: &mut Option<Connection>,
    quit: bool,
) -> Result<(), Failure> {
    if let Some(admin) = administration.as_mut() {
        admin.tick(service, quit)?;
        if let Some(reply) = admin.take_reply() {
            if let Some(c) = connection
                .as_mut()
                .filter(|c| matches!(c.phase, Phase::Pending(Pending::Configure(_))))
            {
                c.reply(reply);
            }
        }
    }

    Ok(())
}
fn main() -> Result<(), Failure> {
    let mut args = std::env::args().skip(1).collect::<Vec<_>>();
    let options = startup_options(&mut args)?;
    match args.as_slice() {
        [verb, path, rest @ ..] if verb == "placement-plan" => {
            placement_plan::run(Path::new(path), rest)
        }
        [enroll_arg, mode, root, id, base, tls, source, source_id, rest @ ..]
            if enroll_arg == "enroll" && rest.len() <= 1 =>
        {
            let (base, id) = ports(base, id)?;
            let create = match mode.as_str() {
                "create" => true,
                "recover" => false,
                _ => return Err("expected enrollment create or recover".into()),
            };
            let input = match (options.deployment.as_deref(), rest.first()) {
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
            let input = match (options.deployment.as_deref(), rest.first()) {
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
                &options,
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
        access: Option<&service_access::ActiveAccess>,
        local: voteboat::secure::LocalIdentity,
        generation: SecureSessionGeneration,
        time: MonoTime,
        mut command: impl FnMut(&str) -> Result<Phase, String>,
    ) -> Option<&'static str> {
        if let Phase::Discovery(server) = &mut self.phase {
            if Instant::now() >= self.deadline {
                return Some("deadline");
            }
            return server
                .poll(time, voteboat::secure::SessionPollBudget::default())
                .err()
                .map(|_| "discovery channel");
        }
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
                        if *sent == bytes.len() && self.stream.is_flushed() {
                            remove = true;
                            remove_reason = "complete";
                        }
                    }
                    Phase::DiscoveryAck { source, sent } => {
                        let bytes = command_discovery::ACK.as_bytes();
                        match self.stream.write(&bytes[*sent..]) {
                            Ok(0) if *sent != bytes.len() => remove = true,
                            Ok(n) => *sent += n,
                            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                            Err(_) => remove = true,
                        }
                        if !remove && *sent == bytes.len() && self.stream.is_flushed() {
                            let server = self
                                .stream
                                .take_secure()
                                .ok_or("missing authenticated session".into())
                                .and_then(|session| source.clone().serve(session, time));
                            match server {
                                Ok(server) => self.phase = Phase::Discovery(Box::new(server)),
                                Err(_) => remove = true,
                            }
                        }
                    }
                    Phase::Pending(_) | Phase::Discovery(_) => (),
                },
            }
        }
        remove.then_some(remove_reason)
    }
}
fn finish_connection(
    service: &mut Service,
    administration: &mut Option<administration::Administration>,
    connection: &Option<Connection>,
    remove_reason: &str,
    observer: &mut Diagnostics,
    time: MonoTime,
) -> Result<(), Failure> {
    if let Some(c) = connection {
        c.observe_close(observer, remove_reason, time);
    }
    // Deadline and TLS/channel failure release the same pending host
    // ticket; cancellation cannot undo an already committed command.
    if let Some(Connection {
        phase: Phase::Pending(p),
        ..
    }) = connection
    {
        eprintln!("command observation_cancelled pending={p:?} reason={remove_reason}");
        match *p {
            Pending::Write(t) => {
                checked(service.cancel_client(t))?;
            }
            Pending::Read(t) => {
                checked(service.cancel_read(t))?;
            }
            Pending::CancelLeadership(_) => (),
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
    command_listen: Option<SocketAddr>,
    discovery_peers: Option<std::path::PathBuf>,
    remote_admin: administration::Mode,
    wal_reclaim_ms: Option<u64>,
    checkpoint_entries: Option<u64>,
    leadership_maintenance: bool,
}
impl StartupOptions {
    fn configure_maintenance(&self, service: &mut Service) -> Result<(), Failure> {
        if let Some(interval_ms) = self.wal_reclaim_ms {
            checked(
                service.configure_wal_maintenance(Some(WalMaintenancePolicy {
                    interval_ms,
                    retry_ms: interval_ms,
                    max_bytes: service
                        .local()
                        .persistence
                        .reclaim_limit()
                        .ok_or("reclamation unavailable")?,
                })),
            )?;
        }
        if let Some(min_entries) = self.checkpoint_entries {
            checked(service.configure_checkpoints(Some(CheckpointPolicy {
                min_entries,
                interval_ms: 100,
                scan_groups: 64,
                max_in_flight: 4,
            })))?;
        }
        Ok(())
    }
}
fn positive_option(value: &str, name: &str) -> Result<u64, Failure> {
    let n = value.parse::<u64>()?;
    if n == 0 {
        return Err(format!("{name} must be positive").into());
    }
    Ok(n)
}
fn startup_options(args: &mut Vec<String>) -> Result<StartupOptions, Failure> {
    let mut protocol = NativePeerProtocol::TcpTls;
    let mut transport_selected = false;
    let mut deployment = None;
    let mut admin_plan = None;
    let mut service_access = None;
    let mut command_listen = None;
    let mut discovery_peers = None;
    let mut remote_admin = administration::Mode::Automatic;
    let mut wal_reclaim_ms = None;
    let mut checkpoint_entries = None;
    let mut leadership_maintenance = None;
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
                | "--command-listen"
                | "--discovery-peers"
                | "--wal-reclaim-ms"
                | "--checkpoint-entries"
                | "--leadership-maintenance"
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
            "--discovery-peers" if discovery_peers.is_none() && args[0] == "serve" => {
                discovery_peers = Some(std::path::PathBuf::from(value));
            }
            "--command-listen" if command_listen.is_none() && args[0] == "serve" => {
                command_listen = Some(value.parse()?);
            }
            "--wal-reclaim-ms" if wal_reclaim_ms.is_none() && args[0] == "serve" => {
                wal_reclaim_ms = Some(positive_option(&value, "WAL reclaim interval")?);
            }
            "--leadership-maintenance"
                if leadership_maintenance.is_none() && args[0] == "serve" =>
            {
                if value != "enabled" {
                    return Err("expected --leadership-maintenance enabled".into());
                }
                leadership_maintenance = Some(true);
            }
            "--checkpoint-entries" if checkpoint_entries.is_none() && args[0] == "serve" => {
                checkpoint_entries = Some(positive_option(&value, "checkpoint entry threshold")?);
            }
            _ => return Err("duplicate or unsupported startup option".into()),
        }
    }
    Ok(StartupOptions {
        protocol,
        deployment,
        admin_plan,
        service_access,
        command_listen,
        discovery_peers,
        remote_admin,
        wal_reclaim_ms,
        checkpoint_entries,
        leadership_maintenance: leadership_maintenance.unwrap_or(false),
    })
}

fn poll_service(
    service: &mut Service,
    observer: &mut Diagnostics,
    administration: Option<&administration::Administration>,
    connection: &Option<Connection>,
    access: Option<&service_access::ActiveAccess>,
    time: MonoTime,
    owner: RuntimeOwner,
) -> Result<(), Failure> {
    let started = Instant::now();
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
    let kind = if result.is_ok() {
        TimingKind::PollCompleted
    } else {
        TimingKind::PollFailed
    };
    observer.record_timing(kind, started.elapsed(), time);
    observer.record(NodeObservation::from_poll(
        owner,
        time,
        service.state(),
        &result,
    ));
    checked(result)?;
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
            let started = Instant::now();
            Ok(Some(Connection {
                stream: service_access::Channel::server(stream, authenticated),
                phase: Phase::Input {
                    bytes: [0; 256],
                    len: 0,
                },
                deadline: started + Duration::from_secs(5),
                started,
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
