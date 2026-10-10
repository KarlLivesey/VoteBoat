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
#[path = "support/administration_set.rs"]
mod administration_set;
#[path = "support/assignment_listing.rs"]
mod assignment_listing;
#[path = "support/command_client.rs"]
mod command_client;
#[path = "support/command_discovery.rs"]
mod command_discovery;
#[path = "support/command_endpoints.rs"]
mod command_endpoints;
#[path = "support/command_observation.rs"]
mod command_observation;
#[path = "support/counter_application.rs"]
mod counter_application;
#[path = "support/credential_reload.rs"]
mod credential_reload;
#[path = "support/credential_worker.rs"]
mod credential_worker;
#[path = "support/diagnostics.rs"]
mod diagnostics;
#[path = "support/drain_commands.rs"]
mod drain_commands;
#[path = "support/drain_runner.rs"]
mod drain_runner;
#[path = "support/drain_service.rs"]
mod drain_service;
#[path = "support/group_command.rs"]
mod group_command;
#[path = "support/group_drain.rs"]
mod group_drain;
#[path = "support/group_drain_plan.rs"]
mod group_drain_plan;
#[path = "support/group_setup.rs"]
mod group_setup;
#[path = "support/leadership_commands.rs"]
mod leadership_commands;
#[path = "support/leadership_set.rs"]
mod leadership_set;
#[path = "support/local_client.rs"]
mod local_client;
#[cfg(test)]
#[path = "support/operator_shutdown.rs"]
mod operator_shutdown;
#[path = "support/peer_credentials.rs"]
mod peer_credentials;
#[path = "support/peer_discovery.rs"]
mod peer_discovery;
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
#[path = "support/peer_metrics.rs"]
mod peer_metrics;
use setup::{checked, group, Failure, Service};
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener},
    path::Path,
    time::{Duration, Instant},
};
use voteboat::observability::*;
use voteboat::worker::PersistenceWorker;
use voteboat::{identity::*, native::connect::NativePeerProtocol, raft::RaftError, runtime::*};

const HELP: &str =
    "voteboat-counter serve create|recover|recover-member DIRECTORY NODE BASE_PORT TLS_DIRECTORY [PEERS_FILE | --deployment FILE] [--transport tcp|quic] [--admin-plan FILE | --remote-admin-plan FILE | --remote-admin-policy FILE] [--service-access FILE] [--command-listen ADDRESS] [--discovery-peers FILE] [--wal-reclaim-ms MS] [--checkpoint-entries N]\n\
voteboat-counter placement-plan INPUT learner OPERATION | replace RETIRING LEARNER_OPERATION VOTER_OPERATION retire|retain | voters OPERATION retire|retain POLICY\n\
voteboat-counter enroll create|recover DIRECTORY NODE BASE_PORT TLS_DIRECTORY SOURCE_DIRECTORY SOURCE_NODE [PEERS_FILE | --deployment FILE] [--leadership-maintenance enabled]\n\
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
Endpoint discovery: client ... --discover-via NODE --command-peers FILE --service-tls TLS_DIRECTORY --principal ID. Raft peer discovery: serve ... --peer-discovery FILE; see docs/COUNTER_SERVICE.md.\n\
Versioned discovery source: discovery-status; discovery-update EXPECTED NEXT NODE SOCKET_ADDRESS. Updates are local volatile hints; persist the v2 startup file separately.\n\
Remote command routing: client ... --command-peers FILE --service-tls TLS_DIRECTORY --principal ID.\n\
--wal-reclaim-ms enables physical reclamation; --checkpoint-entries enables automatic checkpoints.\n\
Use the same operation ID and delta when retrying an unknown write.\n\
Maintenance profile: serve ... --service-access FILE --leadership-maintenance enabled; all peers use schema2/wire8.\n\
Commands: move-leader OP CONFIG TARGET STORE INC; leadership-status OP; resume-leadership OP; cancel-leadership OP.\n\
Retained-replica drain: --node-drain enabled requires the maintenance profile.\n\
Commands: drain-node SEQUENCE OP CONFIG TARGET STORE INC; drain-status|resume-drain|cancel-drain|drain-stop SEQUENCE OP.\n\
Membership drain: source uses --membership-drain FILE and all peers use --remote-admin-plan FILE; see docs/MAINTENANCE.md.\n\
voteboat-counter drain-run BASE SOURCE SEQUENCE OP --service-tls TLS_DIRECTORY --principal ADMIN [--command-peers FILE]\n\
Authenticated local credential reload: reload-access REQUEST EXPECTED NEXT; credential-status REQUEST. Peer rotation: --peer-credentials FILE enables reload-peers REQUEST EXPECTED NEXT and peer-credential-status REQUEST.\n\
Multi-group data service: --groups FILE requires --service-access; see docs/MULTI_GROUP_STARTUP.md.\n\
Commands: group ID INC status|read|add OP DELTA|checkpoint; auto routing supports group reads and adds.\n\
Local inventory: list-assigned-groups CURSOR LIMIT (start with -; limit 1..8); requires Inspect on every local group.\n\
Multi-group membership: --group-admin-plans FILE selects per-group trusted plans; requires --groups and --service-access.\n\
Commands: group ID INC configure OP|configuration-status OP; address the selected group's leader for configure.\n\
Multi-group leadership: --groups FILE --leadership-maintenance enabled; use the same group prefix for move-leader, leadership-status, resume-leadership and cancel-leadership.\n\
Multi-group drain: --node-drain enabled on all peers, --group-drain-plan FILE on the source, plus group administration and maintenance profiles.\n\
voteboat-counter group-drain-run BASE SOURCE SEQUENCE OP --service-tls TLS_DIRECTORY --principal ADMIN [--command-peers FILE]\n\
Commands: drain-node SEQUENCE OP; drain-status|resume-drain|cancel-drain|drain-stop SEQUENCE OP; drain-group SEQUENCE OP OFFSET. See docs/MAINTENANCE.md.";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Pending {
    Write(ClientTicket),
    Read(ReadInvocationTicket),
    Configure(GroupIdentity, OperationId),
    CancelLeadership(GroupIdentity, voteboat::maintenance::LeadershipRecord),
    Drain(u64, OperationId),
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
fn discovery_command(
    source: &Option<command_discovery::Source>,
    words: &[&str],
) -> Option<Result<Phase, String>> {
    let result = match words {
        ["discover"] => source
            .clone()
            .ok_or_else(|| "endpoint discovery disabled".to_owned())
            .map(|source| Phase::DiscoveryAck { source, sent: 0 }),
        ["discovery-status"] => source
            .as_ref()
            .ok_or_else(|| "endpoint discovery disabled".to_owned())
            .map(|source| Phase::Output {
                bytes: format!("{}\n", source.status()).into_bytes(),
                sent: 0,
            }),
        ["discovery-update", fields @ ..] => source
            .as_ref()
            .ok_or_else(|| "endpoint discovery disabled".to_owned())
            .and_then(|source| source.update(fields))
            .map(|reply| Phase::Output {
                bytes: format!("{reply}\n").into_bytes(),
                sent: 0,
            }),
        _ => return None,
    };
    Some(result)
}
fn command(
    service: &mut Service,
    observer: &Diagnostics,
    command: &str,
    administration: &mut Option<administration_set::Administrations>,
    quit: &mut bool,
    credentials: &mut credential_reload::Commands<'_>,
    discovery: &Option<command_discovery::Source>,
) -> Result<Phase, String> {
    let selected = group_command::resolve(service, command)?;
    let group = selected.group;
    let words = selected.text.split_whitespace().collect::<Vec<_>>();
    if let Some(result) = discovery_command(discovery, &words) {
        return result;
    }
    let reply = match words.as_slice() {
        ["credential-status" | "reload-access", ..] => credentials.command(&words)?,
        ["maintenance"] => maintenance_status(service),
        ["list-assigned-groups", cursor, limit] => assignment_listing::list(service, cursor, limit)?,
        ["events", session, after, count] => observer.events(session, after, count)?,
        ["metrics"] => {
            let mut reply = observer.metrics();
            peer_metrics::append(service, &mut reply);
            reply
        },
        ["timings"] => observer.timings(),
        ["explain-quorum", voters, offset, count] => quorum_diagnostics::explain(service, voters, offset, count)?,
        ["configuration-status", operation] => administration::status(service, group, operation)?,
        ["configure-record", record @ ..] => {
            group_command::require_leader(service, group)?;
            let submission = administration.as_mut().ok_or("client-supplied targets disabled")?
                .get_mut(group)?.request_record(&record.join(" "), service)?;
            match submission {
                administration::RecordSubmission::Pending(operation) => return Ok(Phase::Pending(Pending::Configure(group, operation))),
                administration::RecordSubmission::Reply(reply) => reply,
            }
        }
        ["configure", operation] => {
            let operation = administration.as_mut().ok_or("remote administration disabled")?
                .request(service, group, operation)?;
            return Ok(Phase::Pending(Pending::Configure(group, operation)));
        }
        ["status"] => {
            let core = service.local().owner.core(group).ok_or("missing group")?;
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
                    group,
                    operation,
                    bytes: service.local().applications.get(&group).ok_or("missing group")?.data(delta)?,
                })
                .map_err(|r| match r.reason {
                    ClientError::Consensus(RaftError::NotLeader) => "NOT_LEADER".into(),
                    other => format!("{other:?}"),
                })?;
            return Ok(Phase::Pending(Pending::Write(ticket)));
        }
        ["read"] => {
            let ticket = service.read(group, counter_application::Query::Data(())).map_err(|r| match r.reason {
                ReadInvocationError::Consensus(RaftError::NotLeader) => "NOT_LEADER".into(),
                other => format!("{other:?}"),
            })?;
            return Ok(Phase::Pending(Pending::Read(ticket)));
        }
        ["checkpoint"] => {
            service
                .control(group, NodeControl::Checkpoint)
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
    leadership: &mut leadership_set::Leaders,
    drain: &mut Option<drain_service::Driver>,
) -> Result<(), Failure> {
    while let Some(output) = service.poll_client() {
        let original_ticket = output.ticket();
        let ticket = Pending::Write(original_ticket);
        let result = checked(service.complete_client(output).map_err(|r| r.reason))?;
        leadership.complete(original_ticket, &result);
        if let Some(drain) = drain {
            drain.complete(original_ticket, &result);
        }
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
    peers: Option<peer_credentials::Peers>,
    administration: Option<administration_set::Administrations>,
    listener: TcpListener,
    peer_address: std::net::SocketAddr,
    discovery: Option<command_discovery::Source>,
    drain: Option<drain_service::Driver>,
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
    let access_path = options.service_access.as_deref();
    let command_address =
        command_endpoints::listener(options.command_listen, access_path.is_some(), base, id)?;
    let mut discovery =
        command_discovery::Source::load(options.discovery_peers.as_deref(), access_path.is_some())?;
    let source = options
        .peer_discovery
        .as_deref()
        .map(peer_discovery::Prepared::load)
        .transpose()?;
    let create = options.checked_mode(mode, &input)?;
    options.validate_profiles(root)?;
    let mut config = setup::configuration(root, id, base, tls, create, input)?;
    let peers = options.prepare_peer_credentials(&mut config, mode)?;
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
    let administration = options.administration(&config.provisioned_stores)?;
    let drain_plan = options.drain_plan(&config.provisioned_stores, administration.as_ref())?;
    if let Some(plan) = &drain_plan {
        let owner = checked(plan.record(1))?.owner;
        if owner.node != config.startup.node || owner.store != config.startup.store {
            return Err("drain plan belongs to another local owner".into());
        }
    }
    let peer_address = config.startup.listen;
    let prepared = group_setup::prepare(
        config,
        options.groups.as_deref(),
        options.leadership_maintenance,
    )?;
    if administration
        .as_ref()
        .is_some_and(|a| a.groups().any(|g| !prepared.contains(g)))
    {
        return Err("administration group absent from startup manifest".into());
    }
    if let Some(plan) = &drain_plan {
        if options.groups.is_some()
            && (plan.assignments().len() != prepared.group_count()
                || plan
                    .assignments()
                    .iter()
                    .any(|g| !prepared.contains(g.group)))
        {
            return Err("group drain must cover the complete startup group set".into());
        }
    }
    let listener = TcpListener::bind(command_address)?;
    listener.set_nonblocking(true)?;
    let rotation = peers.as_ref().map(|p| p.startup(protocol));
    let mut service = peer_discovery::open(
        prepared,
        protocol,
        mode == "recover-member",
        options.leadership_maintenance,
        rotation,
        source,
    )?;
    options.configure_maintenance(&mut service)?;
    let drain = options
        .node_drain
        .then(|| {
            drain_service::Driver::open(
                &mut service,
                root,
                create,
                drain_plan,
                options.groups.is_some(),
            )
        })
        .transpose()?;
    if let Some(source) = discovery.as_mut() {
        source.bind_session(service.local().owner.identity().store.session.get());
    }
    Ok(PreparedService {
        service,
        credentials,
        peers,
        administration,
        listener,
        peer_address,
        discovery,
        drain,
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
    run_service(prepared, id, options.protocol)
}
fn peer_command(
    peers: &mut Option<peer_credentials::Peers>,
    text: &str,
) -> Option<Result<Phase, String>> {
    peer_credentials::command(peers, text).map(|result| {
        result.map(|reply| Phase::Output {
            bytes: format!("{reply}\n").into_bytes(),
            sent: 0,
        })
    })
}
fn run_service(
    prepared: PreparedService,
    id: u64,
    protocol: NativePeerProtocol,
) -> Result<(), Failure> {
    let mut service = prepared.service;
    // Declare after service: error exits join the credential worker before
    // releasing the service's exclusive data-directory ownership.
    let mut credentials = prepared.credentials;
    let mut peers = prepared.peers;
    let mut drain = prepared.drain;
    let mut administration = prepared.administration;
    let listener = prepared.listener;
    let peer_address = prepared.peer_address;
    let owner = service.local().owner.identity();
    let mut observer = Diagnostics::new(owner).map_err(|e| format!("diagnostic setup: {e:?}"))?;
    let command_local = service_access::server_local(id, owner.store.session);
    let discovery = prepared.discovery;
    let command_scopes = service.local().owner.groups().collect::<Vec<_>>();
    let mut command_generation = 0u64;
    let start = Instant::now();
    println!(
        "ready node={id} peer={peer_address} transport={protocol:?} command={}",
        listener.local_addr()?
    );
    let mut connection: Option<Connection> = None;
    let mut quit = false;
    let mut shutdown_started = None;
    let mut leadership = leadership_set::Leaders::new(&service);
    loop {
        let time = now(start);
        poll_credential_updates(&mut service, &mut credentials, &mut peers, &mut quit);
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
                &command_scopes,
                SecureSessionGeneration::new(command_generation).unwrap(),
                time,
                |s| {
                    if let Some(result) =
                        maintenance_command(&mut service, &mut leadership, &mut drain, s, &mut quit)
                    {
                        return result;
                    }
                    if let Some(result) = peer_command(&mut peers, s) {
                        return result;
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
        advance_leadership(
            &mut service,
            &mut connection,
            &mut leadership,
            &mut drain,
            quit,
        )?;
        advance_administration(&mut service, &mut administration, &mut connection, quit)?;
        advance_shutdown(
            &mut service,
            quit && connection.is_none(),
            &mut shutdown_started,
        )?;
        if service.is_drained() {
            break;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    finish_service(service, drain, credentials, peers, observer, id)
}
fn poll_credential_updates(
    service: &mut Service,
    credentials: &mut credential_reload::Credentials,
    peers: &mut Option<peer_credentials::Peers>,
    quit: &mut bool,
) {
    let peer_failure = !*quit && peers.as_mut().is_some_and(|p| p.poll(service));
    if credentials.poll() || peer_failure {
        eprintln!("credential reload has uncertain durable state; stopping");
        *quit = true;
    }
}
fn finish_service(
    service: Service,
    mut drain: Option<drain_service::Driver>,
    mut credentials: credential_reload::Credentials,
    mut peers: Option<peer_credentials::Peers>,
    mut observer: Diagnostics,
    id: u64,
) -> Result<(), Failure> {
    // The directory owner must outlive both possible publication workers.
    if let Some(drain) = &mut drain {
        drain.finish()?;
    }
    credentials.finish()?;
    if let Some(peers) = &mut peers {
        peers.finish()?;
    }
    observer.close();
    setup::join(service)?;
    println!("stopped node={id} workers_joined=true");
    Ok(())
}
fn advance_leadership(
    service: &mut Service,
    connection: &mut Option<Connection>,
    leadership: &mut leadership_set::Leaders,
    drain: &mut Option<drain_service::Driver>,
    quit: bool,
) -> Result<(), Failure> {
    outputs(service, connection, leadership, drain)?;
    // Consume accepted results while draining, but do not admit new operator
    // controls after the node has closed admission. Publication workers remain
    // owned by finish_service until their real completion is checked and joined.
    if service.state() != NodeState::Running {
        return Ok(());
    }
    if let Some(drain) = drain {
        drain.tick(service, leadership, connection)?;
    }
    leadership.advance_cancel(service, connection);
    leadership.tick(service, quit)
}
fn advance_shutdown(
    service: &mut Service,
    requested: bool,
    started: &mut Option<Instant>,
) -> Result<(), Failure> {
    if requested && started.is_none() {
        service.begin_shutdown();
        *started = Some(Instant::now());
    }
    if !service.is_drained() && started.is_some_and(|t| t.elapsed() > Duration::from_secs(10)) {
        return Err("shutdown timed out; recover durable state on restart".into());
    }
    Ok(())
}
fn advance_administration(
    service: &mut Service,
    administration: &mut Option<administration_set::Administrations>,
    connection: &mut Option<Connection>,
    quit: bool,
) -> Result<(), Failure> {
    if let Some(admin) = administration.as_mut() {
        admin.tick(service, quit)?;
        while let Some((group, operation, reply)) = admin.take_reply() {
            if let Some(c) = connection.as_mut().filter(|c| {
                matches!(c.phase,
                Phase::Pending(Pending::Configure(g, op)) if g == group && op == operation)
            }) {
                c.reply(reply);
            }
        }
    }

    Ok(())
}
fn maintenance_command(
    service: &mut Service,
    leadership: &mut leadership_set::Leaders,
    drain: &mut Option<drain_service::Driver>,
    input: &str,
    quit: &mut bool,
) -> Option<Result<Phase, String>> {
    let selected = match group_command::resolve(service, input) {
        Ok(selected) => selected,
        Err(error) => return Some(Err(error)),
    };
    let words = selected.text.split_whitespace().collect::<Vec<_>>();
    if drain_service::is_command(words.first().copied()) {
        return Some(
            drain
                .as_mut()
                .ok_or_else(|| "drain requires --node-drain enabled".into())
                .and_then(|d| d.command(service, leadership, &words, quit)),
        );
    }
    if !leadership_commands::is_command(words.first().copied()) {
        return None;
    }
    if drain.as_ref().is_some_and(drain_service::Driver::busy) {
        return Some(Err("drain publication in progress".into()));
    }
    Some(
        leadership
            .get_mut(selected.group)
            .and_then(|driver| driver.command(service, &words)),
    )
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
            let (index, term) = setup::enroll(
                config,
                Path::new(source),
                source_id.parse()?,
                options.leadership_maintenance,
            )?;
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
        [verb, base, source, rest @ ..] if verb == "drain-run" || verb == "group-drain-run" => {
            let (base, source) = ports(base, source)?;
            if verb == "group-drain-run" {
                drain_runner::run_multi(base, source, rest)
            } else {
                drain_runner::run(base, source, rest)
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
        command_scopes: &[GroupIdentity],
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
                                    let selected = group_command::parse(s)?;
                                    group_command::authorize(
                                        &self.stream,
                                        access,
                                        &selected,
                                        command_scopes,
                                        time,
                                    )?;
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
    administration: &mut Option<administration_set::Administrations>,
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
            Pending::CancelLeadership(_, _) | Pending::Drain(_, _) => (),
            Pending::Configure(group, _) => {
                if let Some(admin) = administration.as_mut() {
                    admin
                        .get_mut(group)?
                        .cancel_remote(service, remove_reason)?;
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
    peer_credentials: Option<std::path::PathBuf>,
    command_listen: Option<SocketAddr>,
    discovery_peers: Option<std::path::PathBuf>,
    peer_discovery: Option<std::path::PathBuf>,
    remote_admin: administration::Mode,
    wal_reclaim_ms: Option<u64>,
    checkpoint_entries: Option<u64>,
    leadership_maintenance: bool,
    node_drain: bool,
    membership_drain: Option<std::path::PathBuf>,
    groups: Option<std::path::PathBuf>,
    group_admin_plans: Option<std::path::PathBuf>,
    group_drain_plan: Option<std::path::PathBuf>,
}
impl StartupOptions {
    fn checked_mode(&self, mode: &str, input: &setup::PeerInput<'_>) -> Result<bool, Failure> {
        checked_service_mode(
            mode,
            input,
            self.remote_admin != administration::Mode::Automatic,
            self.service_access.is_some(),
            self.admin_plan.is_some(),
        )
    }
    fn prepare_peer_credentials(
        &self,
        config: &mut voteboat::native::startup::NativeMemberStartup,
        mode: &str,
    ) -> Result<Option<peer_credentials::Peers>, Failure> {
        self.peer_credentials
            .as_deref()
            .map(|path| {
                let wire = config.startup.tls.wire_version().max(
                    if self.groups.is_some() || self.leadership_maintenance {
                        8
                    } else if mode == "recover-member" {
                        7
                    } else {
                        1
                    },
                );
                peer_credentials::Peers::load(config, path, wire)
            })
            .transpose()
    }

    fn drain_plan(
        &self,
        stores: &std::collections::BTreeMap<NodeId, StoreIdentity>,
        administration: Option<&administration_set::Administrations>,
    ) -> Result<Option<voteboat::drain::MembershipDrainPlan>, Failure> {
        if let Some(path) = &self.group_drain_plan {
            return group_drain_plan::load(path, stores, administration).map(Some);
        }
        self.membership_drain
            .as_deref()
            .map(|path| {
                drain_commands::load_plan(
                    path,
                    stores,
                    administration
                        .ok_or("missing membership administration")?
                        .get(group())?,
                )
            })
            .transpose()
    }
    fn administration(
        &self,
        stores: &std::collections::BTreeMap<NodeId, StoreIdentity>,
    ) -> Result<Option<administration_set::Administrations>, Failure> {
        if let Some(path) = &self.group_admin_plans {
            return Ok(Some(administration_set::Administrations::load(
                path,
                stores,
                self.leadership_maintenance,
            )?));
        }
        self.admin_plan
            .as_deref()
            .map(|path| {
                administration::Administration::load(
                    path,
                    stores,
                    self.remote_admin,
                    self.leadership_maintenance,
                )
                .map(administration_set::Administrations::single)
            })
            .transpose()
    }
    fn validate_profiles(&self, root: &Path) -> Result<(), Failure> {
        peer_credentials::Peers::validate_profile(
            root,
            self.peer_credentials.as_deref(),
            self.service_access.is_some(),
        )?;
        if self.group_drain_plan.is_some() && (self.groups.is_none() || !self.node_drain) {
            return Err("--group-drain-plan requires --groups and --node-drain enabled".into());
        }
        if self.group_admin_plans.is_some()
            && (self.groups.is_none() || self.service_access.is_none())
        {
            return Err("--group-admin-plans requires --groups and --service-access".into());
        }
        if self.groups.is_some()
            && (self.service_access.is_none()
                || self.admin_plan.is_some()
                || self.discovery_peers.is_some())
        {
            return Err("--groups requires --service-access; multi-group administration uses --group-admin-plans; discovery profiles are not yet supported".into());
        }
        if self.leadership_maintenance
            && (self.service_access.is_none()
                || self.admin_plan.is_some()
                    && self.remote_admin != administration::Mode::Provisioned)
        {
            return Err("leadership maintenance requires --service-access; membership requires an explicitly requested --remote-admin-plan".into());
        }
        if self.node_drain && !self.leadership_maintenance {
            return Err("--node-drain requires --leadership-maintenance enabled".into());
        }
        if self.membership_drain.is_some()
            && (!self.node_drain || self.remote_admin != administration::Mode::Provisioned)
        {
            return Err("--membership-drain requires --node-drain and --remote-admin-plan".into());
        }
        if !self.node_drain && root.join("drain.record").try_exists()? {
            return Err(
            "existing drain journal requires --node-drain enabled; preserve the recovery profile"
                .into(),
        );
        }
        Ok(())
    }

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
fn enabled_option(value: &str, name: &str) -> Result<bool, Failure> {
    if value == "enabled" {
        Ok(true)
    } else {
        Err(format!("expected {name} enabled").into())
    }
}
fn is_startup_option(flag: &str) -> bool {
    matches!(
        flag,
        "--transport"
            | "--deployment"
            | "--admin-plan"
            | "--remote-admin-plan"
            | "--remote-admin-policy"
            | "--service-access"
            | "--command-listen"
            | "--discovery-peers"
            | "--peer-discovery"
            | "--wal-reclaim-ms"
            | "--checkpoint-entries"
            | "--leadership-maintenance"
            | "--node-drain"
            | "--membership-drain"
            | "--groups"
            | "--group-admin-plans"
            | "--group-drain-plan"
            | "--peer-credentials"
    )
}
fn parse_protocol(value: &str) -> Result<NativePeerProtocol, Failure> {
    match value {
        "tcp" => Ok(NativePeerProtocol::TcpTls),
        #[cfg(feature = "quic")]
        "quic" => Ok(NativePeerProtocol::Quic),
        #[cfg(not(feature = "quic"))]
        "quic" => Err("QUIC support requires building with --features quic".into()),
        _ => Err("expected --transport tcp or --transport quic".into()),
    }
}
fn startup_options(args: &mut Vec<String>) -> Result<StartupOptions, Failure> {
    let mut protocol = NativePeerProtocol::TcpTls;
    let mut transport_selected = false;
    let mut deployment = None;
    let mut admin_plan = None;
    let mut service_access = None;
    let mut peer_credentials = None;
    let mut command_listen = None;
    let mut discovery_peers = None;
    let mut peer_discovery = None;
    let mut remote_admin = administration::Mode::Automatic;
    let mut wal_reclaim_ms = None;
    let mut checkpoint_entries = None;
    let mut leadership_maintenance = None;
    let mut node_drain = None;
    let mut membership_drain = None;
    let mut groups = None;
    let mut group_admin_plans = None;
    let mut group_drain_plan = None;
    while args.first().is_some_and(|a| a == "serve" || a == "enroll") && args.len() >= 3 {
        let flag = args[args.len() - 2].as_str();
        if !is_startup_option(flag) {
            break;
        }
        let value = args.pop().unwrap();
        match args.pop().unwrap().as_str() {
            "--group-drain-plan" if group_drain_plan.is_none() && args[0] == "serve" => {
                group_drain_plan = Some(std::path::PathBuf::from(value));
            }
            "--group-admin-plans" if group_admin_plans.is_none() && args[0] == "serve" => {
                group_admin_plans = Some(std::path::PathBuf::from(value));
            }
            "--groups" if groups.is_none() && args[0] == "serve" => {
                groups = Some(std::path::PathBuf::from(value));
            }
            "--transport" if !transport_selected && args[0] == "serve" => {
                transport_selected = true;
                protocol = parse_protocol(&value)?;
            }
            "--deployment" if deployment.is_none() => {
                deployment = Some(std::path::PathBuf::from(value))
            }
            flag @ ("--admin-plan" | "--remote-admin-plan" | "--remote-admin-policy")
                if admin_plan.is_none() && args[0] == "serve" =>
            {
                remote_admin = administration_mode(flag);
                admin_plan = Some(std::path::PathBuf::from(value))
            }
            "--service-access" if service_access.is_none() && args[0] == "serve" => {
                service_access = Some(std::path::PathBuf::from(value));
            }
            "--peer-credentials" if peer_credentials.is_none() && args[0] == "serve" => {
                peer_credentials = Some(std::path::PathBuf::from(value));
            }
            "--discovery-peers" if discovery_peers.is_none() && args[0] == "serve" => {
                discovery_peers = Some(std::path::PathBuf::from(value));
            }
            "--peer-discovery" if peer_discovery.is_none() && args[0] == "serve" => {
                peer_discovery = Some(std::path::PathBuf::from(value));
            }
            "--command-listen" if command_listen.is_none() && args[0] == "serve" => {
                command_listen = Some(value.parse()?);
            }
            "--wal-reclaim-ms" if wal_reclaim_ms.is_none() && args[0] == "serve" => {
                wal_reclaim_ms = Some(positive_option(&value, "WAL reclaim interval")?);
            }
            "--leadership-maintenance" if leadership_maintenance.is_none() => {
                leadership_maintenance = Some(enabled_option(&value, "--leadership-maintenance")?);
            }
            "--node-drain" if node_drain.is_none() && args[0] == "serve" => {
                node_drain = Some(enabled_option(&value, "--node-drain")?);
            }
            "--membership-drain" if membership_drain.is_none() && args[0] == "serve" => {
                membership_drain = Some(std::path::PathBuf::from(value));
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
        peer_credentials,
        command_listen,
        discovery_peers,
        peer_discovery,
        remote_admin,
        wal_reclaim_ms,
        checkpoint_entries,
        leadership_maintenance: leadership_maintenance.unwrap_or(false),
        node_drain: node_drain.unwrap_or(false),
        membership_drain,
        groups,
        group_admin_plans,
        group_drain_plan,
    })
}

fn administration_mode(flag: &str) -> administration::Mode {
    match flag {
        "--remote-admin-plan" => administration::Mode::Provisioned,
        "--remote-admin-policy" => administration::Mode::Targets,
        _ => administration::Mode::Automatic,
    }
}

fn poll_service(
    service: &mut Service,
    observer: &mut Diagnostics,
    administration: Option<&administration_set::Administrations>,
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
                    let scope = core.state().bootstrap.group;
                    let admin = admin.get(scope).map_err(|_| voteboat::raft::ConfigurationProposalError::AuthenticationRequired)?;
                    if admin.remote() {
                        let live = connection.as_ref().filter(|c| Instant::now() < c.deadline && matches!(c.phase,
                            Phase::Pending(Pending::Configure(group, operation)) if group == scope && operation == proposal.record.operation))
                            .ok_or(voteboat::raft::ConfigurationProposalError::AuthenticationRequired)?;
                        live.stream.authorize(access, scope, "configure", time)
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
