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
//! Native replicated metadata authority with explicit trusted initial grants.
#[path = "support/authority_endpoints.rs"]
mod authority_endpoints;
#[path = "support/command_client.rs"]
mod command_client;
#[path = "support/command_endpoints.rs"]
mod command_endpoints;
#[path = "support/credential_reload.rs"]
mod credential_reload;
#[path = "support/credential_worker.rs"]
mod credential_worker;
#[path = "support/directory_client.rs"]
mod directory_client;
#[path = "support/directory_connection.rs"]
mod directory_connection;
#[path = "support/directory_owner.rs"]
mod directory_owner;
#[path = "support/directory_plan.rs"]
mod directory_plan;
#[path = "support/peer_credentials.rs"]
mod peer_credentials;
#[path = "support/policy_input.rs"]
mod policy_input;
#[path = "support/preview_input.rs"]
mod preview_input;
#[path = "support/route_client.rs"]
mod route_client;
#[path = "support/route_discovery.rs"]
mod route_discovery;
#[path = "support/service_access.rs"]
mod service_access;
#[path = "support/service_setup.rs"]
mod service_setup;
use service_setup as setup;
#[path = "support/transfer_preview.rs"]
mod transfer_preview;
use setup::Failure;
use std::{
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use voteboat::{
    directory::Directory,
    identity::SecureSessionGeneration,
    native::{
        connect::{NativePeerProtocol, NativeServiceConnector},
        node::NativeNode,
    },
    runtime::*,
};
type Node = NativeNode<Directory, NativeServiceConnector>;
const HELP: &str = "voteboat-directory split-preview PROFILE\nvoteboat-directory plan AUTHORITY INCARNATION RESPONSIBILITY INCARNATION EXECUTION_GROUP INCARNATION\nvoteboat-directory serve create|recover DIRECTORY NODE BASE TLS PLAN ACCESS [--command-listen ADDRESS] [--peers FILE | --deployment FILE] [--transport tcp|quic] [--peer-credentials FILE]\nvoteboat-directory client BASE NODE TLS PRINCIPAL status|initialize|publish OPERATION|checkpoint|quit|reload-peers REQUEST EXPECTED NEXT|peer-credential-status REQUEST|reload-access REQUEST EXPECTED NEXT|credential-status REQUEST [--command-peers FILE]\nvoteboat-directory lookup BASE NODE TLS PRINCIPAL GROUP INCARNATION RESPONSIBILITY INCARNATION [--command-peers FILE]\nvoteboat-directory route TLS PRINCIPAL AUTHORITY INCARNATION RESPONSIBILITY INCARNATION KEY_BYTE AUTHORITIES_FILE [--max-hops N] [--min-epoch N] [--min-generation N]";
fn ids(base: &str, id: &str) -> Result<(u16, u64), Failure> {
    let base: u16 = base.parse()?;
    let id: u64 = id.parse()?;
    if base == 0
        || !(1..=setup::MAX_NODE).contains(&id)
        || u64::from(base) + 100 + id > u64::from(u16::MAX)
    {
        return Err("invalid base or node".into());
    }
    Ok((base, id))
}
struct Options {
    listen: Option<SocketAddr>,
    peers: Option<PathBuf>,
    deployment: Option<PathBuf>,
    peer_credentials: Option<PathBuf>,
    protocol: NativePeerProtocol,
}
fn options(args: &[String]) -> Result<Options, Failure> {
    let mut o = Options {
        listen: None,
        peers: None,
        deployment: None,
        peer_credentials: None,
        protocol: NativePeerProtocol::TcpTls,
    };
    let mut transport = false;
    if !args.len().is_multiple_of(2) {
        return Err(HELP.into());
    }
    for pair in args.as_chunks::<2>().0 {
        match pair[0].as_str() {
            "--command-listen" if o.listen.is_none() => o.listen = Some(pair[1].parse()?),
            "--deployment" if o.deployment.is_none() => {
                o.deployment = Some(PathBuf::from(&pair[1]))
            }
            "--peers" if o.peers.is_none() => o.peers = Some(PathBuf::from(&pair[1])),
            "--peer-credentials" if o.peer_credentials.is_none() => {
                o.peer_credentials = Some(PathBuf::from(&pair[1]));
            }
            "--transport" if !transport => {
                transport = true;
                o.protocol = match pair[1].as_str() {
                    "tcp" => NativePeerProtocol::TcpTls,
                    #[cfg(feature = "quic")]
                    "quic" => NativePeerProtocol::Quic,
                    _ => return Err("unsupported transport".into()),
                };
            }
            _ => return Err("unknown or duplicate option".into()),
        }
    }
    if o.peers.is_some() && o.deployment.is_some() {
        return Err("select peers or deployment".into());
    }
    Ok(o)
}
fn serve(args: &[String]) -> Result<(), Failure> {
    let [mode, root, id, base, tls, plan, access, rest @ ..] = args else {
        return Err(HELP.into());
    };
    let (base, id) = ids(base, id)?;
    let options = options(rest)?;
    peer_credentials::Peers::validate_profile(
        Path::new(root),
        options.peer_credentials.as_deref(),
        true,
    )?;
    let create = match mode.as_str() {
        "create" => true,
        "recover" => false,
        _ => return Err("expected create or recover".into()),
    };
    let plan = directory_plan::Plan::load(Path::new(plan))?;
    let mut config = setup::configuration(
        Path::new(root),
        id,
        base,
        Path::new(tls),
        create,
        match options.deployment.as_deref() {
            Some(path) => setup::PeerInput::Deployment(path),
            None => setup::PeerInput::Legacy(options.peers.as_deref()),
        },
    )?;
    config.startup.bootstrap.group = plan.authority;
    let wire = config.startup.tls.wire_version();
    let peers = options
        .peer_credentials
        .as_deref()
        .map(|p| peer_credentials::Peers::load(&mut config, p, wire))
        .transpose()?;
    let access = credential_reload::Credentials::load(
        Path::new(root),
        Some(Path::new(access)),
        Path::new(tls),
        id,
        voteboat::secure::PeerIdentity {
            node: config.startup.node,
            store: config.startup.store,
        },
    )?;
    let digest = access
        .access
        .as_ref()
        .and_then(|a| a.material())
        .ok_or("missing command credentials")?
        .digest;
    let address = command_endpoints::listener(options.listen, true, base, id)?;
    let listener = TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    let node = match &peers {
        Some(p) => setup::open_application_with_rotation(
            config,
            options.protocol,
            false,
            plan.app.clone(),
            Some(p.startup(options.protocol)),
        )?,
        None => setup::open_application(config, options.protocol, false, plan.app.clone())?,
    };
    println!("credential_digest={digest:02x?}");
    run(node, plan, listener, access, peers, id)
}
fn run(
    node: Node,
    plan: directory_plan::Plan,
    listener: TcpListener,
    access: credential_reload::Credentials,
    peers: Option<peer_credentials::Peers>,
    id: u64,
) -> Result<(), Failure> {
    let local = service_access::server_local(id, node.local().owner.identity().store.session);
    let mut owner = directory_owner::Owner::new(node, plan.authority);
    // Accepted credential preparation must finish before directory ownership ends.
    let mut credentials = access;
    let mut peers = peers;
    let start = Instant::now();
    let mut connection = None;
    let mut sequence = 0u64;
    let mut quit = false;
    let mut shutdown = None;
    println!(
        "ready directory node={id} authority={:?} command={}",
        plan.authority,
        listener.local_addr()?
    );
    loop {
        let now = MonoTime(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
        let peer_failure = !quit && peers.as_mut().is_some_and(|p| p.poll(owner.node()));
        if credentials.poll() || peer_failure {
            eprintln!("credential reload has uncertain durable state; stopping");
            owner.close_remote();
            quit = true;
        }
        if !quit && connection.is_none() && !owner.remote() {
            match listener.accept() {
                Ok((stream, _)) => {
                    sequence = sequence
                        .checked_add(1)
                        .ok_or("session generation exhausted")?;
                    connection = Some(directory_connection::Connection::new(
                        stream,
                        SecureSessionGeneration::new(sequence).unwrap(),
                    )?);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                Err(e) => return Err(e.into()),
            }
        }
        if let Some(c) = connection.as_mut() {
            let (access, reload) = credentials.split();
            let mut commands = directory_connection::Commands {
                group: plan.authority,
                bootstrap: plan.bootstrap,
                records: &plan.commands,
                quit: &mut quit,
                credentials: reload,
                peers: &mut peers,
            };
            if c.poll(
                &mut owner,
                access.ok_or("missing command credentials")?,
                local,
                now,
                &mut commands,
            )? {
                connection = None;
            }
        }
        owner.poll(now)?;
        directory_connection::Connection::completions(&mut owner, &mut connection)?;
        if quit && connection.is_none() && shutdown.is_none() {
            owner.node().begin_shutdown();
            shutdown = Some(Instant::now());
        }
        if owner.node().is_drained() {
            break;
        }
        if shutdown.is_some_and(|t| t.elapsed() > Duration::from_secs(10)) {
            return Err("metadata shutdown timed out; recover before reuse".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    credentials.finish()?;
    if let Some(peers) = &mut peers {
        peers.finish()?;
    }
    setup::join(owner.take())?;
    println!("stopped node={id} workers_joined=true");
    Ok(())
}
fn main() -> Result<(), Failure> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [verb, rest @ ..] if verb == "plan" => {
            print!("{}", directory_plan::single(rest)?);
            Ok(())
        }
        [verb, rest @ ..] if verb == "serve" => serve(rest),
        [verb, rest @ ..] if verb == "client" => directory_client::command(rest),
        [verb, rest @ ..] if verb == "lookup" => directory_client::lookup(rest),
        [verb, rest @ ..] if verb == "route" => route_client::route(rest),
        [verb, rest @ ..] if verb == "split-preview" => transfer_preview::command(rest),
        [arg] if arg == "--help" => {
            println!("{HELP}");
            Ok(())
        }
        _ => Err(HELP.into()),
    }
}
