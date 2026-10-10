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
#[path = "support/command_client.rs"]
mod command_client;
#[path = "support/command_endpoints.rs"]
mod command_endpoints;
#[path = "support/directory_client.rs"]
mod directory_client;
#[path = "support/directory_connection.rs"]
mod directory_connection;
#[path = "support/directory_owner.rs"]
mod directory_owner;
#[path = "support/directory_plan.rs"]
mod directory_plan;
#[path = "support/service_access.rs"]
mod service_access;
#[path = "support/service_setup.rs"]
mod setup;
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
const HELP: &str = "voteboat-directory plan AUTHORITY INCARNATION RESPONSIBILITY INCARNATION EXECUTION_GROUP INCARNATION\nvoteboat-directory serve create|recover DIRECTORY NODE BASE TLS PLAN ACCESS [--command-listen ADDRESS] [--peers FILE | --deployment FILE] [--transport tcp|quic]\nvoteboat-directory client BASE NODE TLS PRINCIPAL status|initialize|publish OPERATION|checkpoint|quit [--command-peers FILE]\nvoteboat-directory lookup BASE NODE TLS PRINCIPAL GROUP INCARNATION RESPONSIBILITY INCARNATION [--command-peers FILE]";
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
    protocol: NativePeerProtocol,
}
fn options(args: &[String]) -> Result<Options, Failure> {
    let mut o = Options {
        listen: None,
        peers: None,
        deployment: None,
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
    let access = service_access::Access::load(Path::new(access), Path::new(tls), id)?;
    let digest = access.digest;
    let access = service_access::ActiveAccess::new(access.policy.generation(), access);
    let address = command_endpoints::listener(options.listen, true, base, id)?;
    let listener = TcpListener::bind(address)?;
    listener.set_nonblocking(true)?;
    let node = setup::open_application(config, options.protocol, false, plan.app)?;
    let local = service_access::server_local(id, node.local().owner.identity().store.session);
    let mut owner = directory_owner::Owner::new(node, plan.authority);
    let start = Instant::now();
    let mut connection = None;
    let mut sequence = 0u64;
    let mut quit = false;
    let mut shutdown = None;
    println!(
        "ready directory node={id} authority={:?} command={} credential_digest={digest:02x?}",
        plan.authority,
        listener.local_addr()?
    );
    loop {
        let now = MonoTime(start.elapsed().as_millis().min(u64::MAX as u128) as u64);
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
            let mut commands = directory_connection::Commands {
                group: plan.authority,
                bootstrap: plan.bootstrap,
                records: &plan.commands,
                quit: &mut quit,
            };
            if c.poll(&mut owner, &access, local, now, &mut commands)? {
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
        [arg] if arg == "--help" => {
            println!("{HELP}");
            Ok(())
        }
        _ => Err(HELP.into()),
    }
}
