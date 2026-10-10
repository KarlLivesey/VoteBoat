// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    app::{self, App},
    connection::Connection,
    credential_reload::Credentials,
    peer_credentials::Peers,
    profile::{Binding, Profile, Role},
    service_access,
    setup::{self, checked, Failure},
};
use std::{
    fmt::Debug,
    net::TcpListener,
    path::Path,
    time::{Duration, Instant},
};
use voteboat::{
    identity::*,
    native::{
        connect::{NativePeerProtocol, NativeServiceConnector},
        node::NativeNode,
    },
    runtime::*,
    secure::LocalIdentity,
};
pub type Node<A> = NativeNode<A, NativeServiceConnector>;
pub fn serve(args: &[String]) -> Result<(), Failure> {
    let [mode, root, id, base, tls, profile, group, access, transport, rest @ ..] = args else {
        return Err(super::HELP.into());
    };
    let profile = Profile::load(Path::new(profile))?;
    let binding = profile.binding(group)?;
    let id = id.parse::<u64>()?;
    let base = base.parse::<u16>()?;
    let protocol = match transport.as_str() {
        "tcp" => NativePeerProtocol::TcpTls,
        #[cfg(feature = "quic")]
        "quic" => NativePeerProtocol::Quic,
        _ => return Err("unsupported transport".into()),
    };
    let create = match mode.as_str() {
        "create" => true,
        "recover" => false,
        _ => return Err("expected create or recover".into()),
    };
    let (peers, peer_path) = options(rest)?;
    Peers::validate_profile(Path::new(root), peer_path, true)?;
    let mut config =
        setup::configuration(Path::new(root), id, base, Path::new(tls), create, peers)?;
    config.startup.bootstrap.group = binding.group;
    let wire = config.startup.tls.wire_version();
    let peers = peer_path
        .map(|p| Peers::load(&mut config, p, wire))
        .transpose()?;
    let source_binding = super::binding::SourceBinding::new(&config, &profile, binding);
    if let Some(record) = &source_binding {
        record.check(create)?;
    }
    let access = Credentials::load(
        Path::new(root),
        Some(Path::new(access)),
        Path::new(tls),
        id,
        voteboat::secure::PeerIdentity {
            node: config.startup.node,
            store: config.startup.store,
        },
    )?;
    println!(
        "credential_digest={:02x?}",
        access
            .access
            .as_ref()
            .and_then(|a| a.material())
            .ok_or("authenticated transfer credentials unavailable")?
            .digest
    );
    let listener = TcpListener::bind(super::command_endpoints::listener(None, true, base, id)?)?;
    listener.set_nonblocking(true)?;
    match binding.role {
        Role::Metadata => run(
            open(config, protocol, app::metadata(&profile)?, peers.as_ref())?,
            listener,
            profile,
            binding,
            access,
            id,
            peers,
        ),
        Role::Source => {
            if profile.retirement {
                let app = super::retirement::source(&profile, binding)?;
                let node = open_source(
                    config,
                    protocol,
                    app,
                    source_binding,
                    create,
                    peers.as_ref(),
                )?;
                return run(node, listener, profile, binding, access, id, peers);
            }
            let node = open_source(
                config,
                protocol,
                app::source(&profile, binding)?,
                source_binding,
                create,
                peers.as_ref(),
            )?;
            run(node, listener, profile, binding, access, id, peers)
        }
        Role::Target => run(
            open(
                config,
                protocol,
                app::target(&profile, binding)?,
                peers.as_ref(),
            )?,
            listener,
            profile,
            binding,
            access,
            id,
            peers,
        ),
    }
}
fn options(args: &[String]) -> Result<(setup::PeerInput<'_>, Option<&Path>), Failure> {
    if !args.len().is_multiple_of(2) {
        return Err("expected --deployment FILE or --peer-credentials FILE".into());
    }
    let (mut deployment, mut credentials) = (None, None);
    for pair in args.as_chunks::<2>().0 {
        match pair[0].as_str() {
            "--deployment" if deployment.is_none() => deployment = Some(Path::new(&pair[1])),
            "--peer-credentials" if credentials.is_none() => {
                credentials = Some(Path::new(&pair[1]))
            }
            _ => return Err("unknown or duplicate transfer option".into()),
        }
    }
    Ok((
        deployment.map_or(setup::PeerInput::Legacy(None), setup::PeerInput::Deployment),
        credentials,
    ))
}
fn open<A: App>(
    config: voteboat::native::startup::NativeMemberStartup,
    protocol: NativePeerProtocol,
    app: A,
    peers: Option<&Peers>,
) -> Result<Node<A>, Failure> {
    match peers {
        Some(p) => setup::open_application_with_rotation(
            config,
            protocol,
            false,
            app,
            Some(p.startup(protocol)),
        ),
        None => setup::open_application(config, protocol, false, app),
    }
}
fn open_source<A: App>(
    config: voteboat::native::startup::NativeMemberStartup,
    protocol: NativePeerProtocol,
    app: A,
    record: Option<super::binding::SourceBinding>,
    create: bool,
    peers: Option<&Peers>,
) -> Result<Node<A>, Failure> {
    let node = open(config, protocol, app, peers)?;
    if let Some(record) = record {
        if let Err(error) = record.finish(create) {
            recover(node, Instant::now())?;
            return Err(error);
        }
    }
    Ok(node)
}
fn run<A: App>(
    mut node: Node<A>,
    listener: TcpListener,
    p: Profile,
    b: Binding,
    access: Credentials,
    id: u64,
    peers: Option<Peers>,
) -> Result<(), Failure>
where
    A::Receipt: Debug,
{
    // Join accepted credential publication before releasing the node's exclusive
    // directory ownership, including when drive or connection cleanup fails.
    let mut credentials = access;
    let mut peers = peers;
    let local = service_access::server_local(id, node.local().owner.identity().store.session);
    println!(
        "ready transfer group={} node={id} role={:?} command={}",
        b.group.id.get(),
        b.role,
        listener.local_addr()?
    );
    let start = Instant::now();
    let mut host = Host {
        node: &mut node,
        listener: &listener,
        p: &p,
        b,
        credentials: &mut credentials,
        peers: &mut peers,
        local,
        start,
        sequence: 0,
        connection: None,
        quit: false,
        shutdown: None,
    };
    let result = host.drive();
    if let Some(mut c) = host.connection.take() {
        let _ = c.cancel(host.node);
    }
    let finished = credentials.finish();
    let peer_finished = peers.as_mut().map_or(Ok(()), Peers::finish);
    let result = result.and(finished).and(peer_finished);
    if result.is_err() {
        recover(node, start)?;
    } else {
        setup::join(node)?;
    }
    result?;
    println!("stopped group={} workers_joined=true", b.group.id.get());
    Ok(())
}
struct Host<'a, A: App> {
    node: &'a mut Node<A>,
    listener: &'a TcpListener,
    p: &'a Profile,
    b: Binding,
    credentials: &'a mut Credentials,
    peers: &'a mut Option<Peers>,
    local: LocalIdentity,
    start: Instant,
    sequence: u64,
    connection: Option<Connection>,
    quit: bool,
    shutdown: Option<Instant>,
}
impl<A: App> Host<'_, A> {
    fn drive(&mut self) -> Result<(), Failure> {
        loop {
            let now = MonoTime(self.start.elapsed().as_millis().min(u64::MAX as u128) as u64);
            let peer_failure = !self.quit && self.peers.as_mut().is_some_and(|p| p.poll(self.node));
            if self.credentials.poll() || peer_failure {
                eprintln!("credential reload has uncertain durable state; stopping");
                self.quit = true;
            }
            if !self.quit && self.connection.is_none() {
                match self.listener.accept() {
                    Ok((stream, _)) => {
                        self.sequence = self
                            .sequence
                            .checked_add(1)
                            .ok_or("session sequence exhausted")?;
                        self.connection = Some(Connection::new(
                            stream,
                            SecureSessionGeneration::new(self.sequence).unwrap(),
                        )?);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => (),
                    Err(e) => return Err(e.into()),
                }
            }
            if let Some(c) = self.connection.as_mut() {
                let (access, mut commands) = self.credentials.split();
                let context = super::connection::Context {
                    profile: self.p,
                    binding: self.b,
                    access: access.ok_or("authenticated transfer credentials unavailable")?,
                    local: self.local,
                    now,
                };
                if c.poll(
                    self.node,
                    &context,
                    &mut self.quit,
                    &mut commands,
                    self.peers,
                )
                .unwrap_or(true)
                {
                    c.cancel(self.node)?;
                    self.connection = None;
                }
            }
            checked(self.node.poll(now, NodePollBudget::default()))?;
            Connection::completions(self.node, self.p, self.b, self.connection.as_mut())?;
            if self.quit && self.connection.is_none() && self.shutdown.is_none() {
                self.node.begin_shutdown();
                self.shutdown = Some(Instant::now());
            }
            if self.node.is_drained() {
                return Ok(());
            }
            if self
                .shutdown
                .is_some_and(|s| s.elapsed() > Duration::from_secs(10))
            {
                return Err("shutdown timeout; recovery required".into());
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
}
fn recover<A: App>(mut node: Node<A>, start: Instant) -> Result<(), Failure> {
    node.abort();
    let mut recovery = node.into_recovery().map_err(|_| "missing recovery owner")?;
    let mut peers = recovery.peers.take().ok_or("missing peer recovery")?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !peers.is_drained() {
        checked(peers.drain(
            &mut recovery.local.outbound,
            MonoTime(start.elapsed().as_millis().min(u64::MAX as u128) as u64),
            PeerDriverBudget::default(),
        ))?;
        if Instant::now() >= deadline {
            return Err("peer recovery timeout".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let mut dialer = peers
        .into_parts()
        .map_err(|_| "peers not drained")?
        .connector
        .into_dialer()
        .map_err(|_| "dialer not drained")?;
    let mut snapshots = recovery
        .local
        .snapshots
        .take()
        .ok_or("missing snapshot recovery")?;
    let (mut log_done, mut snapshot_done) = (false, false);
    loop {
        let dial_done = match &mut dialer {
            Some(d) => checked(d.try_finish())?,
            None => true,
        };
        if !log_done {
            log_done = recovery.local.persistence.try_reclaim()?.is_some();
        }
        if !snapshot_done {
            snapshot_done = snapshots.worker.try_reclaim()?.is_some();
        }
        if log_done && snapshot_done && dial_done {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("worker recovery timeout".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
