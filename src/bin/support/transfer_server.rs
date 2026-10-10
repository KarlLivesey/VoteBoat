// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    app::{self, App},
    connection::Connection,
    profile::{Binding, Profile, Role},
    service_access::{self, Access, ActiveAccess},
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
    let peers = match rest {
        [] => setup::PeerInput::Legacy(None),
        [flag, path] if flag == "--deployment" => setup::PeerInput::Deployment(Path::new(path)),
        _ => return Err("expected optional --deployment FILE".into()),
    };
    let mut config =
        setup::configuration(Path::new(root), id, base, Path::new(tls), create, peers)?;
    config.startup.bootstrap.group = binding.group;
    let source_binding = super::binding::SourceBinding::new(&config, &profile, binding);
    if let Some(record) = &source_binding {
        record.check(create)?;
    }
    let access = Access::load(Path::new(access), Path::new(tls), id)?;
    println!("credential_digest={:02x?}", access.digest);
    let access = ActiveAccess::new(access.policy.generation(), access);
    let listener = TcpListener::bind(super::command_endpoints::listener(None, true, base, id)?)?;
    listener.set_nonblocking(true)?;
    match binding.role {
        Role::Metadata => {
            let app = app::metadata(&profile)?;
            run(
                setup::open_application(config, protocol, false, app)?,
                listener,
                profile,
                binding,
                access,
                id,
            )
        }
        Role::Source => {
            if profile.retirement {
                let app = super::retirement::source(&profile, binding)?;
                let node = open_source(config, protocol, app, source_binding, create)?;
                return run(node, listener, profile, binding, access, id);
            }
            let app = app::source(&profile, binding)?;
            let node = open_source(config, protocol, app, source_binding, create)?;
            run(node, listener, profile, binding, access, id)
        }
        Role::Target => {
            let app = app::target(&profile, binding)?;
            run(
                setup::open_application(config, protocol, false, app)?,
                listener,
                profile,
                binding,
                access,
                id,
            )
        }
    }
}
fn open_source<A: App>(
    config: voteboat::native::startup::NativeMemberStartup,
    protocol: NativePeerProtocol,
    app: A,
    record: Option<super::binding::SourceBinding>,
    create: bool,
) -> Result<Node<A>, Failure> {
    let node = setup::open_application(config, protocol, false, app)?;
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
    access: ActiveAccess,
    id: u64,
) -> Result<(), Failure>
where
    A::Receipt: Debug,
{
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
        access: &access,
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
    access: &'a ActiveAccess,
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
                let context = super::connection::Context {
                    profile: self.p,
                    binding: self.b,
                    access: self.access,
                    local: self.local,
                    now,
                };
                if c.poll(self.node, &context, &mut self.quit).unwrap_or(true) {
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
