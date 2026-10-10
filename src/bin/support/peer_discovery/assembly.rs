// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use crate::{counter_application::Application, group_setup, setup::Service};
use std::time::{Duration, Instant};
use voteboat::{
    connect::*,
    native::{connect::*, node::*, startup::*},
    runtime::*,
    snapshot_worker::SnapshotWorker,
    worker::PersistenceWorker,
};

pub(crate) fn open(
    prepared: group_setup::Prepared,
    protocol: NativePeerProtocol,
    member: bool,
    maintenance: bool,
    rotation: Option<NativePeerRotationStartup>,
    source: Option<Prepared>,
) -> Result<Service, Failure> {
    let Some(source) = source else {
        return prepared.open(protocol, member, maintenance, rotation);
    };
    let timers = TimerConfig::default();
    let wake = Arc::new(ThreadWake::current());
    let (parts, limits) = match prepared {
        group_setup::Prepared::Single(mut config) => {
            if maintenance || member {
                let wire = if maintenance {
                    8
                } else {
                    config.startup.tls.wire_version().max(7)
                };
                config.startup.tls = checked(config.startup.tls.with_wire_version(wire))?;
            }
            let limits = config.startup.limits;
            let app = Application::new(crate::setup::application()?, maintenance)?;
            let result = if member {
                match rotation {
                    Some(r) => config.prepare_discovery_with_peer_rotation(
                        r,
                        timers,
                        app,
                        wake,
                        MonoTime(0),
                    ),
                    None => config.prepare_for_discovery(protocol, timers, app, wake, MonoTime(0)),
                }
            } else {
                match rotation {
                    Some(r) => config.startup.prepare_discovery_with_peer_rotation(
                        r,
                        timers,
                        app,
                        wake,
                        MonoTime(0),
                    ),
                    None => config.startup.prepare_for_discovery(
                        protocol,
                        timers,
                        app,
                        wake,
                        MonoTime(0),
                    ),
                }
            };
            (prepared_result(result)?, limits)
        }
        group_setup::Prepared::Multi(config, applications) => {
            let limits = config.startup.limits;
            let result = match rotation {
                Some(r) => config.prepare_discovery_with_peer_rotation(
                    r,
                    timers,
                    applications,
                    wake,
                    MonoTime(0),
                ),
                None => {
                    config.prepare_for_discovery(protocol, timers, applications, wake, MonoTime(0))
                }
            };
            (prepared_result(result)?, limits)
        }
    };
    attach(parts, limits, source)
}
fn prepared_result<A, T>(result: Result<T, Box<NativeStartupRejected<A>>>) -> Result<T, Failure> {
    match result {
        Ok(parts) => Ok(parts),
        Err(mut rejected) => {
            let until = Instant::now() + Duration::from_secs(10);
            while !rejected.try_cleanup()? {
                if Instant::now() >= until {
                    return Err("discovery preparation cleanup timed out".into());
                }
                std::thread::park_timeout(Duration::from_millis(1));
            }
            Err(rejected.reason.into())
        }
    }
}
pub(crate) fn attach(
    mut parts: NativeNodeParts<Application, NativeServiceConnector>,
    limits: NodeLimits,
    source: Prepared,
) -> Result<Service, Failure> {
    let source = match source.start(parts.local.owner.identity().store.session) {
        Ok(source) => source,
        Err(error) => {
            cleanup(parts)?;
            return Err(error);
        }
    };
    let PeerParts {
        connector,
        roster,
        factory,
        ingress,
        routes,
        admission_routes,
    } = parts.peers.take().ok_or("missing native peer parts")?;
    let connector = match connector.with_discovery(Box::new(source), MonoTime(0)) {
        Ok(connector) => connector,
        Err((reason, connector, mut returned)) => {
            returned.close();
            let start = Instant::now();
            while returned.discovery_pending() {
                returned
                    .poll_discovery(
                        MonoTime(start.elapsed().as_millis() as u64),
                        SessionPollBudget::default(),
                    )
                    .map_err(|e| format!("{e:?}"))?;
                if start.elapsed() > Duration::from_secs(10) {
                    return Err("source cleanup timed out".into());
                }
                std::thread::park_timeout(Duration::from_millis(1));
            }
            parts.peers = Some(PeerParts {
                connector,
                roster,
                factory,
                ingress,
                routes,
                admission_routes,
            });
            cleanup(parts)?;
            return Err(format!("discovery connector: {reason:?}").into());
        }
    };
    parts.peers = Some(PeerParts {
        connector,
        roster,
        factory,
        ingress,
        routes,
        admission_routes,
    });
    match NativeNode::from_parts(parts, limits, MonoTime(0)) {
        Ok(node) => Ok(node),
        Err(rejected) => {
            let error = format!("discovery assembly: {:?}", rejected.reason);
            cleanup(*rejected.parts)?;
            Err(error.into())
        }
    }
}
fn cleanup(mut parts: NativeNodeParts<Application, NativeServiceConnector>) -> Result<(), Failure> {
    parts
        .local
        .owner
        .close_admission()
        .map_err(|e| format!("{e:?}"))?;
    parts.local.persistence.close();
    let mut snapshots = parts.local.snapshots.take().ok_or("missing snapshots")?;
    snapshots.worker.close();
    let mut connector = parts.peers.take().ok_or("missing peers")?.connector;
    connector.close();
    let start = Instant::now();
    let mut log_done = false;
    let mut snapshot_done = false;
    while !connector.is_drained() || !log_done || !snapshot_done {
        for event in connector
            .poll(
                MonoTime(start.elapsed().as_millis() as u64),
                ConnectPollBudget::default(),
            )
            .map_err(|e| format!("{e:?}"))?
        {
            if let Ok(mut session) = event.result {
                session.close();
            }
        }
        if !log_done {
            log_done = parts.local.persistence.try_reclaim()?.is_some();
        }
        if !snapshot_done {
            snapshot_done = snapshots.worker.try_reclaim()?.is_some();
        }
        if start.elapsed() > Duration::from_secs(10) {
            return Err("prepared cleanup timed out".into());
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    if let Some(mut dialer) = connector
        .into_dialer()
        .map_err(|_| "connector still owned")?
    {
        while !checked(dialer.try_finish())? {
            if start.elapsed() > Duration::from_secs(10) {
                return Err("dialer cleanup timed out".into());
            }
            std::thread::park_timeout(Duration::from_millis(1));
        }
    }
    Ok(())
}
