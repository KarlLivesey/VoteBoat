// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    counter_application::Application,
    setup::{self, checked, Failure, Service},
};
use std::{
    collections::BTreeMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use voteboat::{
    identity::*,
    log::Bootstrap,
    native::{connect::NativePeerProtocol, startup::*, worker::ThreadWake},
    quorum::{Limits, Policy},
    runtime::{MonoTime, TimerConfig},
};

pub enum Prepared {
    Single(Box<NativeMemberStartup>),
    Multi(
        Box<NativeMultiStartup>,
        BTreeMap<GroupIdentity, Application>,
    ),
}
pub fn prepare(
    mut config: NativeMemberStartup,
    path: Option<&Path>,
    maintenance: bool,
) -> Result<Prepared, Failure> {
    let Some(path) = path else {
        return Ok(Prepared::Single(Box::new(config)));
    };
    let data = super::service_setup::material(path, 65536)?;
    let mut lines = std::str::from_utf8(&data)?.lines();
    if lines.next() != Some("voteboat-counter-groups-v1") {
        return Err("expected voteboat-counter-groups-v1 header".into());
    }
    let mut groups = Vec::new();
    for line in lines {
        if groups.len() == MAX_NATIVE_STARTUP_GROUPS {
            return Err("group manifest exceeds 256 groups".into());
        }
        groups.push(bootstrap(line, &config.provisioned_stores)?);
    }
    if groups.is_empty() {
        return Err("group manifest must not be empty".into());
    }
    let applications = groups
        .iter()
        .map(|b| {
            Ok((
                b.group,
                Application::for_group(b.group, setup::application()?, maintenance)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, Failure>>()?;
    config.startup.bootstrap = groups.remove(0);
    config.startup.tls = checked(config.startup.tls.with_wire_version(8))?;
    let config = NativeMultiStartup {
        startup: config.startup,
        additional_groups: groups,
        provisioned_stores: config.provisioned_stores,
    };
    config.validate(&applications)?;
    Ok(Prepared::Multi(Box::new(config), applications))
}
fn bootstrap(line: &str, stores: &BTreeMap<NodeId, StoreIdentity>) -> Result<Bootstrap, Failure> {
    let mut words = line.split_whitespace();
    if words.next() != Some("group") {
        return Err("expected group ID INCARNATION CONFIGURATION POLICY".into());
    }
    let group = GroupIdentity {
        id: GroupId::new(words.next().ok_or("missing group")?.parse()?).ok_or("invalid group")?,
        incarnation: GroupIncarnation::new(words.next().ok_or("missing incarnation")?.parse()?)
            .ok_or("invalid incarnation")?,
    };
    let configuration = ConfigurationId::new(words.next().ok_or("missing configuration")?.parse()?)
        .ok_or("invalid configuration")?;
    let policy = checked(Policy::new(
        super::policy_input::tree(&mut words, 0, &mut 16384, setup::MAX_NODE)?,
        Limits::default(),
    ))?;
    if words.next().is_some() {
        return Err("trailing group fields".into());
    }
    let voter_stores = policy
        .voters()
        .iter()
        .map(|id| {
            stores
                .get(id)
                .map(|s| (*id, *s))
                .ok_or_else(|| "group voter missing from deployment".into())
        })
        .collect::<Result<_, Failure>>()?;
    Ok(Bootstrap {
        group,
        configuration,
        policy,
        voter_stores,
    })
}
impl Prepared {
    pub fn group_count(&self) -> usize {
        match self {
            Self::Single(_) => 1,
            Self::Multi(_, applications) => applications.len(),
        }
    }
    pub fn contains(&self, group: GroupIdentity) -> bool {
        match self {
            Self::Single(config) => config.startup.bootstrap.group == group,
            Self::Multi(_, applications) => applications.contains_key(&group),
        }
    }
    pub fn open(
        self,
        protocol: NativePeerProtocol,
        member: bool,
        maintenance: bool,
        rotation: Option<NativePeerRotationStartup>,
    ) -> Result<Service, Failure> {
        let (config, applications) = match self {
            Self::Single(config) => {
                return setup::open(*config, protocol, member, maintenance, rotation)
            }
            Self::Multi(config, applications) => (config, applications),
        };
        let wake = Arc::new(ThreadWake::current());
        let opened = if let Some(rotation) = rotation {
            config.open_with_peer_rotation(
                rotation,
                TimerConfig::default(),
                applications,
                wake,
                MonoTime(0),
            )
        } else {
            config.open(
                protocol,
                TimerConfig::default(),
                applications,
                wake,
                MonoTime(0),
            )
        };
        match opened {
            Ok(service) => Ok(service),
            Err(mut rejected) => {
                let deadline = Instant::now() + Duration::from_secs(10);
                while !rejected.try_cleanup()? {
                    if Instant::now() >= deadline {
                        return Err("startup cleanup timed out; recover before reuse".into());
                    }
                    std::thread::park_timeout(Duration::from_millis(1));
                }
                Err(rejected.reason.into())
            }
        }
    }
}
