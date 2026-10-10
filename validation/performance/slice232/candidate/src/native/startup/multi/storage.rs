// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

pub(super) fn recover<A: CheckpointStateMachine>(
    config: &NativeStartup,
    groups: &[Bootstrap],
    authorization: &StartupAuthorization,
    prepared: &mut PreparedStartup<A>,
) -> Result<(), NativeStartupError> {
    let create = config.mode == NativeOpenMode::Create;
    let store = if create {
        let mut store = NativeLogStore::create(
            FileLogIo::create_with_manifest_journal(&config.directory)?,
            config.store,
            LogLimits::default(),
        )?;
        let tickets =
            store.append_batch(groups.iter().cloned().map(LogMutation::Create).collect())?;
        store.barrier(&tickets)?;
        store
    } else {
        NativeLogStore::recover(
            FileLogIo::open(&config.directory)?,
            config.store,
            LogLimits::default(),
        )?
    };
    let identities = groups.iter().map(|g| g.group).collect::<Vec<_>>();
    if !store.matches_startup_groups(&identities)? {
        return Err(error(
            "recovery",
            "durable group inventory differs from selected startup",
        ));
    }
    for bootstrap in groups {
        if store.state(bootstrap.group)?.bootstrap != *bootstrap {
            return Err(error(
                "recovery",
                "original bootstrap differs from selected startup",
            ));
        }
        let mut snapshots = snapshot(config, bootstrap.group, create)?;
        let core = checked(recover_member_replica(
            config.node,
            bootstrap.group,
            &store,
            &mut snapshots,
            prepared.applications.get_mut(&bootstrap.group).unwrap(),
        ))?
        .0;
        prepared.cores.push(super::super::configured_core(
            core,
            config.tls.wire_version(),
            authorization,
        ));
        prepared.snapshots.insert(bootstrap.group, snapshots);
    }
    prepared.store = Some(store);
    Ok(())
}
fn snapshot(
    config: &NativeStartup,
    group: GroupIdentity,
    create: bool,
) -> Result<NativeSnapshotStore<FileSnapshotIo>, NativeStartupError> {
    let name = if group == config.bootstrap.group {
        "snapshots".to_owned()
    } else {
        format!("snapshots-{}-{}", group.id.get(), group.incarnation.get())
    };
    let path = config.directory.join(name);
    let identity = SnapshotIdentity {
        store: config.store,
        group,
    };
    if create {
        Ok(NativeSnapshotStore::create(
            FileSnapshotIo::create(path)?,
            identity,
            SnapshotLimits::default(),
        )?)
    } else {
        Ok(NativeSnapshotStore::recover(
            FileSnapshotIo::open(path)?,
            identity,
            SnapshotLimits::default(),
        )?)
    }
}
