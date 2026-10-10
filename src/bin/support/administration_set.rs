// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    administration::{Administration, Mode},
    setup::{Failure, Service},
};
use std::{collections::BTreeMap, path::Path};
use voteboat::{identity::*, native::startup::MAX_NATIVE_STARTUP_GROUPS};

pub struct Administrations {
    groups: BTreeMap<GroupIdentity, Administration>,
}
impl Administrations {
    pub fn single(plan: Administration) -> Self {
        Self {
            groups: [(plan.group(), plan)].into(),
        }
    }
    pub fn load(path: &Path, stores: &BTreeMap<NodeId, StoreIdentity>) -> Result<Self, Failure> {
        let data = super::service_setup::material(path, 65536)?;
        let mut lines = std::str::from_utf8(&data)?.lines();
        if lines.next() != Some("voteboat-counter-group-admin-v1") {
            return Err("expected voteboat-counter-group-admin-v1".into());
        }
        let mut groups = BTreeMap::new();
        let mut loaded = data.len();
        let mut retained = 0usize;
        for line in lines {
            if groups.len() == MAX_NATIVE_STARTUP_GROUPS {
                return Err("group administration exceeds 256 groups".into());
            }
            let words = line.split_whitespace().collect::<Vec<_>>();
            let ["group", id, incarnation, file] = words.as_slice() else {
                return Err("expected group ID INCARNATION PLAN_FILE".into());
            };
            let group = GroupIdentity {
                id: GroupId::new(id.parse()?).ok_or("invalid plan group")?,
                incarnation: GroupIncarnation::new(incarnation.parse()?)
                    .ok_or("invalid plan incarnation")?,
            };
            if groups
                .last_key_value()
                .is_some_and(|(previous, _)| previous >= &group)
            {
                return Err("group administration must be sorted and unique".into());
            }
            let file = path.parent().unwrap_or(Path::new(".")).join(file);
            let bytes = super::service_setup::material(&file, 65536)?;
            loaded = loaded
                .checked_add(bytes.len())
                .filter(|n| *n <= 1024 * 1024)
                .ok_or("aggregate plan file byte limit")?;
            let plan = Administration::from_data(group, &bytes, stores, Mode::Provisioned, false)?;
            retained = retained
                .checked_add(plan.retained_bytes())
                .filter(|n| *n <= 4 * 1024 * 1024)
                .ok_or("aggregate retained plan limit")?;
            groups.insert(group, plan);
        }
        if groups.is_empty() {
            return Err("empty group administration manifest".into());
        }
        Ok(Self { groups })
    }
    pub fn groups(&self) -> impl Iterator<Item = GroupIdentity> + '_ {
        self.groups.keys().copied()
    }
    pub fn get(&self, group: GroupIdentity) -> Result<&Administration, String> {
        self.groups
            .get(&group)
            .ok_or_else(|| "administration disabled for this group".into())
    }
    pub fn get_mut(&mut self, group: GroupIdentity) -> Result<&mut Administration, String> {
        self.groups
            .get_mut(&group)
            .ok_or_else(|| "administration disabled for this group".into())
    }
    pub fn request(
        &mut self,
        service: &Service,
        group: GroupIdentity,
        operation: &str,
    ) -> Result<OperationId, String> {
        let operation = operation
            .parse::<u128>()
            .ok()
            .and_then(OperationId::new)
            .ok_or("invalid operation ID")?;
        super::group_command::require_leader(service, group)?;
        self.get_mut(group)?.request(operation)?;
        Ok(operation)
    }
    pub fn tick(&mut self, service: &mut Service, quit: bool) -> Result<(), Failure> {
        while let Some(result) = service.poll_configuration() {
            self.get_mut(result.ticket.admission().group)?
                .complete(result)?;
        }
        for plan in self.groups.values_mut() {
            plan.tick(service, quit)?;
        }
        Ok(())
    }
    pub fn take_reply(&mut self) -> Option<(GroupIdentity, OperationId, String)> {
        self.groups.iter_mut().find_map(|(group, plan)| {
            plan.take_reply()
                .map(|(operation, reply)| (*group, operation, reply))
        })
    }
}
