// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    counter_application::Receipt,
    leadership_commands::Driver,
    setup::{Failure, Service},
    Connection,
};
use std::collections::BTreeMap;
use voteboat::{identity::GroupIdentity, runtime::*};

pub struct Leaders {
    groups: BTreeMap<GroupIdentity, Driver>,
}
impl Leaders {
    pub fn new(service: &Service) -> Self {
        Self {
            groups: service
                .local()
                .applications
                .keys()
                .map(|group| (*group, Driver::new(*group)))
                .collect(),
        }
    }
    pub fn get_mut(&mut self, group: GroupIdentity) -> Result<&mut Driver, String> {
        self.groups
            .get_mut(&group)
            .ok_or_else(|| "unknown leadership group".into())
    }
    pub fn complete(&mut self, ticket: ClientTicket, outcome: &ClientOutcome<Receipt>) {
        if let Some(driver) = self.groups.get_mut(&ticket.group) {
            driver.complete(ticket, outcome);
        }
    }
    pub fn advance_cancel(&mut self, service: &mut Service, connection: &mut Option<Connection>) {
        for driver in self.groups.values_mut() {
            driver.advance_cancel(service, connection);
        }
    }
    pub fn tick(&mut self, service: &mut Service, quit: bool) -> Result<(), Failure> {
        for driver in self.groups.values_mut() {
            driver.tick(service, quit)?;
        }
        Ok(())
    }
}
