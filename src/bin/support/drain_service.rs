// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    counter_application::Receipt,
    drain_commands, group_drain,
    leadership_set::Leaders,
    setup::{group, Failure, Service},
    Connection, Phase,
};
use std::path::Path;
use voteboat::{drain::MembershipDrainPlan, runtime::*};

pub enum Driver {
    Single(Box<drain_commands::Driver>),
    Multi(Box<group_drain::Driver>),
}
pub fn is_command(word: Option<&str>) -> bool {
    word == Some("drain-group") || drain_commands::is_command(word)
}
impl Driver {
    pub fn open(
        service: &mut Service,
        root: &Path,
        create: bool,
        plan: Option<MembershipDrainPlan>,
        multi: bool,
    ) -> Result<Self, Failure> {
        if multi {
            group_drain::Driver::open(service, root, create, plan).map(|d| Self::Multi(Box::new(d)))
        } else {
            drain_commands::Driver::open(service, root, create, plan)
                .map(|d| Self::Single(Box::new(d)))
        }
    }
    pub fn multi(&self) -> bool {
        matches!(self, Self::Multi(_))
    }
    pub fn scopes(&self, service: &Service) -> Vec<voteboat::identity::GroupIdentity> {
        if self.multi() {
            service.local().applications.keys().copied().collect()
        } else {
            Vec::new()
        }
    }
    pub fn busy(&self) -> bool {
        match self {
            Self::Single(d) => d.busy(),
            Self::Multi(d) => d.busy(),
        }
    }
    pub fn finish(&mut self) -> Result<(), Failure> {
        match self {
            Self::Single(d) => d.finish(),
            Self::Multi(d) => d.finish(),
        }
    }
    pub fn complete(&mut self, ticket: ClientTicket, outcome: &ClientOutcome<Receipt>) {
        if let Self::Single(d) = self {
            d.complete(ticket, outcome);
        }
    }
    pub fn tick(
        &mut self,
        service: &mut Service,
        leaders: &mut Leaders,
        connection: &mut Option<Connection>,
    ) -> Result<(), Failure> {
        match self {
            Self::Single(d) => d.tick(service, leaders.get_mut(group())?, connection),
            Self::Multi(d) => d.tick(service, connection),
        }
    }
    pub fn command(
        &mut self,
        service: &mut Service,
        leaders: &mut Leaders,
        words: &[&str],
        quit: &mut bool,
    ) -> Result<Phase, String> {
        match self {
            Self::Single(d) => d.command(service, leaders.get_mut(group())?, words, quit),
            Self::Multi(d) => d.command(service, words, quit),
        }
    }
}
pub fn authorize(
    channel: &super::service_access::Channel,
    access: Option<&super::service_access::ActiveAccess>,
    command: &super::group_command::Command<'_>,
    scopes: &[voteboat::identity::GroupIdentity],
    time: MonoTime,
) -> Result<(), String> {
    channel.authorize(access, command.group, command.text, time)?;
    if is_command(command.text.split_whitespace().next()) {
        for scope in scopes {
            channel.authorize(access, *scope, command.text, time)?;
        }
    }
    Ok(())
}
