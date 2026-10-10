// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::setup::{group, Service};
use voteboat::identity::*;

pub struct Command<'a> {
    pub group: GroupIdentity,
    pub text: &'a str,
    scoped: bool,
}
fn word(input: &str) -> Result<(&str, &str), String> {
    let input = input.trim_start();
    let end = input.find(char::is_whitespace).unwrap_or(input.len());
    if end == 0 {
        return Err("missing group command field".into());
    }
    Ok((&input[..end], input[end..].trim_start()))
}
pub fn parse(input: &str) -> Result<Command<'_>, String> {
    let (first, rest) = word(input)?;
    if first != "group" {
        return Ok(Command {
            group: group(),
            text: input,
            scoped: false,
        });
    }
    let (id, rest) = word(rest)?;
    let (incarnation, text) = word(rest)?;
    let group = GroupIdentity {
        id: id
            .parse()
            .ok()
            .and_then(GroupId::new)
            .ok_or("invalid group ID")?,
        incarnation: incarnation
            .parse()
            .ok()
            .and_then(GroupIncarnation::new)
            .ok_or("invalid group incarnation")?,
    };
    if !matches!(
        word(text)?.0,
        "status"
            | "read"
            | "add"
            | "checkpoint"
            | "configure"
            | "configuration-status"
            | "move-leader"
            | "leadership-status"
            | "resume-leadership"
            | "cancel-leadership"
    ) {
        return Err("unsupported group command".into());
    }
    Ok(Command {
        group,
        text,
        scoped: true,
    })
}
pub fn resolve<'a>(service: &Service, text: &'a str) -> Result<Command<'a>, String> {
    let selected = parse(text)?;
    if selected.scoped && service.local().owner.core(selected.group).is_none() {
        return Err("unknown group or incarnation".into());
    }
    Ok(selected)
}
pub fn require_leader(service: &Service, group: GroupIdentity) -> Result<(), String> {
    if service
        .local()
        .owner
        .core(group)
        .is_none_or(|core| core.role() != voteboat::raft::Role::Leader)
    {
        return Err("NOT_LEADER".into());
    }
    Ok(())
}
pub fn payload(words: &[String]) -> &[String] {
    if words.first().is_some_and(|w| w == "group") {
        words.get(3..).unwrap_or_default()
    } else {
        words
    }
}
