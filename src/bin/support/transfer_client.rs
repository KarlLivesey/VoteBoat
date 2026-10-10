// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{
    command_client::{self, Attempt},
    command_endpoints::Endpoint,
    profile::Profile,
    service_access::ClientAccess,
    setup::{checked, Failure},
    wire,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use voteboat::{identity::*, transfer::*};
struct Client {
    profile: Profile,
    endpoints: BTreeMap<u128, Vec<Endpoint>>,
    tls: PathBuf,
    principal: u64,
}
impl Client {
    fn load(profile: &str, endpoints: &str, tls: &str, principal: &str) -> Result<Self, Failure> {
        let profile = Profile::load(Path::new(profile))?;
        let text = wire::file(Path::new(endpoints), 64 * 1024)?;
        let mut lines = text.lines();
        if lines.next() != Some("voteboat-transfer-endpoints-v1") {
            return Err("invalid transfer endpoints".into());
        }
        let mut endpoints: BTreeMap<u128, Vec<Endpoint>> = BTreeMap::new();
        let mut count = 0;
        for line in lines {
            count += 1;
            if count > 18 * 64 {
                return Err("endpoint budget".into());
            }
            let words = line.split_whitespace().collect::<Vec<_>>();
            if let [group, incarnation, path] = words.as_slice() {
                let b = profile.binding(group)?;
                if b.group.incarnation.get() != incarnation.parse()?
                    || endpoints.contains_key(&b.group.id.get())
                {
                    return Err("conflicting group endpoints".into());
                }
                endpoints.insert(
                    b.group.id.get(),
                    super::command_endpoints::targets(0, None, Some(Path::new(path)))?,
                );
                continue;
            }
            let [group, incarnation, node, address] = words.as_slice() else {
                return Err("expected GROUP INC NODE ADDRESS".into());
            };
            let b = profile.binding(group)?;
            if b.group.incarnation.get() != incarnation.parse()? {
                return Err("wrong group incarnation".into());
            }
            let node = node.parse::<u64>()?;
            if !(1..=super::setup::MAX_NODE).contains(&node) {
                return Err("invalid endpoint node".into());
            }
            let entries = endpoints.entry(b.group.id.get()).or_default();
            if entries.len() == 64 || entries.iter().any(|e| e.node == node) {
                return Err("duplicate or excess node".into());
            }
            entries.push(Endpoint {
                node,
                address: address.parse()?,
                server_name: format!("node{node}.voteboat.test"),
            });
        }
        if profile.groups.keys().any(|g| !endpoints.contains_key(g)) {
            return Err("missing group endpoints".into());
        }
        Ok(Self {
            profile,
            endpoints,
            tls: PathBuf::from(tls),
            principal: principal.parse()?,
        })
    }
    fn ask(&self, group: GroupIdentity, text: &str, read: bool) -> Result<String, Failure> {
        if text.len() + 1 > wire::COMMAND_BYTES {
            return Err("command budget".into());
        }
        let targets = &self.endpoints[&group.id.get()];
        let access = ClientAccess::load(&self.tls, self.principal, targets)?;
        let start = Instant::now();
        let deadline = start + Duration::from_secs(12);
        let text = format!("{text}\n");
        loop {
            for target in targets {
                let mut channel =
                    match command_client::connect(target, deadline, Some(&access), start) {
                        Ok(c) => c,
                        Err(_) => continue,
                    };
                let attempt =
                    if text.starts_with("transfer-read ") || text.starts_with("transfer-export ") {
                        command_client::request_bounded(
                            &mut channel,
                            text.as_bytes(),
                            deadline,
                            start,
                            wire::COMMAND_BYTES,
                        )
                    } else {
                        command_client::request(&mut channel, text.as_bytes(), deadline, start)
                    };
                match attempt {
                    Attempt::Reply(reply) if reply.starts_with("OK ") => {
                        return Ok(reply.trim_end().to_owned())
                    }
                    Attempt::Reply(reply) if reply.contains("AUTHORIZATION") => {
                        return Err(reply.into())
                    }
                    Attempt::Reply(reply)
                        if reply.contains("NotLeader") || reply.contains("ReadNotReady") => {}
                    Attempt::Reply(reply) if reply.starts_with("UNKNOWN") => {
                        if !read {
                            return Ok(reply);
                        }
                    }
                    Attempt::Reply(reply) => return Err(reply.into()),
                    Attempt::Interrupted(reason) => {
                        if !read {
                            return Ok(format!("UNKNOWN {reason}"));
                        }
                    }
                    Attempt::Unavailable => (),
                }
            }
            if Instant::now() >= deadline {
                return Err("group unavailable; original operation remains unresolved".into());
            }
            std::thread::park_timeout(Duration::from_millis(25));
        }
    }
    fn observations(&self) -> Result<Vec<TransferObservation>, Failure> {
        self.profile
            .operation
            .reads()
            .into_iter()
            .map(|query| {
                let kind = match query.kind {
                    TransferReadKind::Intent => "intent",
                    TransferReadKind::Publication => "publication",
                    TransferReadKind::Source => "source",
                    TransferReadKind::Target => "target",
                };
                let reply = self.ask(query.group, &format!("transfer-read {kind}"), true)?;
                let encoded = reply
                    .strip_prefix("OK observation ")
                    .ok_or("missing observation")?;
                let value = checked(TransferObservation::decode_authenticated(&wire::unhex(
                    encoded,
                    MAX_TRANSFER_OBSERVATION_BYTES,
                )?))?;
                if value.read() != query {
                    return Err("wrong observation group or query".into());
                }
                Ok(value)
            })
            .collect()
    }
    fn next(&self, reads: &[TransferObservation]) -> Result<TransferAction, Failure> {
        let mut images = Vec::new();
        let mut bytes = 0usize;
        loop {
            let borrowed = images
                .iter()
                .map(|(source, target, image)| TransferImage {
                    source: *source,
                    target: *target,
                    image,
                })
                .collect::<Vec<_>>();
            let action = checked(self.profile.operation.next(reads, &borrowed))?;
            let TransferAction::Export(export) = action else {
                return Ok(action);
            };
            if images.len() >= MAX_OPERATION_IMAGES {
                return Err("transfer image count budget exceeded".into());
            }
            let reply = self.ask(
                export.source,
                &format!("transfer-export {}", export.target.id.get()),
                true,
            )?;
            let image = wire::read_image(reply.strip_prefix("OK image ").ok_or("missing image")?)?;
            bytes = bytes
                .checked_add(image.payload_capacity())
                .ok_or("image size overflow")?;
            if bytes > wire::APP_BYTES {
                return Err("transfer image byte budget exceeded".into());
            }
            images.push((export.source, export.target, image));
        }
    }
    fn execute(&self, action: &TransferAction) -> Result<String, Failure> {
        let authority = self.profile.operation.intent().before().input().authority;
        let (group, command) = match action {
            TransferAction::RecordIntent => (authority, "transfer-step intent".into()),
            TransferAction::Stage(g) => (*g, "transfer-step stage".into()),
            TransferAction::Fence(g) => (*g, "transfer-step fence".into()),
            TransferAction::Import(import) => (
                import.target(),
                format!(
                    "transfer-step import {}",
                    wire::hex(&checked(import.encode(wire::APP_BYTES))?)
                ),
            ),
            TransferAction::Publish(publication) => (
                authority,
                format!(
                    "transfer-step publish {}",
                    wire::hex(&checked(publication.encode(wire::APP_BYTES))?)
                ),
            ),
            TransferAction::Activate { target, activation } => (
                *target,
                format!(
                    "transfer-step activate {} {}",
                    activation.metadata_configuration.get(),
                    wire::hex(&checked(activation.decision.encode(wire::APP_BYTES))?)
                ),
            ),
            _ => return Err("action has no command".into()),
        };
        self.ask(group, &command, false)
    }
    fn retire(&self, args: &[String]) -> Result<(), Failure> {
        let [group, id] = args else {
            return Err("expected retire SOURCE RELEASE_ID".into());
        };
        let (source, release) = super::retirement::release(&self.profile, group, id)?;
        if let Some(status) = self.retirement_status(source, release)? {
            println!("{status}");
            return Ok(());
        }
        let observations = self.observations()?;
        let proof = checked(self.profile.operation.retirement_proof(
            &observations,
            source,
            release,
        ))?;
        let bytes = checked(proof.encode(voteboat::retirement::MAX_RETIREMENT_PROOF_BYTES))?;
        let reply = self.ask(
            source,
            &format!("retire-group {}", wire::hex(&bytes)),
            false,
        )?;
        println!("{reply}");
        if reply.starts_with("UNKNOWN") {
            return Err(
                "retirement outcome unknown; repeat the same profile, source and release".into(),
            );
        }
        let status = self
            .retirement_status(source, release)?
            .ok_or("retirement not confirmed; retry the same profile, source and release")?;
        println!("{status}");
        Ok(())
    }
    fn retirement_status(
        &self,
        source: GroupIdentity,
        release: OperationId,
    ) -> Result<Option<String>, Failure> {
        let status = self.ask(source, "retirement-status", true)?;
        if !status.split_whitespace().any(|s| s == "retirement=retired") {
            return Ok(None);
        }
        for expected in [
            format!("source={}", source.id.get()),
            format!("incarnation={}", source.incarnation.get()),
            format!("operation={}", self.profile.operation.operation().get()),
            format!("release={}", release.get()),
        ] {
            if !status.split_whitespace().any(|s| s == expected) {
                return Err("conflicting retirement identity or release".into());
            }
        }
        Ok(Some(status))
    }
}
pub fn operate(args: &[String]) -> Result<(), Failure> {
    let [profile, endpoints, tls, principal, verb, rest @ ..] = args else {
        return Err(super::HELP.into());
    };
    if verb == "retire" {
        return Client::load(profile, endpoints, tls, principal)?.retire(rest);
    }
    if !rest.is_empty() {
        return Err(super::HELP.into());
    }
    if !matches!(verb.as_str(), "status" | "start" | "resume" | "step") {
        return Err("expected status, start, resume or step".into());
    }
    let client = Client::load(profile, endpoints, tls, principal)?;
    let deadline = Instant::now() + Duration::from_secs(120);
    for _ in 0..128 {
        if Instant::now() >= deadline {
            break;
        }
        let reads = client.observations()?;
        if verb == "status" {
            println!(
                "OK next={:?}",
                checked(client.profile.operation.next(&reads, &[]))?
            );
            return Ok(());
        }
        let action = client.next(&reads)?;
        if action == TransferAction::Complete {
            println!("OK complete");
            return Ok(());
        }
        let result = client.execute(&action)?;
        println!("{result}");
        if verb == "step" {
            return Ok(());
        }
    }
    Err("operation still pending; resume original profile".into())
}
pub fn command(args: &[String]) -> Result<(), Failure> {
    let [profile, endpoints, tls, principal, group, words @ ..] = args else {
        return Err(super::HELP.into());
    };
    if words.is_empty() {
        return Err("missing command".into());
    }
    let client = Client::load(profile, endpoints, tls, principal)?;
    let group = client.profile.binding(group)?.group;
    let read = matches!(
        words[0].as_str(),
        "status" | "read" | "transfer-read" | "retirement-status"
    );
    let reply = client.ask(group, &words.join(" "), read)?;
    println!("{reply}");
    if reply.starts_with("UNKNOWN") {
        return Err("outcome unknown; inspect and retry original operation".into());
    }
    Ok(())
}
