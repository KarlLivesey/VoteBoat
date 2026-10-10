// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Explicit metadata commands and one quorum-backed remote manifest lookup.
use super::{
    command_client::{self, Attempt},
    command_endpoints,
    directory_connection::ACK,
    service_access::{Channel, ClientAccess},
    setup::{checked, Failure},
};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use voteboat::{
    identity::*,
    native::{remote_manifest::*, routing::NativeManifestCache},
    routing::*,
    runtime::MonoTime,
    secure::SessionPollBudget,
};
struct Client {
    channel: Channel,
    start: Instant,
    deadline: Instant,
    node: u64,
}
fn connection(args: &[String], command: &str) -> Result<Client, Failure> {
    let [base, node, tls, principal, rest @ ..] = args else {
        return Err("expected BASE NODE TLS PRINCIPAL".into());
    };
    let (base, node) = super::ids(base, node)?;
    let path = match rest {
        [] => None,
        [flag, path] if flag == "--command-peers" => Some(Path::new(path)),
        _ => return Err("expected optional --command-peers FILE".into()),
    };
    let targets = command_endpoints::targets(base, Some(node), path)?;
    let access = ClientAccess::load(Path::new(tls), principal.parse()?, &targets)?;
    let start = Instant::now();
    let deadline = start + Duration::from_secs(10);
    let mut channel = command_client::connect(&targets[0], deadline, Some(&access), start)
        .map_err(|_| "metadata connection or authentication failed")?;
    match command_client::request(&mut channel, command.as_bytes(), deadline, start) {
        Attempt::Reply(reply) if command == "manifest-session\n" && reply == ACK => (),
        Attempt::Reply(reply) => {
            print!("{reply}");
            if !reply.starts_with("OK ") {
                return Err("metadata command refused".into());
            }
        }
        Attempt::Interrupted(reason) => {
            return Err(format!(
                "metadata command outcome unknown ({reason}); retry original operation"
            )
            .into())
        }
        Attempt::Unavailable => return Err("metadata source unavailable".into()),
    }
    Ok(Client {
        channel,
        start,
        deadline,
        node,
    })
}
pub fn command(args: &[String]) -> Result<(), Failure> {
    let [base, node, tls, principal, rest @ ..] = args else {
        return Err("expected BASE NODE TLS PRINCIPAL COMMAND".into());
    };
    let mut parts = vec![base.clone(), node.clone(), tls.clone(), principal.clone()];
    let (words, options) = if rest.len() >= 2 && rest[rest.len() - 2] == "--command-peers" {
        rest.split_at(rest.len() - 2)
    } else {
        (rest, &[][..])
    };
    if words.is_empty() {
        return Err("missing metadata command".into());
    }
    parts.extend_from_slice(options);
    let text = format!("{}\n", words.join(" "));
    if text.len() > 256 {
        return Err("command exceeds256 bytes".into());
    }
    connection(&parts, &text)?;
    Ok(())
}
pub fn lookup(args: &[String]) -> Result<(), Failure> {
    let [base, node, tls, principal, group, incarnation, responsibility, responsibility_incarnation, rest @ ..] =
        args
    else {
        return Err(
            "expected BASE NODE TLS PRINCIPAL GROUP INCARNATION RESPONSIBILITY INCARNATION".into(),
        );
    };
    let query = ManifestLookup {
        locator: AuthorityLocator {
            authority: GroupIdentity {
                id: GroupId::new(group.parse()?).ok_or("invalid group")?,
                incarnation: GroupIncarnation::new(incarnation.parse()?)
                    .ok_or("invalid incarnation")?,
            },
            responsibility: ResponsibilityIdentity {
                id: ResponsibilityId::new(responsibility.parse()?)
                    .ok_or("invalid responsibility")?,
                incarnation: ResponsibilityIncarnation::new(responsibility_incarnation.parse()?)
                    .ok_or("invalid responsibility incarnation")?,
            },
        },
        minimum_epoch: None,
        minimum_generation: None,
    };
    let mut parts = vec![base.clone(), node.clone(), tls.clone(), principal.clone()];
    parts.extend_from_slice(rest);
    let mut client = connection(&parts, "manifest-session\n")?;
    let session = client
        .channel
        .take_secure()
        .ok_or("missing secure channel")?;
    let timestamp = || MonoTime(client.start.elapsed().as_millis().min(u64::MAX as u128) as u64);
    let cache = checked(NativeManifestCache::new(ManifestCacheLimits {
        manifests: 64,
        bytes: 256 * 1024,
    }))?;
    let mut remote = NativeRemoteManifestDiscovery::new(
        session,
        super::service_access::transport_identity(client.node, false),
        query.locator.authority,
        cache,
        RemoteManifestConfig::default(),
        timestamp(),
    )
    .map_err(|(e, _, _)| format!("remote manifest setup: {e:?}"))?;
    loop {
        if Instant::now() >= client.deadline {
            return Err("manifest lookup deadline expired".into());
        }
        match remote.lookup(query, timestamp()) {
            Ok(value) => {
                let m = value.manifest.input();
                println!("OK responsibility={} incarnation={} authority={} authority_incarnation={} generation={} epoch={} execution={:?}",m.responsibility.id.get(),m.responsibility.incarnation.get(),m.authority.id.get(),m.authority.incarnation.get(),m.generation.get(),m.epoch.get(),m.execution);
                remote.close();
                return Ok(());
            }
            Err(ManifestDiscoveryError::Unavailable) => (),
            Err(e) => return Err(format!("manifest lookup: {e:?}").into()),
        }
        if let Some(c) = remote
            .poll(timestamp(), SessionPollBudget::default())
            .map_err(|e| format!("manifest transport: {e:?}"))?
        {
            c.result.map_err(|e| format!("manifest response: {e:?}"))?;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
}
