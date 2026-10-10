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
//! Bounded configured routing. Writes require explicit non-acceptance; retried reads
//! always request a new quorum barrier.
use super::{
    command_client::{connect, request, Attempt},
    command_endpoints::{self, Endpoint},
    service_access::ClientAccess,
    setup::Failure,
};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
const NOT_LEADER: &str = "ERR NOT_LEADER\n";
pub(super) fn exchange(
    target: &Endpoint,
    text: &[u8],
    deadline: Instant,
    auth: Option<&ClientAccess>,
    start: Instant,
) -> Attempt {
    let mut stream = match connect(target, deadline, auth, start) {
        Ok(s) => s,
        Err(e) => return e,
    };
    request(&mut stream, text, deadline, start)
}
fn retryable_reply(command: &[String], response: &str) -> bool {
    let command = super::group_command::payload(command);
    response == NOT_LEADER
        || command == ["read"]
            && matches!(
                response,
                "ERR NotRead(ReadNotReady)\n"
                    | "ERR Unavailable(LeadershipChanged)\n"
                    | "ERR Draining\n"
            )
}
fn terminal(response: String) -> Result<(), Failure> {
    print!("{response}");
    if response.starts_with("OK ") {
        Ok(())
    } else {
        Err(
            "request unsuccessful; preserve the operation ID and payload when retrying a write"
                .into(),
        )
    }
}
fn interrupted(command: &[String], reason: &str) -> Result<(), Failure> {
    let command = super::group_command::payload(command);
    if command.first().is_some_and(|c| c == "add") {
        println!("UNKNOWN {reason}; retry the same operation ID and delta");
    } else if command
        .first()
        .is_some_and(|c| c == "configure" || c == "configure-record")
    {
        println!("UNKNOWN {reason}; retry the same configuration operation ID and record");
    } else if command.first().is_some_and(|c| {
        matches!(
            c.as_str(),
            "move-leader" | "resume-leadership" | "cancel-leadership"
        )
    }) {
        println!("UNKNOWN {reason}; retry the same administrative operation ID and record");
    } else {
        println!("ERR {reason}");
    }
    Err("request interrupted after connection; automatic routing stopped".into())
}
pub(super) struct Options {
    pub tls_directory: Option<PathBuf>,
    pub principal: Option<u64>,
    pub command_peers: Option<PathBuf>,
    pub discover_via: Option<u64>,
}
pub(super) fn options(command: &mut Vec<String>) -> Result<Options, Failure> {
    let mut tls_directory = None;
    let mut principal = None;
    let mut command_peers = None;
    let mut discover_via = None;
    while command.len() >= 2 {
        let flag = command[command.len() - 2].as_str();
        if !matches!(
            flag,
            "--service-tls" | "--principal" | "--command-peers" | "--discover-via"
        ) {
            break;
        }
        let value = command.pop().unwrap();
        match command.pop().unwrap().as_str() {
            "--service-tls" if tls_directory.is_none() => {
                tls_directory = Some(std::path::PathBuf::from(value))
            }
            "--principal" if principal.is_none() => principal = Some(value.parse::<u64>()?),
            "--discover-via" if discover_via.is_none() => {
                discover_via = Some(value.parse::<u64>()?)
            }
            "--command-peers" if command_peers.is_none() => {
                command_peers = Some(PathBuf::from(value))
            }
            _ => return Err("duplicate service client option".into()),
        }
    }
    if command_peers.is_some() && (tls_directory.is_none() || principal.is_none()) {
        return Err("--command-peers requires --service-tls and --principal".into());
    }
    if discover_via.is_some() && command_peers.is_none() {
        return Err("--discover-via requires --command-peers".into());
    }
    Ok(Options {
        tls_directory,
        principal,
        command_peers,
        discover_via,
    })
}
pub fn run(base: u16, id: Option<u64>, input: &[String]) -> Result<(), Failure> {
    let mut command = input.to_vec();
    let options = options(&mut command)?;
    let mut targets = command_endpoints::targets(
        base,
        if options.discover_via.is_some() {
            None
        } else {
            id
        },
        options.command_peers.as_deref(),
    )?;
    let source = options
        .discover_via
        .map(|node| {
            targets
                .iter()
                .find(|t| t.node == node)
                .cloned()
                .ok_or("discovery source missing from command peers")
        })
        .transpose()?;
    let auth = match (options.tls_directory.as_deref(), options.principal) {
        (None, None) => None,
        (Some(directory), Some(principal)) => {
            Some(ClientAccess::load(directory, principal, &targets)?)
        }
        _ => return Err("select --service-tls and --principal together".into()),
    };
    if let Some(node) = id {
        targets.retain(|t| t.node == node);
    }
    if targets.is_empty() {
        return Err("target node missing from command peers".into());
    }
    let command = command.as_slice();
    if id.is_none() {
        match super::group_command::payload(command) {
            [cmd] if cmd == "read" => (),
            [cmd, operation, delta]
                if cmd == "add"
                    && operation.parse::<u128>().is_ok_and(|n| n != 0)
                    && delta.parse::<i64>().is_ok() => {}
            _ => return Err("auto accepts only read or add NONZERO_OPERATION_ID DELTA".into()),
        }
    }
    let bytes = command.iter().try_fold(0usize, |bytes, part| {
        bytes.checked_add(part.len())?.checked_add(1)
    });
    if bytes.is_none_or(|n| n > 256) {
        return Err("command too long".into());
    }
    let text = format!("{}\n", command.join(" "));
    super::group_command::parse(&text)?;
    let start = Instant::now();
    let deadline = start + Duration::from_secs(10);
    if let Some(source) = source {
        let mut stream = connect(&source, deadline, auth.as_ref(), start).map_err(|_| {
            "discovery source connection/authentication failed before command submission"
        })?;
        match request(&mut stream, b"discover\n", deadline, start) {
            Attempt::Reply(reply) if reply == super::command_discovery::ACK => (),
            _ => {
                return Err(
                    "discovery upgrade refused or interrupted before command submission".into(),
                )
            }
        }
        let session = stream.take_secure().ok_or("discovery requires TLS")?;
        super::command_discovery::resolve(session, source.node, &mut targets, start, deadline)?;
    }
    if id.is_some() {
        return match exchange(&targets[0], text.as_bytes(), deadline, auth.as_ref(), start) {
            Attempt::Unavailable => Err("node unavailable before connection".into()),
            Attempt::Interrupted(reason) => interrupted(command, reason),
            Attempt::Reply(response) => terminal(response),
        };
    }
    // Each attempt owns one socket and the same bounded original command. No leader cache.
    for _ in 0..100 {
        for target in &targets {
            if Instant::now() >= deadline {
                return Err(
                    "no eligible configured leader found within the routing deadline".into(),
                );
            }
            match exchange(target, text.as_bytes(), deadline, auth.as_ref(), start) {
                Attempt::Unavailable => (),
                Attempt::Reply(response) if retryable_reply(command, &response) => {}
                Attempt::Reply(response) => return terminal(response),
                Attempt::Interrupted(reason) => return interrupted(command, reason),
            }
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        std::thread::park_timeout(remaining.min(Duration::from_millis(50)));
    }
    Err("no eligible configured leader found within the routing attempt limit".into())
}
