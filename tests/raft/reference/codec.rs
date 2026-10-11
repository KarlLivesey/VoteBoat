// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
impl Trace {
    pub(super) fn encode(&self) -> Vec<u8> {
        let version = if self
            .actions
            .iter()
            .any(|a| matches!(a, Action::Persistence(..)))
        {
            2
        } else {
            1
        };
        let mut text = format!("VBDIF{version} {}\n", self.seed);
        for action in &self.actions {
            let line = match action {
                Action::Campaign(id) => format!("campaign {id}"),
                Action::Write(id, operation, delta) => format!("write {id} {operation} {delta}"),
                Action::Isolate(id) => format!("isolate {id}"),
                Action::Heal => "heal".into(),
                Action::Pump => "pump".into(),
                Action::Heartbeat(id) => format!("heartbeat {id}"),
                Action::Read(id, request) => format!("read {id} {request}"),
                Action::Check => "check".into(),
                Action::CorruptReference(id) => format!("corrupt {id}"),
                Action::Persistence(id, operation) => {
                    format!("{} {id}", persistence_tag(*operation))
                }
            };
            text.push_str(&line);
            text.push('\n');
        }
        text.into_bytes()
    }
    pub(super) fn decode(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() > 65536 || !bytes.ends_with(b"\n") {
            return Err("trace bounds");
        }
        let text = std::str::from_utf8(bytes).map_err(|_| "trace text")?;
        let mut lines = text.lines();
        let header: Vec<_> = lines
            .next()
            .ok_or("trace header")?
            .split_whitespace()
            .collect();
        let [version @ ("VBDIF1" | "VBDIF2"), seed] = header.as_slice() else {
            return Err("trace version");
        };
        let seed = seed.parse::<u64>().map_err(|_| "trace seed")?;
        if seed == 0 {
            return Err("trace seed");
        }
        let mut actions = Vec::new();
        for line in lines {
            if actions.len() == MAX_ACTIONS {
                return Err("action bound");
            }
            actions.push(decode_action(line, *version == "VBDIF2")?);
        }
        Ok(Self { seed, actions })
    }
}
fn decode_action(line: &str, persistence: bool) -> Result<Action, &'static str> {
    let fields: Vec<_> = line.split_whitespace().collect();
    Ok(match fields.as_slice() {
        ["campaign", id] => Action::Campaign(node_id(id)?),
        ["write", id, operation, delta] => {
            let operation = operation.parse::<u128>().map_err(|_| "operation")?;
            if operation == 0 {
                return Err("operation");
            }
            Action::Write(node_id(id)?, operation, delta.parse().map_err(|_| "delta")?)
        }
        ["isolate", id] => Action::Isolate(node_id(id)?),
        ["heal"] => Action::Heal,
        ["pump"] => Action::Pump,
        ["heartbeat", id] => Action::Heartbeat(node_id(id)?),
        ["read", id, request] => {
            let request = request.parse::<u64>().map_err(|_| "request")?;
            if request == 0 {
                return Err("request");
            }
            Action::Read(node_id(id)?, request)
        }
        ["check"] => Action::Check,
        ["corrupt", id] => Action::CorruptReference(node_id(id)?),
        [tag, id] if persistence => Action::Persistence(node_id(id)?, parse_persistence(tag)?),
        _ => return Err("action"),
    })
}
fn persistence_tag(operation: Persistence) -> &'static str {
    match operation {
        Persistence::Hold => "hold",
        Persistence::Write => "persist_write",
        Persistence::Sync => "sync",
        Persistence::Complete => "complete",
        Persistence::ProcessCrash => "process_crash",
        Persistence::PowerLoss => "power_loss",
        Persistence::Stale => "stale",
        Persistence::LieReferenceSync => "lie_sync",
    }
}
fn parse_persistence(tag: &str) -> Result<Persistence, &'static str> {
    match tag {
        "hold" => Ok(Persistence::Hold),
        "persist_write" => Ok(Persistence::Write),
        "sync" => Ok(Persistence::Sync),
        "complete" => Ok(Persistence::Complete),
        "process_crash" => Ok(Persistence::ProcessCrash),
        "power_loss" => Ok(Persistence::PowerLoss),
        "stale" => Ok(Persistence::Stale),
        "lie_sync" => Ok(Persistence::LieReferenceSync),
        _ => Err("persistence action"),
    }
}
fn node_id(text: &str) -> Result<u64, &'static str> {
    let id = text.parse().map_err(|_| "node")?;
    if !(1..=3).contains(&id) {
        return Err("node");
    }
    Ok(id)
}
