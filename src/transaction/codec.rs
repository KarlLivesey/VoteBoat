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
use super::*;
mod checkpoint;
mod outcome;
mod wire;
pub(super) use checkpoint::{checkpoint, checkpoint_bound, restore};
use wire::*;
const COMMAND_MAGIC: &[u8; 8] = b"VBTRCMD1";

pub(super) fn encode(command: Command) -> Result<Vec<u8>, ApplicationError> {
    command.validate()?;
    let mut out = Vec::with_capacity(TRANSFER_COMMAND_BYTES);
    out.extend(COMMAND_MAGIC);
    out.push(step_byte(command.step));
    spec(&mut out, command.spec);
    out.push(decision_byte(command.decision));
    out.push(command.refusal.map_or(0, |(p, _)| p as u8 + 1));
    for f in command.facts {
        fact(&mut out, f);
    }
    refusal(&mut out, command.refusal.map(|(_, r)| r));
    debug_assert_eq!(out.len(), TRANSFER_COMMAND_BYTES);
    Ok(out)
}
pub(super) fn decode(bytes: &[u8]) -> Result<Command, ApplicationError> {
    if bytes.len() != TRANSFER_COMMAND_BYTES {
        return Err(ApplicationError::InvalidCommand);
    }
    let mut r = Reader(bytes);
    if r.take(8)? != COMMAND_MAGIC {
        return Err(ApplicationError::InvalidCommand);
    }
    let step = r.step()?;
    let spec = r.spec()?;
    let decision = r.decision()?;
    let p = r.byte()?;
    let facts = [r.fact()?, r.fact()?];
    let refusal = r.refusal()?;
    let refusal = match (p, refusal) {
        (0, None) => None,
        (1..=2, Some(r)) => Some((usize::from(p - 1), r)),
        _ => return Err(ApplicationError::InvalidCommand),
    };
    r.finish()?;
    let command = Command {
        spec,
        step,
        decision,
        facts,
        refusal,
    };
    command.validate()?;
    Ok(command)
}
