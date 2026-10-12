// SPDX-License-Identifier: RPL-1.5
use super::*;
mod atoms;
mod checkpoint;
mod outcomes;
mod program;
use atoms::*;
pub(super) use checkpoint::{checkpoint, checkpoint_bound, restore};
const MAGIC: &[u8; 8] = b"VBPRCMD1";
pub(super) fn encode(command: &Command) -> Result<Vec<u8>, ApplicationError> {
    command.validate()?;
    let mut out = Vec::with_capacity(PROGRAM_COMMAND_BYTES);
    out.extend(MAGIC);
    out.extend(PROGRAM_SCHEMA.to_le_bytes());
    out.push(step_byte(command.step));
    write_program(&mut out, &command.program);
    out.push(decision_byte(command.decision));
    out.push(command.facts.len() as u8);
    for f in &command.facts {
        group(&mut out, f.group);
        out.extend(f.operation.get().to_le_bytes());
        out.extend(f.index.to_le_bytes());
    }
    match command.refusal {
        Some((p, r)) => {
            out.push(p as u8 + 1);
            write_refusal(&mut out, Some(r));
        }
        None => {
            out.push(0);
            write_refusal(&mut out, None);
        }
    }
    if out.len() > PROGRAM_COMMAND_BYTES {
        return Err(ApplicationError::InvalidCommand);
    }
    out.shrink_to_fit();
    Ok(out)
}
pub(super) fn decode(bytes: &[u8]) -> Result<Command, ApplicationError> {
    if bytes.len() > PROGRAM_COMMAND_BYTES {
        return Err(ApplicationError::InvalidCommand);
    }
    let mut r = Reader(bytes);
    if r.take(8)? != MAGIC || r.u64()? != PROGRAM_SCHEMA {
        return Err(ApplicationError::InvalidCommand);
    }
    let step = r.step()?;
    let program = r.program()?;
    let decision = r.decision()?;
    let n = r.count_byte(MAX_CATALOG_GROUPS, 48)?;
    let mut facts = Vec::with_capacity(n);
    for _ in 0..n {
        facts.push(PhaseFact {
            group: r.group()?,
            operation: r.operation()?,
            index: r.u64()?,
        });
    }
    let p = r.byte()?;
    let refusal = match (p, r.refusal()?) {
        (0, None) => None,
        (1..=8, Some(refusal)) => Some((usize::from(p - 1), refusal)),
        _ => return Err(ApplicationError::InvalidCommand),
    };
    r.finish()?;
    let command = Command {
        program,
        step,
        decision,
        facts: facts.into_boxed_slice(),
        refusal,
    };
    command.validate()?;
    if encode(&command)? != bytes {
        return Err(ApplicationError::InvalidCommand);
    }
    Ok(command)
}
fn step_byte(step: ProgramStep) -> u8 {
    match step {
        ProgramStep::Start => 0,
        ProgramStep::Prepare => 1,
        ProgramStep::Decide => 2,
        ProgramStep::ApplyDecision => 3,
        ProgramStep::AppliedAll => 4,
        ProgramStep::Release => 5,
        ProgramStep::Finish => 6,
    }
}
fn decision_byte(d: Option<Decision>) -> u8 {
    match d {
        None => 0,
        Some(Decision::Commit) => 1,
        Some(Decision::Abort) => 2,
    }
}
fn group(out: &mut Vec<u8>, g: GroupIdentity) {
    out.extend(g.id.get().to_le_bytes());
    out.extend(g.incarnation.get().to_le_bytes());
}
use outcomes::{write_outcome, write_refusal};
use program::{write_profile, write_program};
