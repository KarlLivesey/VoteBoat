// SPDX-License-Identifier: RPL-1.5
use super::*;
const MAGIC: &[u8; 8] = b"VBPRCP01";
pub(in crate::program_transaction) fn checkpoint_bound(app: &ProgramApplication) -> usize {
    512 + app.limits.keys() * 48
        + app.transaction_capacity() * 4 * (16 + PROGRAM_COMMAND_BYTES + 35)
}
pub(in crate::program_transaction) fn checkpoint(
    app: &ProgramApplication,
    max: usize,
) -> Result<Vec<u8>, ApplicationError> {
    let base = 99
        + (app.profile.coordinators().len() + app.profile.participants().len()) * 24
        + app.keys.len() * 48;
    if base > max {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    let mut out = Vec::with_capacity(base);
    out.extend(MAGIC);
    out.extend(PROGRAM_SCHEMA.to_le_bytes());
    write_profile(&mut out, &app.profile);
    group(&mut out, app.group);
    out.push(u8::from(app.role == ProgramRole::Participant));
    out.extend((app.limits.coordinator_transactions() as u64).to_le_bytes());
    out.extend((app.limits.keys() as u64).to_le_bytes());
    out.extend(app.applied.to_le_bytes());
    out.extend((app.keys.len() as u64).to_le_bytes());
    out.extend((app.records.len() as u64).to_le_bytes());
    for (id, key) in &app.keys {
        out.extend(id.get().to_le_bytes());
        out.extend(key.opening.to_le_bytes());
        out.extend(key.value.to_le_bytes());
        out.extend(key.version.get().to_le_bytes());
        out.extend(key.lock.map_or(0, TransactionId::get).to_le_bytes());
    }
    let mut events: Vec<_> = app
        .records
        .values()
        .flat_map(|r| r.phases.iter().flatten())
        .collect();
    events.sort_by_key(|p| p.index);
    out.extend((events.len() as u64).to_le_bytes());
    for p in events {
        let bytes = encode(&p.command)?;
        if out
            .len()
            .checked_add(16 + bytes.len() + 35)
            .is_none_or(|n| n > max)
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        out.extend(p.index.to_le_bytes());
        out.extend((bytes.len() as u64).to_le_bytes());
        out.extend(bytes);
        write_outcome(&mut out, p.outcome);
    }
    if out.len() > max {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    Ok(out)
}
pub(in crate::program_transaction) fn restore(
    app: &mut ProgramApplication,
    schema: u64,
    applied: u64,
    bytes: &[u8],
) -> Result<(), ApplicationError> {
    if schema != PROGRAM_SCHEMA {
        return Err(ApplicationError::UnsupportedSchema);
    }
    let next = reconstruct(app, applied, bytes).map_err(|_| ApplicationError::InvalidCheckpoint)?;
    if checkpoint(&next, bytes.len())? != bytes {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    *app = next;
    Ok(())
}
fn reconstruct(
    config: &ProgramApplication,
    applied: u64,
    bytes: &[u8],
) -> Result<ProgramApplication, ApplicationError> {
    if bytes.len() > checkpoint_bound(config) {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    let mut r = Reader(bytes);
    check_header(config, applied, &mut r)?;
    let key_count = r.count_u64(config.limits.keys(), 48)?;
    let records = r.count_u64(config.transaction_capacity(), 0)?;
    if key_count != config.keys.len() || records as u64 > applied {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    // Materialized key bytes are compared to independently replayed state below.
    r.take(key_count * 48)?;
    let n = r.count_u64(config.transaction_capacity() * 4, 51)?;
    let mut next = ProgramApplication::empty(
        config.group,
        config.profile.clone(),
        config.limits,
        config.role,
    );
    next.keys = config
        .keys
        .iter()
        .map(|(id, k)| {
            (
                *id,
                Key {
                    opening: k.opening,
                    value: k.opening,
                    version: KeyVersion::default(),
                    lock: None,
                },
            )
        })
        .collect();
    let mut previous = 0;
    for _ in 0..n {
        let index = r.u64()?;
        if index <= previous || index > applied {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        let len = r.count_u64(PROGRAM_COMMAND_BYTES, 1)?;
        let command = decode(r.take(len)?)?;
        let expected = r.outcome()?;
        next.check_command(command.operation(config.group)?, &command)?;
        let (outcome, duplicate, first) = next.execute(command, index)?;
        if duplicate || first != index || outcome != expected {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        previous = index;
    }
    r.finish()?;
    if next.records.len() != records {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    next.applied = applied;
    Ok(next)
}
fn check_header(
    config: &ProgramApplication,
    applied: u64,
    r: &mut Reader<'_>,
) -> Result<(), ApplicationError> {
    if r.take(8)? != MAGIC
        || r.u64()? != PROGRAM_SCHEMA
        || r.profile()? != config.profile
        || r.group()? != config.group
        || r.byte()? != u8::from(config.role == ProgramRole::Participant)
        || r.u64()? != config.limits.coordinator_transactions() as u64
        || r.u64()? != config.limits.keys() as u64
        || r.u64()? != applied
    {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    Ok(())
}
