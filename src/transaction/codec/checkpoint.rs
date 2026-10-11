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
use crate::identity::OperationId;
use crate::transaction::state::StoredPhase;
use std::collections::BTreeSet;

const MAGIC: &[u8; 8] = b"VBTRCP01";
const HEADER: usize = 153;
const ACCOUNT: usize = 40;
const SLOT: usize = 1 + 8 + TRANSFER_COMMAND_BYTES + OUTCOME_BYTES;
const RECORD: usize = SPEC_BYTES + 2 + OUTCOME_BYTES + 4 * SLOT;
pub(in crate::transaction) fn checkpoint_bound(app: &TransferApplication) -> usize {
    HEADER + ACCOUNT * app.limits.accounts() + RECORD * app.limits.transactions()
}
pub(in crate::transaction) fn checkpoint(
    app: &TransferApplication,
    max: usize,
) -> Result<Vec<u8>, ApplicationError> {
    let size = HEADER + ACCOUNT * app.accounts.len() + RECORD * app.records.len();
    if size > max {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    let mut out = Vec::with_capacity(size);
    out.extend(MAGIC);
    out.extend(TRANSFER_SCHEMA.to_le_bytes());
    out.push(role_byte(app.role));
    group(&mut out, app.group);
    profile(&mut out, app.profile);
    for n in [app.limits.transactions(), app.limits.accounts()] {
        out.extend((n as u64).to_le_bytes());
    }
    out.extend(app.applied.to_le_bytes());
    out.extend((app.accounts.len() as u64).to_le_bytes());
    out.extend((app.records.len() as u64).to_le_bytes());
    for (id, account) in &app.accounts {
        out.extend(id.get().to_le_bytes());
        out.extend(account.opening.to_le_bytes());
        out.extend(account.balance.to_le_bytes());
        out.extend(account.lock.map_or(0, TransactionId::get).to_le_bytes());
    }
    for record in app.records.values() {
        spec(&mut out, record.spec);
        phase(&mut out, record.phase);
        outcome(&mut out, record.prepare.map(TransferOutcome::Prepare));
        for phase in record.phases {
            if let Some(p) = phase {
                out.push(1);
                out.extend(p.index.to_le_bytes());
                out.extend(encode(p.command)?);
                outcome(&mut out, Some(p.outcome));
            } else {
                out.extend([0; SLOT]);
            }
        }
    }
    debug_assert_eq!(out.len(), size);
    Ok(out)
}
fn role_byte(role: TransferRole) -> u8 {
    match role {
        TransferRole::Coordinator => 0,
        TransferRole::Participant(p) => p as u8 + 1,
    }
}
pub(in crate::transaction) fn restore(
    app: &mut TransferApplication,
    schema: u64,
    applied: u64,
    bytes: &[u8],
) -> Result<(), ApplicationError> {
    if schema != TRANSFER_SCHEMA {
        return Err(ApplicationError::UnsupportedSchema);
    }
    let parsed = parse(app, applied, bytes).map_err(|_| ApplicationError::InvalidCheckpoint)?;
    let rebuilt = reconstruct(app, &parsed).map_err(|_| ApplicationError::InvalidCheckpoint)?;
    if rebuilt != parsed {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    *app = rebuilt;
    Ok(())
}
fn parse(
    app: &TransferApplication,
    applied: u64,
    bytes: &[u8],
) -> Result<TransferApplication, ApplicationError> {
    if bytes.len() < HEADER || bytes.len() > checkpoint_bound(app) {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    let mut r = Reader(bytes);
    if r.take(8)? != MAGIC
        || r.u64()? != TRANSFER_SCHEMA
        || r.byte()? != role_byte(app.role)
        || r.group()? != app.group
        || r.profile()? != app.profile
        || r.u64()? != app.limits.transactions() as u64
        || r.u64()? != app.limits.accounts() as u64
        || r.u64()? != applied
    {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    let accounts = usize::try_from(r.u64()?).map_err(|_| ApplicationError::InvalidCheckpoint)?;
    let records = usize::try_from(r.u64()?).map_err(|_| ApplicationError::InvalidCheckpoint)?;
    if accounts != app.accounts.len()
        || records > app.limits.transactions()
        || records as u64 > applied
        || bytes.len() != HEADER + accounts * ACCOUNT + records * RECORD
    {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    let mut next = app.clone();
    next.applied = applied;
    next.accounts.clear();
    next.records.clear();
    for _ in 0..accounts {
        let id = AccountId::new(r.u64()?).ok_or(ApplicationError::InvalidCheckpoint)?;
        let opening = r.i64()?;
        let balance = r.i64()?;
        let lock = r.u128()?;
        if next
            .accounts
            .last_key_value()
            .is_some_and(|(previous, _)| *previous >= id)
            || opening < 0
            || balance < 0
            || app.accounts.get(&id).is_none_or(|a| a.opening != opening)
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
        next.accounts.insert(
            id,
            Account {
                opening,
                balance,
                lock: if lock == 0 {
                    None
                } else {
                    Some(TransactionId::new(lock).ok_or(ApplicationError::InvalidCheckpoint)?)
                },
            },
        );
    }
    for _ in 0..records {
        read_record(&mut next, &mut r)?;
    }
    r.finish()?;
    Ok(next)
}
fn read_record(app: &mut TransferApplication, r: &mut Reader<'_>) -> Result<(), ApplicationError> {
    let spec = r.spec()?;
    let phase = r.phase()?;
    let prepare = match r.outcome()? {
        None => None,
        Some(TransferOutcome::Prepare(p)) => Some(p),
        _ => return Err(ApplicationError::InvalidCheckpoint),
    };
    let mut phases = [None; 4];
    for slot in &mut phases {
        let present = r.byte()?;
        let index = r.u64()?;
        let bytes = r.take(TRANSFER_COMMAND_BYTES)?;
        let outcome = r.outcome()?;
        *slot = match (present, index, outcome) {
            (0, 0, None) if bytes.iter().all(|v| *v == 0) => None,
            (1, 1.., Some(outcome)) if index <= app.applied => Some(StoredPhase {
                command: decode(bytes)?,
                outcome,
                index,
            }),
            _ => return Err(ApplicationError::InvalidCheckpoint),
        };
    }
    if app
        .records
        .last_key_value()
        .is_some_and(|(previous, _)| *previous >= spec.id())
        || spec.profile() != app.profile
    {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    app.records.insert(
        spec.id(),
        Record {
            spec,
            phase,
            prepare,
            phases,
        },
    );
    Ok(())
}
fn reconstruct(
    config: &TransferApplication,
    parsed: &TransferApplication,
) -> Result<TransferApplication, ApplicationError> {
    let mut next =
        TransferApplication::empty(config.group, config.profile, config.limits, config.role);
    next.accounts = config
        .accounts
        .iter()
        .map(|(id, a)| {
            (
                *id,
                Account {
                    opening: a.opening,
                    balance: a.opening,
                    lock: None,
                },
            )
        })
        .collect();
    let mut positions = BTreeSet::new();
    let mut events = Vec::with_capacity(parsed.records.len() * 4);
    for record in parsed.records.values() {
        for (slot, stored) in record.phases.iter().enumerate() {
            if let Some(p) = stored {
                if p.command.spec != record.spec
                    || p.command.slot() != slot
                    || !positions.insert(p.index)
                {
                    return Err(ApplicationError::InvalidCheckpoint);
                }
                events.push(*p);
            }
        }
    }
    events.sort_by_key(|p| p.index);
    for p in events {
        let operation: OperationId = p.command.operation(config.group)?;
        next.check_command(operation, p.command)?;
        let (outcome, duplicate, first) = next.execute(p.command, p.index)?;
        if duplicate || first != p.index || outcome != p.outcome {
            return Err(ApplicationError::InvalidCheckpoint);
        }
    }
    next.applied = parsed.applied;
    Ok(next)
}
