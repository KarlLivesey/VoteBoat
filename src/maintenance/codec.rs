// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
const COMMAND: &[u8; 8] = b"VBMNT001";
const CHECKPOINT: &[u8; 8] = b"VBMCP001";
pub(super) const RECORD_BYTES: usize = 121;
pub(super) const HEADER_BYTES: usize = 72;
pub(super) enum Envelope<'a> {
    Data(&'a [u8]),
    Control(LeadershipCommand),
}
fn valid_intent(i: LeadershipIntent) -> Result<(), ApplicationError> {
    if i.source.node == i.request.target.node {
        return Err(ApplicationError::InvalidCommand);
    }
    Ok(())
}
fn number(out: &mut Vec<u8>, n: u64) {
    out.extend(n.to_le_bytes());
}
fn peer(out: &mut Vec<u8>, p: PeerIdentity) {
    number(out, p.node.get());
    out.extend(p.store.id.get().to_le_bytes());
    number(out, p.store.incarnation.get());
}
fn intent(out: &mut Vec<u8>, i: LeadershipIntent) {
    out.extend(i.request.operation.get().to_le_bytes());
    number(out, i.request.configuration.get());
    peer(out, i.source);
    peer(out, i.request.target);
}
pub(super) fn data(bytes: &[u8], max: usize) -> Result<Vec<u8>, ApplicationError> {
    if bytes.len().checked_add(9).is_none_or(|n| n > max) {
        return Err(ApplicationError::InvalidCommand);
    }
    let mut out = Vec::with_capacity(9 + bytes.len());
    out.extend(COMMAND);
    out.push(0);
    out.extend(bytes);
    Ok(out)
}
pub(super) fn encode(command: LeadershipCommand) -> Result<Vec<u8>, ApplicationError> {
    valid_intent(command.intent())?;
    let mut out = Vec::with_capacity(113);
    out.extend(COMMAND);
    out.push(match command {
        LeadershipCommand::Begin(_) => 1,
        LeadershipCommand::Complete { .. } => 2,
        LeadershipCommand::Cancel { .. } => 3,
    });
    intent(&mut out, command.intent());
    match command {
        LeadershipCommand::Begin(_) => (),
        LeadershipCommand::Complete { index, term, .. } => {
            number(&mut out, index);
            number(&mut out, term);
        }
        LeadershipCommand::Cancel { index, .. } => number(&mut out, index),
    }
    Ok(out)
}
pub(super) fn decode(bytes: &[u8]) -> Result<Envelope<'_>, ApplicationError> {
    let mut r = Reader(bytes);
    if r.take(8)? != COMMAND {
        return Err(ApplicationError::InvalidCommand);
    }
    let kind = r.u8()?;
    if kind == 0 {
        return Ok(Envelope::Data(r.0));
    }
    let i = r.intent()?;
    let command = match kind {
        1 => LeadershipCommand::Begin(i),
        2 => LeadershipCommand::Complete {
            intent: i,
            index: r.u64()?,
            term: r.u64()?,
        },
        3 => LeadershipCommand::Cancel {
            intent: i,
            index: r.u64()?,
        },
        _ => return Err(ApplicationError::InvalidCommand),
    };
    r.finish()?;
    Ok(Envelope::Control(command))
}
pub(super) fn checkpoint<A: CheckpointStateMachine>(
    app: &Maintenance<A>,
    max: usize,
) -> Result<Vec<u8>, ApplicationError> {
    let overhead = HEADER_BYTES
        .checked_add(RECORD_BYTES * app.records.len())
        .ok_or(ApplicationError::InvalidCheckpoint)?;
    let inner = app.inner.checkpoint(
        max.checked_sub(overhead)
            .ok_or(ApplicationError::InvalidCheckpoint)?,
    )?;
    let mut out = Vec::with_capacity(overhead + inner.len());
    out.extend(CHECKPOINT);
    number(&mut out, app.schema);
    out.extend(app.group.id.get().to_le_bytes());
    number(&mut out, app.group.incarnation.get());
    number(&mut out, app.capacity as u64);
    number(&mut out, app.inner.schema_version());
    number(&mut out, inner.len() as u64);
    number(&mut out, app.records.len() as u64);
    out.extend(inner);
    for record in app.records.values() {
        intent(&mut out, record.intent);
        number(&mut out, record.index);
        number(&mut out, record.term);
        let (kind, index, term) = match record.phase {
            LeadershipPhase::Pending => (0, 0, 0),
            LeadershipPhase::Completed { index, term } => (1, index, term),
            LeadershipPhase::Cancelled { index, term } => (2, index, term),
        };
        out.push(kind);
        number(&mut out, index);
        number(&mut out, term);
    }
    Ok(out)
}
pub(super) fn restore<A: CheckpointStateMachine>(
    app: &mut Maintenance<A>,
    schema: u64,
    applied: u64,
    bytes: &[u8],
) -> Result<(), ApplicationError> {
    let mut r = Reader(bytes);
    if schema != app.schema
        || r.take(8)? != CHECKPOINT
        || r.u64()? != schema
        || r.u128()? != app.group.id.get()
        || r.u64()? != app.group.incarnation.get()
        || r.u64()? != app.capacity as u64
        || r.u64()? != app.inner.schema_version()
    {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    let inner_bytes = usize::try_from(r.u64()?).map_err(|_| ApplicationError::InvalidCheckpoint)?;
    let count = usize::try_from(r.u64()?).map_err(|_| ApplicationError::InvalidCheckpoint)?;
    if count > app.capacity {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    let inner = r.take(inner_bytes)?;
    let mut records = BTreeMap::new();
    for _ in 0..count {
        let intent = r.intent()?;
        let index = r.u64()?;
        let term = r.u64()?;
        let kind = r.u8()?;
        let end = r.u64()?;
        let end_term = r.u64()?;
        let phase = match kind {
            0 if end == 0 && end_term == 0 => LeadershipPhase::Pending,
            1 if end > index && end <= applied && end_term > term => LeadershipPhase::Completed {
                index: end,
                term: end_term,
            },
            2 if end > index && end <= applied && end_term >= term => LeadershipPhase::Cancelled {
                index: end,
                term: end_term,
            },
            _ => return Err(ApplicationError::InvalidCheckpoint),
        };
        if index == 0
            || index > applied
            || term == 0
            || records
                .insert(
                    intent.request.operation,
                    LeadershipRecord {
                        intent,
                        index,
                        term,
                        phase,
                    },
                )
                .is_some()
        {
            return Err(ApplicationError::InvalidCheckpoint);
        }
    }
    r.finish()?;
    if records
        .values()
        .filter(|r| r.phase == LeadershipPhase::Pending)
        .count()
        > 1
    {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    let mut next = app.inner.clone();
    next.restore_checkpoint(next.schema_version(), applied, inner)?;
    if next.applied_index() != applied {
        return Err(ApplicationError::InvalidCheckpoint);
    }
    app.inner = next;
    app.records = records;
    Ok(())
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], ApplicationError> {
        let (a, b) = self
            .0
            .split_at_checked(n)
            .ok_or(ApplicationError::InvalidCommand)?;
        self.0 = b;
        Ok(a)
    }
    fn u8(&mut self) -> Result<u8, ApplicationError> {
        Ok(self.take(1)?[0])
    }
    fn u64(&mut self) -> Result<u64, ApplicationError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u128(&mut self) -> Result<u128, ApplicationError> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    fn peer(&mut self) -> Result<PeerIdentity, ApplicationError> {
        Ok(PeerIdentity {
            node: NodeId::new(self.u64()?).ok_or(ApplicationError::InvalidCommand)?,
            store: StoreIdentity {
                id: StoreId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)?,
                incarnation: StoreIncarnation::new(self.u64()?)
                    .ok_or(ApplicationError::InvalidCommand)?,
            },
        })
    }
    fn intent(&mut self) -> Result<LeadershipIntent, ApplicationError> {
        let operation = OperationId::new(self.u128()?).ok_or(ApplicationError::InvalidCommand)?;
        let configuration =
            ConfigurationId::new(self.u64()?).ok_or(ApplicationError::InvalidCommand)?;
        let source = self.peer()?;
        let target = self.peer()?;
        let i = LeadershipIntent {
            request: LeadershipTransferRequest {
                operation,
                target,
                configuration,
            },
            source,
        };
        valid_intent(i)?;
        Ok(i)
    }
    fn finish(self) -> Result<(), ApplicationError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(ApplicationError::InvalidCommand)
        }
    }
}
