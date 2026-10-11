// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Enumerates voter sets and replays recorded bytes without protocol helpers.
use super::types::*;

fn majority(mask: u8, members: [usize; 3]) -> bool {
    let mut count = 0;
    for member in members {
        if mask & (1 << member) != 0 {
            count += 1;
        }
    }
    count >= 2
}
fn predicate(policy: Policy, mask: u8) -> bool {
    match policy {
        Policy::Old => majority(mask, [0, 1, 2]),
        Policy::Joint => majority(mask, [0, 1, 2]) && majority(mask, [1, 2, 3]),
        Policy::New => majority(mask, [1, 2, 3]),
    }
}
fn control(entries: &[Entry]) -> Result<Control, &'static str> {
    let mut out = Control::default();
    for entry in entries {
        match *entry {
            Entry::Begin { operation, layer } => {
                if out.receipts.iter().all(|(old, _)| *old != operation) {
                    let free = out.active.is_none();
                    out.receipts.push((operation, free));
                    if free {
                        out.active = Some((operation, layer, false));
                    }
                }
            }
            Entry::Outcome { operation, layer } => {
                if out.active != Some((operation, layer, false)) {
                    return Err("outcome has no exact original replicated intent");
                }
                out.active = Some((operation, layer, true));
            }
            Entry::Release(op) => {
                if !matches!(out.active, Some((old, _, true)) if old == op) {
                    return Err("intent release precedes durable terminal outcome");
                }
                out.active = None;
            }
            Entry::Joint(_) | Entry::Final(_) | Entry::Noop => {}
        }
    }
    Ok(out)
}
fn decode(records: &[Record]) -> Result<Image, &'static str> {
    let mut out = Image::default();
    for record in records {
        match record {
            Record::Promise => {
                out.term = 1;
                out.vote = Some(7);
                out.bytes = vec![0x58, 0x59];
            }
            Record::Import(bytes) => {
                if bytes.term != 1
                    || bytes.vote != Some(7)
                    || bytes.bytes != [0x58, 0x59]
                    || bytes.epoch != 0
                    || bytes.representative != 0
                {
                    return Err("new logical cell imported an absent payload or foreign fence");
                }
                out = *bytes.clone();
            }
            Record::Parent { position, entry } => {
                if *position != out.parent.len() + 1 {
                    return Err("parent accepted prefix has a hole");
                }
                out.parent.push(*entry);
            }
            Record::Replace { position, entry } => {
                if *position == 0 || *position > out.parent.len() {
                    return Err("replacement is outside the accepted parent suffix");
                }
                out.parent.truncate(*position - 1);
                out.parent.push(*entry);
            }
            Record::Claim {
                epoch,
                representative,
            } => {
                if *epoch != out.epoch + 1 || *representative >= 4 {
                    return Err("representative claim erased or reused its fence");
                }
                out.epoch = *epoch;
                out.representative = *representative;
            }
            Record::Joint(_) | Record::Final(_) | Record::Noop => {}
        }
    }
    Ok(out)
}
fn cell(cell: &Cell) -> Result<(), &'static str> {
    if cell.observed > cell.applied
        || cell.applied > cell.committed
        || cell.committed > cell.accepted.len()
        || cell.accepted.len() > 64
    {
        return Err("child boundary or bounded record capacity violated");
    }
    let canonical = &cell.accepted[..cell.committed];
    let mut witnesses = 0;
    for (i, physical) in cell.physical.iter().enumerate() {
        if cell.physical_mask & (1 << i) == 0 {
            continue;
        }
        // Reconstruct each store from its OWN synchronized checkpoint and suffix.
        let own: Vec<_> = physical
            .checkpoint
            .iter()
            .chain(&physical.synced)
            .cloned()
            .collect();
        if !canonical.starts_with(&physical.checkpoint) {
            return Err("checkpoint published outside committed ordered history");
        }
        // A lagging follower may retain a different UNCOMMITTED suffix while
        // another exact synchronized quorum commits its replacement. Such a
        // follower is not a witness; it need not instantly receive the commit.
        if own.starts_with(canonical) {
            witnesses |= 1 << i;
        }
        if physical.checkpoint.len() > own.len() {
            return Err("checkpoint boundary has no bytes");
        }
    }
    // Prefix preservation is the qualified ordinary child-Raft assumption.
    // Once committed it must remain present in an intersecting voter set.
    let policy = canonical.iter().fold(Policy::Old, |old, r| match r {
        Record::Joint(_) => Policy::Joint,
        Record::Final(_) => Policy::New,
        _ => old,
    });
    if !predicate(policy, witnesses) {
        return Err("committed child prefix lacks recoverable witnesses");
    }
    let value = decode(&cell.accepted[..cell.applied])?;
    if value.term != 0 && (value.term != 1 || value.vote != Some(7) || value.bytes != [0x58, 0x59])
    {
        return Err("replacement lost original parent promise or payload");
    }
    Ok(())
}
pub fn check(state: &State) -> Result<(), &'static str> {
    for (i, c) in state.cells.iter().enumerate() {
        if usize::from(c.identity) != i {
            return Err("logical cell identity aliased another cell");
        }
        cell(c)?;
    }
    if let Some(recovery) = &state.recovery {
        let c = &state.cells[recovery.cell];
        let p = &c.physical[recovery.physical];
        let actual: Vec<_> = p.checkpoint.iter().chain(&p.synced).cloned().collect();
        if recovery.boundary != recovery.own_bytes.len()
            || !actual.starts_with(&recovery.own_bytes)
            || !c.accepted.starts_with(&recovery.own_bytes)
        {
            return Err("recovery restored a cache/oracle image instead of selected own bytes");
        }
        let decoded = decode(&recovery.own_bytes)?;
        if decoded.term != 1 || decoded.vote != Some(7) || decoded.bytes != [0x58, 0x59] {
            return Err("own checkpoint plus suffix lost the original parent state");
        }
    }
    for event in &state.evidence {
        let (derived, joint) = configuration(&event.configuration);
        if event.policy != derived || event.joint_committed != joint {
            return Err("activation context differs from its accepted/committed source log");
        }
        if event.accepted != predicate(derived, event.witnesses) {
            return Err("election/read/commit used new-only evidence before joint safety");
        }
        if event.policy == Policy::New && !event.joint_committed {
            return Err("final-only predicate preceded actual joint commitment");
        }
    }
    for event in &state.admissions {
        let actual = control(&event.parent_prefix)?;
        if actual.active != Some((event.operation, event.layer, false)) {
            return Err("overlapping layer transition lacks its authoritative parent intent");
        }
    }
    let p = &state.parent;
    if p.applied > p.committed
        || p.committed > p.accepted.len()
        || p.accepted.len() > 32
        || p.observed > p.applied
    {
        return Err("parent boundary or bounded control capacity violated");
    }
    let actual = control(&p.accepted[..p.applied])?;
    if actual.active != p.active || actual.receipts != p.receipts {
        return Err("local mutex/cache replaced replicated intent truth");
    }
    if p.committed != 0 {
        let mut witnesses = 0;
        for (i, c) in state.cells.iter().enumerate() {
            let data = decode(&c.accepted[..c.applied])?;
            if data.term == 1
                && data.vote == Some(7)
                && data.bytes == [0x58, 0x59]
                && data.parent.starts_with(&p.accepted[..p.committed])
            {
                witnesses |= 1 << i;
            }
        }
        let policy = p.accepted[..p.committed]
            .iter()
            .fold(Policy::Old, |old, e| match e {
                Entry::Joint(_) => Policy::Joint,
                Entry::Final(_) => Policy::New,
                _ => old,
            });
        if !predicate(policy, witnesses) {
            return Err("parent commit lacks distinct recoverable logical cells");
        }
    }
    Ok(())
}
fn configuration(source: &ConfigurationSource) -> (Policy, bool) {
    match source {
        ConfigurationSource::Child(records, committed) => {
            let mut policy = Policy::Old;
            let mut joint = false;
            for (i, record) in records.iter().enumerate() {
                match record {
                    Record::Joint(_) => {
                        policy = Policy::Joint;
                        joint |= i < *committed;
                    }
                    Record::Final(_) => policy = Policy::New,
                    _ => {}
                }
            }
            (policy, joint)
        }
        ConfigurationSource::Parent(entries, committed) => {
            let mut policy = Policy::Old;
            let mut joint = false;
            for (i, entry) in entries.iter().enumerate() {
                match entry {
                    Entry::Joint(_) => {
                        policy = Policy::Joint;
                        joint |= i < *committed;
                    }
                    Entry::Final(_) => policy = Policy::New,
                    _ => {}
                }
            }
            (policy, joint)
        }
    }
}
