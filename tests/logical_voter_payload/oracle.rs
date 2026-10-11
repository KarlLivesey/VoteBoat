// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Independent witness/history decoder; does not call model update/view/decode.
use super::{model::Model, types::*};
#[derive(Clone, Debug, Eq, PartialEq)]
struct Decoded {
    entries: [Option<Entry>; 2],
    snapshots: [Option<(u8, [Option<Entry>; 2])>; 2],
    commit: u8,
    base: u8,
    anchor: Option<usize>,
    term: u8,
    vote: Option<u8>,
    configuration: u8,
    epoch: u8,
    generation: u8,
    dedup: u8,
}
fn raw(image: &Image) -> Decoded {
    Decoded {
        entries: image.entries,
        snapshots: std::array::from_fn(|slot| {
            image.snapshots[slot]
                .as_ref()
                .and_then(|s| s.bytes.as_ref())
                .filter(|b| b.at == image.snapshots[slot].as_ref().unwrap().at)
                .map(|b| (b.at.0, b.entries))
        }),
        commit: image.commit.0,
        base: image.base.0,
        anchor: image.anchor,
        term: image.term,
        vote: image.vote,
        configuration: image.configuration,
        epoch: image.epoch,
        generation: image.generation,
        dedup: image.dedup,
    }
}
fn values(decoded: &Decoded) -> Option<[Option<Entry>; 2]> {
    let mut entries = decoded.entries;
    if decoded.base > 0 {
        let (boundary, snapshot) = decoded.snapshots[decoded.anchor?]?;
        if boundary != decoded.base {
            return None;
        }
        let prefix = decoded.base as usize;
        entries[..prefix].copy_from_slice(&snapshot[..prefix]);
    }
    Some(entries)
}
fn apply(decoded: &mut Decoded, record: &Record) -> Option<()> {
    use Kind::*;
    match record.kind {
        Claim1 => decoded.epoch = 1,
        Promise => {
            decoded.term = 1;
            decoded.vote = Some(1);
        }
        AppendX => {
            decoded.entries[0] = Some(Entry {
                term: 1,
                tag: Tag::X,
                bytes: record.body,
            })
        }
        Commit1 => {
            if decoded.commit != 0 {
                return None;
            }
            decoded.commit = 1;
            decoded.dedup |= 1;
        }
        Meta0 => decoded.snapshots[0] = None,
        Meta1 => decoded.snapshots[1] = None,
        Bytes0 | Bytes1 => {
            let slot = usize::from(record.kind == Bytes1);
            decoded.snapshots[slot] = Some((decoded.commit, values(decoded)?));
        }
        Publish0 | Publish1 => {}
        Anchor0 | Anchor1 => {
            let slot = usize::from(record.kind == Anchor1);
            decoded.anchor = Some(slot);
            decoded.base = if slot == 0 { 1 } else { 2 };
            for entry in decoded.entries.iter_mut().take(decoded.base as usize) {
                *entry = None;
            }
        }
        Tentative => {
            decoded.entries[1] = Some(Entry {
                term: 1,
                tag: Tag::X,
                bytes: Some([88, 0, 1]),
            })
        }
        Replace => {
            decoded.generation = 1;
            decoded.entries[1] = Some(Entry {
                term: 1,
                tag: Tag::Y,
                bytes: Some([89, 0, 2]),
            });
        }
        Commit2 => {
            if decoded.commit != 1 {
                return None;
            }
            decoded.commit = 2;
            decoded.dedup |= 2;
        }
        Reclaim0 => decoded.snapshots[0] = None,
        Claim2 => decoded.epoch = 2,
        Serve2 => {}
        FillX => decoded.entries[0].as_mut()?.bytes = record.body,
    }
    Some(())
}
fn decode(store: &Physical, at: ChildIndex) -> Option<Decoded> {
    let base = store.anchor.as_ref().map_or(0, |c| c.at.0);
    if base > at.0 || base + store.synced.len() < at.0 {
        return None;
    }
    let mut decoded = store
        .anchor
        .as_ref()
        .map_or_else(|| raw(&Image::default()), |c| raw(&c.image));
    for record in store.synced.iter().take(at.0 - base) {
        apply(&mut decoded, record)?;
    }
    Some(decoded)
}
fn exact(entry: Option<Entry>, slot: usize) -> bool {
    entry.is_some_and(|e| {
        e.term == 1
            && e.tag == if slot == 0 { Tag::X } else { Tag::Y }
            && e.bytes == Some(if slot == 0 { [88, 0, 1] } else { [89, 0, 2] })
    })
}
pub fn check(model: &Model) -> Result<(), String> {
    if model.other[0].logical == model.other[1].logical {
        return Err("logical witnesses counted twice".into());
    }
    for cell in &model.other {
        let witnesses = cell
            .synced
            .iter()
            .filter(|s| s.len() == 2 && exact(Some(s[0]), 0) && exact(Some(s[1]), 1))
            .count();
        if cell.applied.0 > cell.commit.0 || cell.commit.0 > 0 && witnesses < 2 {
            return Err("unmatched backing-child parent witness".into());
        }
    }
    if model.image.term == 1 && model.image.vote != Some(1) {
        return Err("replacement lost original vote".into());
    }
    for slot in 0..2 {
        if model.acknowledged[slot] {
            let witnesses = model
                .stores
                .iter()
                .filter_map(|s| decode(s, model.applied))
                .filter_map(|d| values(&d))
                .filter(|entries| exact(entries[slot], slot))
                .count();
            if witnesses < 2 {
                return Err(format!("acknowledged parent{} has only{witnesses} recoverable exact-byte child witnesses",slot+1));
            }
        }
    }
    if let Some(slot) = model.image.anchor {
        let snapshot = model.image.snapshots[slot]
            .as_ref()
            .ok_or("anchor points at reclaimed slot")?;
        if !snapshot.published || snapshot.bytes.is_none() || !model.image.pins[slot] {
            return Err("parent anchor has no full published child-backed checkpoint pin".into());
        }
    }
    for (physical, recovered) in model.recovered.iter().enumerate() {
        let Some(recovered) = recovered else {
            continue;
        };
        let original = decode(&model.stores[physical], model.applied)
            .ok_or("own synchronized image missing")?;
        if recovered.at.0 != original.commit
            || recovered.entries
                != values(&original).ok_or("own selected parent checkpoint absent")?
            || recovered.term != original.term
            || recovered.vote != original.vote
            || recovered.configuration != original.configuration
            || recovered.epoch != original.epoch
            || recovered.generation != original.generation
            || recovered.dedup != original.dedup
        {
            return Err(
                "recovered image differs from own exact synchronized checkpoint and suffix".into(),
            );
        }
        for slot in 0..2 {
            if model.acknowledged[slot] && !exact(recovered.entries[slot], slot) {
                return Err("own recovered image lost acknowledged bytes".into());
            }
        }
    }

    Ok(())
}
pub fn own_bytes(store: &Physical, at: ChildIndex) -> Option<[Option<[u8; 3]>; 2]> {
    let decoded = decode(store, at)?;
    let entries = values(&decoded)?;
    Some(entries.map(|entry| entry.and_then(|e| e.bytes)))
}
