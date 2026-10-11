// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::types::*;
pub fn view(image: &Image) -> Option<ParentBytes> {
    let mut entries = image.entries;
    if image.base.0 > 0 {
        let checkpoint = image.snapshots[image.anchor?].as_ref()?.bytes.as_ref()?;
        for (slot, entry) in entries.iter_mut().enumerate().take(image.base.0 as usize) {
            *entry = checkpoint.entries[slot];
        }
    }
    for entry in entries.iter().take(image.commit.0 as usize) {
        entry.as_ref()?.bytes?;
    }
    Some(ParentBytes {
        at: image.commit,
        entries,
        term: image.term,
        vote: image.vote,
        configuration: image.configuration,
        epoch: image.epoch,
        generation: image.generation,
        dedup: image.dedup,
    })
}
fn snapshot(image: &Image, slot: usize) -> Option<&Snapshot> {
    image.snapshots[slot].as_ref()
}
fn anchor_ready(image: &Image, slot: usize, weak: Weakening) -> bool {
    snapshot(image, slot)
        .is_some_and(|s| weak == Weakening::EarlyAnchor || (s.published && s.bytes.is_some()))
}
pub fn allowed(image: &Image, kind: Kind, weak: Weakening) -> bool {
    use Kind::*;
    match kind {
        Claim1 => image.epoch == 0,
        Promise => image.epoch == 1 && image.term == 0,
        AppendX => image.term == 1 && image.entries[0].is_none() && image.base.0 == 0,
        Commit1 => {
            image.entries[0].is_some_and(|e| weak == Weakening::MissingPayload || e.bytes.is_some())
        }
        Meta0 => image.commit.0 == 1 && image.snapshots[0].is_none(),
        Bytes0 => snapshot(image, 0).is_some_and(|s| s.bytes.is_none()) && view(image).is_some(),
        Publish0 => snapshot(image, 0).is_some_and(|s| s.bytes.is_some() && !s.published),
        Anchor0 => anchor_ready(image, 0, weak),
        Tentative => image.base.0 == 1 && image.entries[1].is_none(),
        Replace => image.commit.0 == 1 && image.entries[1].is_some() && image.generation == 0,
        Commit2 => image.entries[1].is_some_and(|e| e.tag == Tag::Y && e.bytes.is_some()),
        Meta1 => image.commit.0 == 2 && image.snapshots[1].is_none(),
        Bytes1 => snapshot(image, 1).is_some_and(|s| s.bytes.is_none()) && view(image).is_some(),
        Publish1 => snapshot(image, 1).is_some_and(|s| s.bytes.is_some() && !s.published),
        Anchor1 => anchor_ready(image, 1, weak),
        Reclaim0 => image.anchor == Some(1) && image.pins[0],
        Claim2 => image.epoch == 1 && image.anchor == Some(1),
        Serve2 => image.epoch == 2 && view(image).is_some(),
        FillX => {
            image.commit.0 == 0
                && image.entries[0].is_some_and(|e| e.tag == Tag::X && e.bytes.is_none())
        }
    }
}
pub fn update(image: &mut Image, record: &Record, weak: Weakening) -> bool {
    let h = record.header;
    if h.epoch != image.epoch
        || h.generation != image.generation
        || h.revision != image.revision
        || image.used & (1 << h.operation) != 0
        || !allowed(image, record.kind, weak)
    {
        return false;
    }
    mutate(image, record);
    image.revision += 1;
    image.used |= 1 << h.operation;
    image.receipts.push(Completion {
        header: h,
        position: ChildIndex(image.revision as usize),
    });
    true
}
fn mutate(image: &mut Image, record: &Record) {
    use Kind::*;
    match record.kind {
        Claim1 => image.epoch = 1,
        Promise => {
            image.term = 1;
            image.vote = Some(1);
        }
        AppendX => {
            image.entries[0] = Some(Entry {
                term: 1,
                tag: Tag::X,
                bytes: record.body,
            })
        }
        Commit1 => {
            image.commit = ParentIndex(1);
            image.dedup |= 1;
        }
        Meta0 | Meta1 => {
            let slot = usize::from(record.kind == Meta1);
            image.snapshots[slot] = Some(Snapshot {
                at: image.commit,
                bytes: None,
                published: false,
            });
        }
        Bytes0 | Bytes1 => {
            let slot = usize::from(record.kind == Bytes1);
            let bytes = view(image).unwrap();
            image.snapshots[slot].as_mut().unwrap().bytes = Some(bytes);
            image.pins[slot] = true;
        }
        Publish0 | Publish1 => {
            let slot = usize::from(record.kind == Publish1);
            image.snapshots[slot].as_mut().unwrap().published = true;
        }
        Anchor0 | Anchor1 => {
            let slot = usize::from(record.kind == Anchor1);
            let at = image.snapshots[slot].as_ref().unwrap().at;
            image.anchor = Some(slot);
            image.base = at;
            for entry in image.entries.iter_mut().take(at.0 as usize) {
                *entry = None;
            }
        }
        Tentative => image.entries[1] = Some(Entry::exact(Tag::X)),
        Replace => {
            image.generation = 1;
            image.entries[1] = Some(Entry::exact(Tag::Y));
        }
        Commit2 => {
            image.commit = ParentIndex(2);
            image.dedup |= 2;
        }
        Reclaim0 => {
            image.pins[0] = false;
            image.snapshots[0] = None;
        }
        Claim2 => image.epoch = 2,
        Serve2 => {}
        FillX => image.entries[0].as_mut().unwrap().bytes = record.body,
    }
}
/// Recovery decoder used by model, deliberately not imported by the oracle.
pub fn decode(store: &Physical, boundary: ChildIndex, weak: Weakening) -> Option<Image> {
    if store.base() > boundary.0 || store.end() < boundary.0 {
        return None;
    }
    let mut image = store
        .anchor
        .as_ref()
        .map_or_else(Image::default, |c| c.image.clone());
    for record in store.synced.iter().take(boundary.0 - store.base()) {
        if !update(&mut image, record, weak) {
            return None;
        }
    }
    Some(image)
}
