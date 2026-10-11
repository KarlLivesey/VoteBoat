// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
use raft_reference::eraftpb::HardState as ReferenceHardState;

#[derive(Clone, Default)]
struct Image {
    entries: Vec<Entry>,
    hard: ReferenceHardState,
}
#[derive(Default)]
pub(super) struct Disk {
    written: Image,
    durable: Image,
}
pub(super) struct Held {
    pub(super) ready: Ready,
    pub(super) phase: Phase,
}
impl Replica {
    pub(super) fn write_ready(&mut self, ready: &Ready) {
        let store = self.core.raft.raft_log.store.clone();
        let mut cache = store.wl();
        cache.append(ready.entries()).unwrap();
        if let Some(first) = ready.entries().first() {
            let from = usize::try_from(first.index - 1).unwrap();
            assert!(from <= self.disk.written.entries.len());
            self.disk.written.entries.truncate(from);
            self.disk.written.entries.extend_from_slice(ready.entries());
        }
        if let Some(hard) = ready.hs() {
            cache.set_hardstate(hard.clone());
            self.disk.written.hard = hard.clone();
        }
        assert!(self.disk.written.entries.len() <= MAX_ACTIONS * 4);
    }
    pub(super) fn sync_image(&mut self) {
        if !self.bad_sync {
            self.disk.durable = self.disk.written.clone();
        }
    }
}
pub(super) fn finish(
    replica: &mut Replica,
    mut ready: Ready,
    messages: &mut VecDeque<ReferenceMessage>,
) {
    messages.extend(ready.take_messages());
    messages.extend(ready.take_persisted_messages());
    for read in ready.take_read_states() {
        let request = u64::from_le_bytes(read.request_ctx.as_slice().try_into().unwrap());
        assert!(replica.reads.insert(request, read.index).is_none());
    }
    apply(&mut replica.applied, ready.take_committed_entries());
    let mut light = replica.core.advance(ready);
    if let Some(commit) = light.commit_index() {
        replica
            .core
            .raft
            .raft_log
            .store
            .wl()
            .mut_hard_state()
            .commit = commit;
        replica.disk.written.hard.commit = commit;
        // This host durably stores LightReady commit-only state before an
        // application result can escape, matching its declared fault contract.
        replica.sync_image();
    }
    messages.extend(light.take_messages());
    apply(&mut replica.applied, light.take_committed_entries());
    replica.core.advance_apply();
}
impl Reference {
    pub(in super::super) fn hold(&mut self, id: u64) {
        self.replicas.get_mut(&id).unwrap().paused = true;
    }
    pub(in super::super) fn phase(&self, id: u64) -> Option<Phase> {
        self.replicas[&id].held.as_ref().map(|held| held.phase)
    }
    pub(in super::super) fn write_pending(&mut self, id: u64) -> Result<(), Mismatch> {
        let replica = self.replicas.get_mut(&id).unwrap();
        let mut held = replica.held.take().ok_or(Mismatch::Action(id))?;
        if held.phase != Phase::Accepted {
            replica.held = Some(held);
            return Err(Mismatch::Action(id));
        }
        replica.write_ready(&held.ready);
        held.phase = Phase::Written;
        replica.held = Some(held);
        Ok(())
    }
    pub(in super::super) fn sync(&mut self, id: u64) -> Result<(), Mismatch> {
        let replica = self.replicas.get_mut(&id).unwrap();
        let held = replica.held.as_mut().ok_or(Mismatch::Action(id))?;
        if held.phase != Phase::Written {
            return Err(Mismatch::Action(id));
        }
        held.phase = Phase::Synced;
        replica.sync_image();
        Ok(())
    }
    pub(in super::super) fn complete(&mut self, id: u64) -> Result<(), Mismatch> {
        let replica = self.replicas.get_mut(&id).unwrap();
        let held = replica.held.take().ok_or(Mismatch::Action(id))?;
        if held.phase != Phase::Synced {
            replica.held = Some(held);
            return Err(Mismatch::Action(id));
        }
        replica.paused = false;
        replica.previous = Some((replica.epoch, held.ready.number()));
        finish(replica, held.ready, &mut self.messages);
        self.ready(id);
        Ok(())
    }
    pub(in super::super) fn lie_sync(&mut self, id: u64) {
        self.replicas.get_mut(&id).unwrap().bad_sync = true;
    }
    pub(in super::super) fn restart(&mut self, id: u64, power: bool) -> Result<(), Mismatch> {
        let original = self.commands(id);
        let old = self.replicas.remove(&id).unwrap();
        let previous = old
            .held
            .as_ref()
            .map(|held| (old.epoch, held.ready.number()))
            .or(old.previous);
        let image = if power {
            old.disk.durable
        } else {
            old.disk.written
        };
        let epoch = old.epoch.checked_add(1).unwrap();
        // No shared MemStorage clone survives restart. Own selected bytes and
        // HardState build a new provider; the comparison oracle supplies nothing.
        let store = MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
        {
            let mut disk = store.wl();
            disk.append(&image.entries).unwrap();
            disk.set_hardstate(image.hard.clone());
        }
        let logger = slog::Logger::root(slog::Discard, slog::o!());
        let config = Config {
            id,
            max_uncommitted_size: 65536,
            ..Config::default()
        };
        let replica = Replica {
            core: RawNode::new(&config, store, &logger).unwrap(),
            applied: Vec::new(),
            reads: BTreeMap::new(),
            corrupted_value: false,
            disk: Disk {
                written: image.clone(),
                durable: image,
            },
            held: None,
            paused: false,
            epoch,
            previous,
            bad_sync: false,
        };
        self.replicas.insert(id, replica);
        self.ready(id);
        let restored = self.commands(id);
        if !restored.starts_with(&original) {
            let operation = original
                .iter()
                .zip(restored.iter().map(Some).chain(std::iter::repeat(None)))
                .find(|(old, new)| Some(*old) != *new)
                .unwrap()
                .0
                 .0;
            return Err(Mismatch::ReferenceRecovery(id, operation));
        }
        Ok(())
    }
    pub(in super::super) fn stale(&mut self, id: u64) -> Result<(), Mismatch> {
        let replica = &self.replicas[&id];
        let (epoch, _) = replica.previous.ok_or(Mismatch::Action(id))?;
        // The host owns Ready callbacks. RawNode numbers restart from one;
        // reject the old owner before calling any advancement on the new core.
        if epoch == replica.epoch {
            return Err(Mismatch::Action(id));
        }
        Ok(())
    }
}
