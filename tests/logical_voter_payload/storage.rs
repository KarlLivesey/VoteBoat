// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::{model::Model, types::*};
impl Model {
    pub(super) fn stage(&mut self, p: usize, weak: Weakening) -> bool {
        let Some(last) = self.admitted.get(self.applied.0.wrapping_sub(1)) else {
            return false;
        };
        if !matches!(last.kind, Kind::Anchor0 | Kind::Meta1 | Kind::Reclaim0)
            || self.matching(self.applied, weak) < 2
        {
            return false;
        }
        let store = &mut self.stores[p];
        if store.end() < self.applied.0
            || store.staged.is_some()
            || store
                .published
                .as_ref()
                .is_some_and(|c| c.at.0 >= self.applied.0)
            || store
                .anchor
                .as_ref()
                .is_some_and(|c| c.at.0 >= self.applied.0)
        {
            return false;
        }
        store.staged = Some(ChildImage {
            at: self.applied,
            image: self.image.clone(),
        });
        true
    }
    pub(super) fn publish(&mut self, p: usize) -> bool {
        let Some(image) = self.stores[p].staged.take() else {
            return false;
        };
        self.stores[p].published = Some(image);
        true
    }
    pub(super) fn anchor(&mut self, p: usize) -> bool {
        let store = &mut self.stores[p];
        let Some(image) = store.published.as_ref() else {
            return false;
        };
        if store
            .anchor
            .as_ref()
            .is_some_and(|old| old.at.0 >= image.at.0)
            || store.end() < image.at.0
        {
            return false;
        }
        let removed = image.at.0 - store.base();
        store.written.drain(..removed);
        store.synced.drain(..removed);
        store.anchor = Some(image.clone());
        true
    }
}
