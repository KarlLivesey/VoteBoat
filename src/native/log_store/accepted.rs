// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Private admitted overlays. Their metadata is never durable voter evidence.
use super::*;
use crate::log::transition::{
    CompactDelta, LogMetadata, LogSummary, LogView, NextLog, OwnedAppend, OwnedChange,
    OwnedNextLog, PlainDelta,
};

#[derive(Default)]
pub(super) struct Accepted {
    overlays: BTreeMap<GroupIdentity, Overlay>,
    summaries: BTreeMap<GroupIdentity, LogSummary>,
}
enum Overlay {
    Plain(Box<Plain>),
    Full(Box<Full>),
}
struct Plain {
    base: LogMetadata,
    metadata: LogMetadata,
    summary: LogSummary,
    tail: Vec<LogEntry>,
    removed: usize,
}
struct Full {
    state: GroupLog,
    summary: LogSummary,
}
pub(super) enum Plan {
    Create(Box<GroupLog>),
    Update(Box<OwnedNextLog>),
}
impl Plan {
    pub(super) fn metadata(&self) -> LogMetadata {
        match self {
            Self::Create(state) => LogMetadata::from_log(state),
            Self::Update(next) => next.metadata(),
        }
    }
}
impl Accepted {
    pub(super) fn recovered(durable: &BTreeMap<GroupIdentity, GroupLog>) -> Self {
        Self {
            overlays: BTreeMap::new(),
            summaries: durable
                .iter()
                .map(|(group, state)| (*group, LogSummary::from_validated(state)))
                .collect(),
        }
    }
    pub(super) fn is_clean(&self) -> bool {
        self.overlays.is_empty()
    }

    fn view<'a>(
        &'a self,
        durable: &'a BTreeMap<GroupIdentity, GroupLog>,
        group: GroupIdentity,
    ) -> Result<(LogView<'a>, LogSummary), StorageError> {
        match self.overlays.get(&group) {
            Some(Overlay::Full(full)) => {
                if !full.summary.matches(&full.state) {
                    return Err(StorageError::Rejected("stale log summary"));
                }
                Ok((LogView::new(&full.state), full.summary))
            }
            Some(Overlay::Plain(plain)) => {
                let base = self.clean(durable, group)?;
                if LogMetadata::from_log(base) != plain.base
                    || !plain.summary.matches_metadata(&plain.metadata)
                {
                    return Err(StorageError::Rejected("stale log summary"));
                }
                Ok((
                    LogView::trimmed_overlay(base, &plain.tail, plain.metadata, plain.removed),
                    plain.summary,
                ))
            }
            None => {
                let state = self.clean(durable, group)?;
                Ok((LogView::new(state), self.summaries[&group]))
            }
        }
    }
    fn clean<'a>(
        &self,
        durable: &'a BTreeMap<GroupIdentity, GroupLog>,
        group: GroupIdentity,
    ) -> Result<&'a GroupLog, StorageError> {
        let state = durable
            .get(&group)
            .ok_or(StorageError::Rejected("group not bootstrapped"))?;
        if self
            .summaries
            .get(&group)
            .is_none_or(|summary| !summary.matches(state))
        {
            return Err(StorageError::Rejected("stale log summary"));
        }
        Ok(state)
    }
    pub(super) fn plan_batch(
        &self,
        durable: &BTreeMap<GroupIdentity, GroupLog>,
        mutations: Vec<LogMutation>,
        limits: LogLimits,
    ) -> Result<BTreeMap<GroupIdentity, Plan>, StorageError> {
        let limits = limits.validate()?;
        if mutations.is_empty() || mutations.len() > limits.max_batch_units {
            return Err(StorageError::Rejected("batch unit limit"));
        }
        let mut plans = BTreeMap::new();
        for mutation in mutations {
            let group = mutation_group(&mutation);
            if plans.contains_key(&group) {
                return Err(StorageError::Rejected(
                    "one atomic unit per group per batch",
                ));
            }
            let plan = match mutation {
                LogMutation::Create(bootstrap) => {
                    self.plan_create(durable, &plans, &bootstrap, limits)?
                }
                LogMutation::Update(update) => {
                    let (view, summary) = self.view(durable, group)?;
                    Plan::Update(Box::new(OwnedNextLog::plan_owned(
                        view, &summary, update, limits,
                    )?))
                }
            };
            plans.insert(group, plan);
        }
        Ok(plans)
    }
    fn plan_create(
        &self,
        durable: &BTreeMap<GroupIdentity, GroupLog>,
        plans: &BTreeMap<GroupIdentity, Plan>,
        bootstrap: &Bootstrap,
        limits: LogLimits,
    ) -> Result<Plan, StorageError> {
        let created = plans
            .values()
            .filter(|plan| matches!(plan, Plan::Create(_)))
            .count();
        let accepted_groups = durable.len()
            + self
                .overlays
                .keys()
                .filter(|group| !durable.contains_key(group))
                .count();
        if accepted_groups + created >= limits.max_groups
            || durable
                .keys()
                .chain(self.overlays.keys())
                .chain(plans.keys())
                .any(|group| group.id == bootstrap.group.id)
        {
            return Err(StorageError::Rejected(
                "existing group identity or group limit",
            ));
        }
        let mut created = stage_batch(
            &BTreeMap::new(),
            &[LogMutation::Create(bootstrap.clone())],
            limits,
        )?;
        Ok(Plan::Create(Box::new(
            created.remove(&bootstrap.group).unwrap(),
        )))
    }
    pub(super) fn install(
        &mut self,
        durable: &BTreeMap<GroupIdentity, GroupLog>,
        plans: BTreeMap<GroupIdentity, Plan>,
    ) -> Result<(), StorageError> {
        for (group, plan) in plans {
            match plan {
                Plan::Create(state) => self.put_full(group, *state),
                Plan::Update(next) => {
                    let current = self.view(durable, group)?.0.metadata();
                    match next.into_change() {
                        OwnedChange::Append(append) => {
                            append.check_install(current)?;
                            self.install_owned_append(group, append)?;
                        }
                        OwnedChange::Other { next, update } => {
                            self.install_update(durable, group, next, update)?
                        }
                    }
                }
            }
        }
        Ok(())
    }
    fn install_owned_append(
        &mut self,
        group: GroupIdentity,
        append: OwnedAppend,
    ) -> Result<(), StorageError> {
        let metadata = append.metadata();
        let summary = append.summary();
        let base = append.predecessor();
        match self.overlays.get_mut(&group) {
            Some(Overlay::Full(full)) => {
                append.check_install(LogMetadata::from_log(&full.state))?;
                full.summary = append.install(&mut full.state);
            }
            Some(Overlay::Plain(plain)) => {
                if let Some(suffix) = append.into_suffix() {
                    plain.tail.extend(suffix.entries);
                }
                plain.metadata = metadata;
                plain.summary = summary;
            }
            None => {
                self.overlays.insert(
                    group,
                    Overlay::Plain(Box::new(Plain {
                        base,
                        metadata,
                        summary,
                        tail: append
                            .into_suffix()
                            .map_or_else(Vec::new, |suffix| suffix.entries),
                        removed: 0,
                    })),
                );
            }
        }
        Ok(())
    }
    fn put_full(&mut self, group: GroupIdentity, state: GroupLog) {
        let summary = LogSummary::from_validated(&state);
        self.overlays
            .insert(group, Overlay::Full(Box::new(Full { state, summary })));
    }
    fn install_update(
        &mut self,
        durable: &BTreeMap<GroupIdentity, GroupLog>,
        group: GroupIdentity,
        next: NextLog,
        update: LogUpdate,
    ) -> Result<(), StorageError> {
        let current = self.view(durable, group)?.0.metadata();
        match next {
            NextLog::Full {
                predecessor,
                next,
                update: original,
            } => {
                if current != predecessor || update != original {
                    return Err(StorageError::Rejected("accepted predecessor mismatch"));
                }
                self.put_full(group, *next);
            }
            NextLog::Delta(delta) => {
                delta.check_install(current, &update)?;
                self.install_delta(group, delta, update)?;
            }
            NextLog::Compact(delta) => {
                delta.check_install(current, &update)?;
                self.install_compact(durable, group, delta, update)?;
            }
        }
        Ok(())
    }
    fn install_delta(
        &mut self,
        group: GroupIdentity,
        delta: PlainDelta,
        update: LogUpdate,
    ) -> Result<(), StorageError> {
        let metadata = delta.metadata();
        match self.overlays.get_mut(&group) {
            Some(Overlay::Full(full)) => {
                full.summary = NextLog::Delta(delta).install(&mut full.state, update)?;
            }
            Some(Overlay::Plain(plain)) => {
                if let Some(suffix) = delta.append {
                    plain.tail.extend(suffix.entries);
                }
                plain.metadata = metadata;
                plain.summary = delta.summary;
            }
            None => {
                self.overlays.insert(
                    group,
                    Overlay::Plain(Box::new(Plain {
                        base: delta.predecessor,
                        metadata,
                        summary: delta.summary,
                        tail: delta.append.map_or_else(Vec::new, |suffix| suffix.entries),
                        removed: 0,
                    })),
                );
            }
        }
        Ok(())
    }
    fn install_compact(
        &mut self,
        durable: &BTreeMap<GroupIdentity, GroupLog>,
        group: GroupIdentity,
        delta: CompactDelta,
        update: LogUpdate,
    ) -> Result<(), StorageError> {
        let metadata = delta.metadata();
        match self.overlays.get_mut(&group) {
            Some(Overlay::Full(full)) => {
                full.summary = NextLog::Compact(delta).install(&mut full.state, update)?;
            }
            Some(Overlay::Plain(plain)) => {
                let durable_removed = delta
                    .removed
                    .min(durable[&group].entries.len() - plain.removed);
                plain.removed += durable_removed;
                plain.tail.drain(..delta.removed - durable_removed);
                plain.metadata = metadata;
                plain.summary = delta.summary;
            }
            None => {
                self.overlays.insert(
                    group,
                    Overlay::Plain(Box::new(Plain {
                        base: LogMetadata::from_log(&durable[&group]),
                        metadata,
                        summary: delta.summary,
                        tail: Vec::new(),
                        removed: delta.removed,
                    })),
                );
            }
        }
        Ok(())
    }
    pub(super) fn check_promotion(
        &self,
        durable: &BTreeMap<GroupIdentity, GroupLog>,
    ) -> Result<(), StorageError> {
        for (&group, overlay) in &self.overlays {
            let (view, summary) = self.view(durable, group)?;
            if !summary.matches_metadata(&view.metadata()) {
                return Err(StorageError::Rejected("stale log summary"));
            }
            if let Overlay::Plain(plain) = overlay {
                let base = &durable[&group];
                if base
                    .entries
                    .len()
                    .checked_sub(plain.removed)
                    .and_then(|left| left.checked_add(plain.tail.len()))
                    != Some(plain.metadata.entries_len)
                {
                    return Err(StorageError::Rejected("accepted tail length mismatch"));
                }
            }
        }
        Ok(())
    }
    pub(super) fn promote(
        &mut self,
        durable: &mut BTreeMap<GroupIdentity, GroupLog>,
    ) -> Result<(), StorageError> {
        self.check_promotion(durable)?;
        for (group, overlay) in std::mem::take(&mut self.overlays) {
            let summary = match overlay {
                Overlay::Full(full) => {
                    durable.insert(group, full.state);
                    full.summary
                }
                Overlay::Plain(plain) => {
                    let state = durable.get_mut(&group).unwrap();
                    state.entries.drain(..plain.removed);
                    state.entries.extend(plain.tail);
                    plain.metadata.install_header(state);
                    if LogMetadata::from_log(state) != plain.metadata {
                        return Err(StorageError::Rejected("promoted log metadata mismatch"));
                    }
                    plain.summary
                }
            };
            self.summaries.insert(group, summary);
        }
        Ok(())
    }
}
pub(super) fn mutation_group(mutation: &LogMutation) -> GroupIdentity {
    match mutation {
        LogMutation::Create(bootstrap) => bootstrap.group,
        LogMutation::Update(update) => update.group,
    }
}

#[cfg(test)]
mod tests;
