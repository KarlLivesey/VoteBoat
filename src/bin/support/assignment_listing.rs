// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded local accepted-configuration pages; no quorum or ownership claim.
use super::setup::Service;
use std::fmt::Write;
use voteboat::identity::*;

const MAX_GROUPS: usize = 256;
const MAX_PAGE: usize = 8;
#[derive(Clone, Copy)]
struct Row {
    group: GroupIdentity,
    accepted: ConfigurationId,
    stable: ConfigurationId,
    next: Option<ConfigurationId>,
}
struct View {
    node: NodeId,
    store: StoreBinding,
    rows: Vec<Row>,
}
pub fn list(service: &Service, cursor: &str, limit: &str) -> Result<String, String> {
    let owner = &service.local().owner;
    let mut rows = Vec::new();
    for group in owner.groups_after(None).take(MAX_GROUPS + 1) {
        let core = owner.core(group).ok_or("missing assigned core")?;
        let membership = core.membership();
        rows.push(Row {
            group,
            accepted: membership.id(),
            stable: membership.stable().id(),
            next: membership.joint().map(|j| j.next.id()),
        });
    }
    let first = rows.first().ok_or("no local assignments")?;
    let node = owner
        .core(first.group)
        .ok_or("missing assigned core")?
        .local_node();
    View {
        node,
        store: owner.identity().store,
        rows,
    }
    .page(cursor, limit)
}
impl View {
    fn fingerprint(&self) -> String {
        let mut hash = ring::digest::Context::new(&ring::digest::SHA256);
        hash.update(b"voteboat-local-assignments-v1");
        hash.update(&self.node.get().to_le_bytes());
        hash.update(&self.store.identity.id.get().to_le_bytes());
        hash.update(&self.store.identity.incarnation.get().to_le_bytes());
        hash.update(&self.store.session.get().to_le_bytes());
        for row in &self.rows {
            hash.update(&row.group.id.get().to_le_bytes());
            hash.update(&row.group.incarnation.get().to_le_bytes());
            hash.update(&row.accepted.get().to_le_bytes());
            hash.update(&row.stable.get().to_le_bytes());
            hash.update(&row.next.map_or(0, ConfigurationId::get).to_le_bytes());
        }
        let mut text = String::with_capacity(64);
        for byte in hash.finish().as_ref() {
            write!(text, "{byte:02x}").unwrap();
        }
        text
    }
    fn offset(&self, cursor: &str, fingerprint: &str) -> Result<usize, String> {
        if cursor == "-" {
            return Ok(0);
        }
        let mut parts = cursor.split('/');
        let (Some(digest), Some(group), Some(incarnation), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err("invalid assignment cursor".into());
        };
        if digest != fingerprint {
            return Err("stale assignment cursor".into());
        }
        let group = GroupIdentity {
            id: group
                .parse()
                .ok()
                .and_then(GroupId::new)
                .ok_or("invalid cursor group")?,
            incarnation: incarnation
                .parse()
                .ok()
                .and_then(GroupIncarnation::new)
                .ok_or("invalid cursor incarnation")?,
        };
        self.rows
            .binary_search_by_key(&group, |r| r.group)
            .map(|offset| offset + 1)
            .map_err(|_| "cursor group is not assigned".into())
    }
    fn page(&self, cursor: &str, limit: &str) -> Result<String, String> {
        let limit: usize = limit.parse().map_err(|_| "invalid assignment limit")?;
        if !(1..=MAX_PAGE).contains(&limit) || self.rows.len() > MAX_GROUPS {
            return Err("assignment limits exceeded".into());
        }
        let fingerprint = self.fingerprint();
        let offset = self.offset(cursor, &fingerprint)?;
        let end = (offset + limit).min(self.rows.len());
        let more = end < self.rows.len();
        let next = if more {
            let last = self.rows[end - 1].group;
            format!("{fingerprint}/{}/{}", last.id.get(), last.incarnation.get())
        } else {
            "-".into()
        };
        let mut text = format!("OK evidence=local_accepted_configuration node={} store={} store_incarnation={} store_session={} groups={} rows={} more={more} next={next} assignments=",
            self.node.get(), self.store.identity.id.get(), self.store.identity.incarnation.get(), self.store.session.get(), self.rows.len(), end-offset);
        for (index, row) in self.rows[offset..end].iter().enumerate() {
            if index != 0 {
                text.push(',');
            }
            let next = row
                .next
                .map_or_else(|| "-".into(), |id| id.get().to_string());
            write!(
                text,
                "{}:{}:{}:{}:{next}",
                row.group.id.get(),
                row.group.incarnation.get(),
                row.accepted.get(),
                row.stable.get()
            )
            .unwrap();
        }
        Ok(text)
    }
}
#[cfg(test)]
#[path = "assignment_listing_tests.rs"]
mod tests;
