// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit volatile host backlog profile; never changes consensus batch limits.
use super::*;
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) enum CapacityProfile {
    #[default]
    Legacy,
    Offered4096,
}
impl CapacityProfile {
    pub(super) fn extract(args: &mut Vec<String>) -> Result<Self, Failure> {
        let Some(position) = args.iter().position(|arg| arg == "--offered-capacity") else {
            return Ok(Self::Legacy);
        };
        if args
            .iter()
            .skip(position + 1)
            .any(|arg| arg == "--offered-capacity")
        {
            return Err("duplicate --offered-capacity".into());
        }
        if args.get(position + 1).map(String::as_str) != Some("4096") {
            return Err("--offered-capacity requires 4096".into());
        }
        args.drain(position..position + 2);
        Ok(Self::Offered4096)
    }
    pub(super) fn validate(
        self,
        offered: bool,
        window: usize,
        ready: usize,
    ) -> Result<(), Failure> {
        let max = match self {
            Self::Legacy => self.clients(ready).group_requests,
            Self::Offered4096 if offered => 4096,
            Self::Offered4096 => return Err("--offered-capacity requires offered mode".into()),
        };
        if !(1..=max).contains(&window) {
            return Err("window exceeds selected offered capacity".into());
        }
        Ok(())
    }
    pub(super) fn validate_workload(
        self,
        count: usize,
        offered: bool,
        window: usize,
        ready: usize,
    ) -> Result<(), Failure> {
        if !(1..=50000).contains(&count) {
            return Err("count/window out of bounds".into());
        }
        self.validate(offered, window, ready)
    }
    pub(super) fn shard(self, ready: usize, groups: usize) -> ShardLimits {
        let mut limits = ShardLimits {
            max_groups: groups,
            visit_items: ready,
            ..Default::default()
        };
        if self == Self::Offered4096 {
            limits.group_items = 4112;
        }
        // Same rule as native startup: a group's queue holds every admitted
        // client original beside its control reserve.
        limits.group_items = limits
            .group_items
            .max(self.clients(ready).group_requests + limits.group_control_items);
        limits
    }
    /// Legacy mirrors native service startup: four visits per group, never
    /// below the router default, within the router-wide request ceiling.
    pub(super) fn clients(self, ready: usize) -> ClientRouterLimits {
        let mut limits = ClientRouterLimits::default();
        limits.group_requests = if self == Self::Offered4096 {
            4096
        } else {
            ready
                .saturating_mul(4)
                .clamp(limits.group_requests, limits.requests)
        };
        limits
    }
    pub(super) fn readback(
        self,
        shard: ShardLimits,
        clients: ClientRouterLimits,
        ready: usize,
        groups: usize,
    ) -> Result<(), Failure> {
        let expected_shard = self.shard(ready, groups);
        let expected_clients = self.clients(ready);
        // Compare every selected/independent admission bound before workers exist.
        if format!("{shard:?}") != format!("{expected_shard:?}")
            || format!("{clients:?}") != format!("{expected_clients:?}")
        {
            return Err("offered capacity effective limits mismatch".into());
        }
        Ok(())
    }
    pub(super) fn settings(self) -> String {
        if self == Self::Legacy {
            return String::new();
        }
        let s = self.shard(16, 1);
        let c = self.clients(16);
        format!(" offered_capacity_requested=4096 offered_capacity_effective=4096 client_requests={} client_group_requests={} client_bytes={} client_receipt_bytes={} shard_max_items={} shard_control_items={} shard_group_items={} shard_group_control_items={} shard_group_bytes={}", c.requests, c.group_requests, c.bytes, c.receipt_bytes, s.max_items, s.control_items, s.group_items, s.group_control_items, s.group_bytes)
    }
}
#[cfg(test)]
#[path = "offered_capacity/tests.rs"]
mod tests;
