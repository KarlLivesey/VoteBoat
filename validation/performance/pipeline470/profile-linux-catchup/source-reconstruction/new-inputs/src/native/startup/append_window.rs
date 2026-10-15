// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Explicit startup capability checks before files, sockets or workers exist.
use super::*;

impl NativeStartup {
    pub(super) fn validate_append_application<A: ProposalAdmission>(
        &self,
        application: &A,
    ) -> Result<(), NativeStartupError>
    where
        A::Receipt: ApplicationReceipt,
    {
        if self.append_window.is_some() && !application.supports_append_window() {
            return Err(error(
                "append window",
                "application does not support successive append admission",
            ));
        }
        Ok(())
    }

    pub(super) fn validate_append_limits(
        &self,
        log: LogLimits,
        worker: WorkerLimits,
    ) -> Result<(), NativeStartupError> {
        let Some(window) = self.append_window else {
            return Ok(());
        };
        checked(window.validate(log, crate::raft::BatchLimits::default()))?;
        let shard = startup_shard_limits(log, 1, self.ready_visit_items)?;
        let clients = ClientRouterLimits::default();
        // The full request/original window must fit ordinary credits. Control
        // reserves remain untouched; aggregate dynamic bytes are checked before
        // the owner stages its first unit, using the actual recovered Core.
        if window.max_units > worker.max_requests - worker.control_requests
            || window.max_units > worker.max_units - worker.control_units
            || window.max_units > log.max_pending_units
            || window.max_units > self.limits.replica.leases
            || window.max_originals > clients.group_requests
            || window.max_originals > shard.group_items - shard.group_control_items
            || window.max_originals > shard.max_items - shard.control_items
        {
            return Err(error(
                "append window",
                "window exceeds selected ordinary resource capacity",
            ));
        }
        self.validate_append_minimum(log, shard)?;
        Ok(())
    }

    /// Pure minimum-envelope check. This temporary initial-bootstrap Core is
    /// used only for resource arithmetic; it is never a recovered local voter,
    /// submitted request or durability/authority assertion. Actual recovered
    /// ownership is checked again before workers start.
    fn validate_append_minimum(
        &self,
        log: LogLimits,
        shard: ShardLimits,
    ) -> Result<(), NativeStartupError> {
        let mut groups = BTreeMap::new();
        checked(crate::log::apply_batch(
            &mut groups,
            &[LogMutation::Create(self.bootstrap.clone())],
            log,
        ))?;
        let (&node, &identity) = self
            .bootstrap
            .voter_stores
            .iter()
            .next()
            .ok_or_else(|| error("append window", "missing initial voter"))?;
        let state = groups
            .remove(&self.bootstrap.group)
            .ok_or_else(|| error("append window", "missing initial state"))?;
        let mut minimum = checked(crate::raft::Raft::recover(
            node,
            StoreBinding {
                identity,
                session: StoreSession::new(1).unwrap(),
            },
            state,
            log,
        ))?;
        checked(minimum.configure_append_window(self.append_window))?;
        let envelope = owner_limits::startup_owner_limits(
            std::slice::from_ref(&minimum),
            shard.max_event_bytes,
            log,
        )?;
        let required =
            crate::runtime::append_window_effect_reservation(&minimum, shard.max_event_bytes)
                .ok_or_else(|| error("append window", "minimum output reservation overflow"))?;
        if required > envelope.reserved_bytes - envelope.control_bytes
            || self.append_window.unwrap().max_units
                > envelope.active_visits - envelope.control_visits
        {
            return Err(error(
                "append window",
                "minimum window exceeds ordinary owner capacity",
            ));
        }
        Ok(())
    }
}
