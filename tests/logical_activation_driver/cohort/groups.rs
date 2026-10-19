// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Three distinct physical cohorts share one real parent bootstrap/catalog.
use super::*;
use std::collections::BTreeMap;
use voteboat::{
    log::Bootstrap,
    log_format::NativeLogCodec,
    quorum::{Limits, Policy, Tree},
    snapshot_format::NativeSnapshotCodec,
    wire_format::NativeWireCodec,
};
pub fn ids(slot: usize) -> Vec<u64> {
    match slot {
        0 => vec![1, 2, 3, 4],
        1 => vec![5, 6, 7],
        2 => vec![8, 9, 10],
        _ => panic!("bounded slot"),
    }
}
pub fn mapping(slot: usize) -> LogicalStoreMapping {
    LogicalStoreMapping::new(
        support::group(800),
        support::node(11 + slot as u64),
        support::identity(101 + slot as u128),
        support::group(801 + slot as u128),
    )
    .unwrap()
}
pub fn child(slot: usize) -> Bootstrap {
    let voters = ids(slot).into_iter().take(3).collect::<Vec<_>>();
    Bootstrap {
        group: mapping(slot).child,
        configuration: ConfigurationId::new(1).unwrap(),
        policy: Policy::new(
            Tree::Majority(
                voters
                    .iter()
                    .map(|&n| Tree::Voter(support::node(n)))
                    .collect(),
            ),
            Limits::default(),
        )
        .unwrap(),
        voter_stores: voters
            .into_iter()
            .map(|n| (support::node(n), support::identity(n.into())))
            .collect(),
    }
}
pub fn application(slot: usize) -> LogicalApplication {
    let catalog = (0..3).map(mapping).collect();
    LogicalApplication::new(
        mapping(slot),
        child(slot),
        setup::parent(),
        storage_limits(),
        NativeLogCodec,
        NativeSnapshotCodec,
        wire_codec(),
    )
    .unwrap()
    .with_activation_control_profile(fixture::app_with_mappings(catalog).control_profile())
    .unwrap()
}
pub fn certificate_slot(id: u64) -> u64 {
    (id - 1) % 3 + 1
}
pub fn stores(slot: usize) -> BTreeMap<NodeId, StoreIdentity> {
    ids(slot)
        .into_iter()
        .map(|n| (support::node(n), support::identity(n.into())))
        .collect()
}

pub fn certificate(id: u64) -> &'static [u8] {
    super::setup::certificate(id)
}

pub fn physical_log_limits() -> voteboat::log::LogLimits {
    voteboat::log::LogLimits {
        max_command_bytes: storage_limits().max_command_bytes,
        ..Default::default()
    }
}

pub fn storage_limits() -> LogicalLimits {
    let mut limits = fixture::storage_limits();
    // The budget was sized for the whole finite replacement workflow while
    // records were never removed; since LV2 consumed records are retired, so
    // it is now headroom. Kept unchanged so the workflow's limits are too.
    limits.max_actions = 128;
    limits.max_command_bytes = limits
        .parent_snapshots
        .max_file_bytes()
        .checked_add(1024)
        .unwrap();
    let ordinary = limits
        .max_retained_action_bytes
        .checked_sub(limits.control_action_bytes)
        .unwrap();
    limits.control_action_bytes = limits.max_command_bytes.checked_mul(2).unwrap();
    limits.max_retained_action_bytes = limits.control_action_bytes.checked_add(ordinary).unwrap();
    limits.validate().unwrap()
}

pub fn wire_limits() -> voteboat::wire::WireLimits {
    voteboat::wire::WireLimits {
        max_command_bytes: storage_limits().max_command_bytes,
        ..Default::default()
    }
}
pub fn wire_codec() -> NativeWireCodec {
    NativeWireCodec::with_leadership_transfer(wire_limits()).unwrap()
}

pub fn client_limits() -> ClientRouterLimits {
    ClientRouterLimits {
        command_bytes: storage_limits().max_command_bytes,
        ..Default::default()
    }
}
pub fn timers() -> TimerConfig {
    TimerConfig {
        heartbeat_ms: 250,
        election_min_ms: 3000,
        election_spread_ms: 2000,
        expirations_per_poll: 32,
    }
}
