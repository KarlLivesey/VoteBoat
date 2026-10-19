// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "tls")]
// One actual three-voter child cohort. Full three-leaf parent assembly is separate.
#[path = "logical_voter_native/cleanup.rs"]
mod cleanup;
#[path = "logical_voter_native/cluster.rs"]
pub mod cluster;
#[path = "logical_voter_native/drive.rs"]
mod drive;
#[path = "logical_voter_native/harness.rs"]
pub mod harness;
#[path = "logical_voter_native/history.rs"]
mod history;
#[path = "logical_voter_native/setup.rs"]
mod setup;
mod support;

#[path = "logical_voter_native/aliases.rs"]
mod aliases;
#[path = "logical_voter_native/application.rs"]
mod application;
#[path = "logical_voter_native/authority.rs"]
mod authority;
#[path = "logical_voter_native/checkpoint.rs"]
mod checkpoint;
#[path = "logical_voter_native/outbound.rs"]
mod outbound;
#[path = "logical_voter_native/parent.rs"]
mod parent;
#[path = "logical_voter_native/parent_snapshots.rs"]
mod parent_snapshots;
#[path = "logical_voter_native/persistence.rs"]
mod persistence;
#[path = "logical_voter_native/recovery.rs"]
mod recovery;
#[path = "logical_voter_native/results.rs"]
mod results;

#[path = "logical_voter_native/backing.rs"]
mod backing;

#[path = "logical_voter_native/endpoint.rs"]
mod endpoint;

#[path = "logical_voter_native/facade.rs"]
mod facade;
