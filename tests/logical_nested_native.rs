// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "tls")]
// Static3 upper logical voters over9 middle logical voters over27 physical owners.
#[path = "logical_nested_native/aliases.rs"]
mod aliases;
#[path = "logical_nested_native/application.rs"]
mod application;
#[path = "logical_nested_native/channels.rs"]
mod channels;
#[path = "logical_nested_native/cleanup.rs"]
mod cleanup;
#[path = "logical_nested_native/cluster.rs"]
mod cluster;
#[path = "logical_nested_native/cluster_inspection.rs"]
mod cluster_inspection;
#[path = "logical_nested_native/history.rs"]
mod history;
#[path = "logical_nested_native/host.rs"]
mod host;
#[path = "logical_nested_native/lower.rs"]
mod lower;
#[path = "logical_nested_native/middle.rs"]
mod middle;
#[path = "logical_nested_native/parts.rs"]
mod parts;
#[path = "support/protocol_close.rs"]
mod protocol_close;
#[path = "logical_nested_native/setup.rs"]
mod setup;
mod support;

#[path = "logical_nested_native/diagnostic.rs"]
mod diagnostic;

#[path = "logical_nested_native/observed.rs"]
mod observed;

#[path = "logical_nested_native/watchdog.rs"]
mod watchdog;

#[path = "logical_nested_native/cold_recovery.rs"]
mod cold_recovery;
#[path = "logical_nested_native/convergence.rs"]
mod convergence;
#[path = "logical_nested_native/handoff_control.rs"]
mod handoff_control;
#[path = "logical_nested_native/maintenance.rs"]
mod maintenance;
#[path = "logical_nested_native/maintenance_controls.rs"]
mod maintenance_controls;
#[path = "logical_nested_native/read.rs"]
mod read;
#[path = "logical_nested_native/read_inspection.rs"]
mod read_inspection;
#[path = "logical_nested_native/requests.rs"]
mod requests;
#[path = "logical_nested_native/shutdown.rs"]
mod shutdown;
#[path = "logical_nested_native/write.rs"]
mod write;
