// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "tls")]
//! Actual nine original physical owners and three genuine learner backing owners.
#[path = "logical_activation_application/application.rs"]
mod application;
#[path = "logical_activation_nested/assembly.rs"]
mod assembly;
#[path = "logical_activation_nested/business.rs"]
mod business;
#[path = "logical_activation_nested/channels.rs"]
mod channels;
#[path = "logical_nested_native/cleanup.rs"]
mod cleanup;
#[path = "logical_activation_nested/cluster.rs"]
mod cluster;
#[path = "logical_nested_native/cluster_inspection.rs"]
mod cluster_inspection;
#[path = "logical_activation_nested/cold_joint.rs"]
mod cold_joint;
#[path = "logical_activation_nested/cold_target.rs"]
mod cold_target;
#[path = "logical_activation_nested/controls.rs"]
mod controls;
#[path = "logical_activation_nested/diagnostic.rs"]
mod diagnostic;
#[path = "logical_activation_nested/enrollment.rs"]
mod enrollment;
#[path = "logical_activation_nested/history.rs"]
mod history;
#[path = "logical_activation_nested/lower.rs"]
mod lower;
#[path = "logical_activation_nested/parts.rs"]
mod parts;
#[path = "logical_activation_nested/physical.rs"]
mod physical;
#[path = "logical_activation_nested/profile.rs"]
mod profile;
#[path = "logical_activation_nested/progress.rs"]
mod progress;
#[path = "logical_activation_nested/readiness.rs"]
mod readiness;
#[path = "logical_activation_nested/repeated.rs"]
mod repeated;
#[path = "logical_activation_nested/setup.rs"]
mod setup;
#[path = "logical_activation_nested/shutdown.rs"]
mod shutdown;
pub mod support;
#[path = "logical_activation_nested/uncommitted_target.rs"]
mod uncommitted_target;
#[path = "logical_activation_nested/unknown_target.rs"]
mod unknown_target;
use voteboat::{logical_voter::*, native::runtime::*};
type Middle = LogicalNode<FairScheduler, DeadlineQueue, JitterEntropy, LogicalApplication>;
type Top = LogicalNode<
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    LogicalActivationApplication<application::App>,
>;
type UpperBridge<'a> = LogicalBackingBridge<'a, Middle>;
