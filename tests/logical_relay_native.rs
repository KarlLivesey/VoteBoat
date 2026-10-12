// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "tls")]
//! Actual native relay-backed physical cell consumed by the logical bridge.
#[path = "logical_activation_application/application.rs"]
mod application;
#[path = "native_relay/channels.rs"]
mod channels;
#[path = "logical_relay_native/cluster.rs"]
mod cluster;
#[path = "logical_relay_native/history.rs"]
mod history;
#[path = "logical_activation_nested/parts.rs"]
mod parts;
#[path = "logical_relay_native/setup.rs"]
mod setup;
pub mod support;
use voteboat::{
    logical_voter::*,
    native::{relay::NativeRelayNetwork, runtime::*},
    secure::SecureSession,
};
type Network = NativeRelayNetwork<Box<dyn SecureSession>, voteboat::native::wire::NativeWireCodec>;
type Boat = voteboat::native::node::NativeNetworkNode<LogicalApplication, Network>;
type Bridge<'a> = LogicalBackingBridge<'a, Boat>;
type Facade = LogicalNode<FairScheduler, DeadlineQueue, JitterEntropy, application::App>;
