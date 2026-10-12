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
#![cfg(feature = "tls")]
// Real child provenance with independently controlled host-channel I/O.
#[path = "logical_voter_network/aliases.rs"]
mod aliases;
#[path = "logical_voter_network/application.rs"]
mod application;
#[path = "logical_voter_network/claim.rs"]
mod claim;
#[path = "logical_voter_network/cleanup.rs"]
mod cleanup;
#[path = "logical_voter_network/cluster.rs"]
mod cluster;
#[path = "logical_network/controls.rs"]
mod controls;
#[path = "logical_network/fixture.rs"]
mod fixture;
#[path = "logical_network/host.rs"]
mod host;
#[path = "logical_voter_network/parent.rs"]
mod parent;
#[path = "logical_voter_native/parent_snapshots.rs"]
mod parent_snapshots;
#[path = "logical_network/refusals.rs"]
mod refusals;
#[path = "logical_voter_network/setup.rs"]
mod setup;
mod support;
