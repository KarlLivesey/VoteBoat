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
//! Deterministic consensus components. See the README for implemented scope.
pub mod admission;
pub mod application;
pub mod authorization;
pub mod bucket_counter;
pub mod buffer;
pub mod child_slots;
pub mod connect;
pub mod contracts;
pub mod delegation;
pub mod deletion;
pub mod dial;
pub mod directory;
pub mod discovery;
pub mod identity;
pub mod log;
pub mod membership;
#[cfg(feature = "native")]
pub mod native;
pub mod observability;
pub mod outbound;
pub mod placement;
pub mod quorum;
pub mod raft;
pub mod reparent_commit;
pub mod reparent_guard;
pub mod reparenting;
pub mod routed;
pub mod routing;
pub mod runtime;
pub mod scope;
pub mod scoped_source;
pub mod secure;
pub mod snapshot;
pub mod snapshot_worker;
pub mod transfer;
pub mod transport;
pub mod vote;
pub mod wire;
pub mod worker;

pub mod retirement;
pub mod transfer_publication;
pub mod transfer_source;
pub mod transfer_target;

pub mod group_creation;
pub mod namespace_creation;
