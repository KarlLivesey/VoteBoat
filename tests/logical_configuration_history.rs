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
//! Public retained inspection contract. These application reads are not quorum observations.
use std::collections::BTreeMap;
use voteboat::{
    application::*,
    identity::*,
    log::*,
    log_format::NativeLogCodec,
    logical_voter::*,
    membership::*,
    quorum::{Limits as PolicyLimits, Policy, Tree},
    runtime::LogicalResponseCodec,
    snapshot_format::NativeSnapshotCodec,
    wire::WireLimits,
    wire_format::NativeWireCodec,
};
#[path = "logical_configuration_history/fixture.rs"]
mod fixture;
use fixture::*;
#[path = "logical_configuration_history/encoding.rs"]
mod encoding;
#[path = "logical_configuration_history/retention.rs"]
mod retention;
