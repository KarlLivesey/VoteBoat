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
//! Public host export ownership controls, with separately labeled native worker evidence.
#[path = "snapshot_exports/contention.rs"]
mod contention;
mod support;
use std::collections::{BTreeMap, VecDeque};
use support::*;
use voteboat::{
    application::*, contracts::*, identity::*, log::*, raft::*, runtime::*, snapshot::*,
    snapshot_worker::*, worker::*,
};
#[path = "snapshot_exports/errors.rs"]
mod errors;
#[path = "snapshot_exports/failed_jobs.rs"]
mod failed_jobs;
#[path = "snapshot_exports/fixture.rs"]
mod fixture;
#[path = "snapshot_exports/intake.rs"]
mod intake;
#[path = "snapshot_exports/lifecycle.rs"]
mod lifecycle;
#[cfg(feature = "native")]
#[path = "snapshot_exports/native.rs"]
mod native;
#[path = "snapshot_exports/providers.rs"]
mod providers;
use fixture::{committed, Fixture, ALLOWANCE};
use providers::Owner;
