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
//! Actual three logical parents backed by nine distinct native physical owners.
use super::*;
#[path = "cohort/assembly.rs"]
mod assembly;
#[path = "cohort/assertions.rs"]
mod assertions;
#[path = "cohort/business.rs"]
mod business;
#[path = "cohort/channels.rs"]
mod channels;
#[path = "cohort/cleanup.rs"]
mod cleanup;
#[path = "cohort/close.rs"]
mod close;
#[path = "cohort/export.rs"]
mod export;
#[path = "cohort/groups.rs"]
mod groups;
#[path = "cohort/history.rs"]
mod history;
#[path = "cohort/members.rs"]
mod members;
#[path = "cohort/preflight.rs"]
mod preflight;
#[path = "cohort/progress.rs"]
mod progress;
#[path = "cohort/rejection.rs"]
mod rejection;
#[path = "cohort/retained.rs"]
mod retained;
#[path = "cohort/snapshots.rs"]
mod snapshots;

#[path = "cohort/pressure.rs"]
mod pressure;

#[path = "cohort/byte_pressure.rs"]
mod byte_pressure;
