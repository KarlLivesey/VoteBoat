// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#![cfg(feature = "native")]
//! Public-provider histories and on-disk corruption controls, not power-loss tests.
#[path = "witness_log_store/corruption.rs"]
mod corruption;
#[path = "witness_log_store/fixture.rs"]
mod fixture;
#[path = "witness_log_store/history.rs"]
mod history;
#[path = "witness_log_store/selection.rs"]
mod selection;
mod support;
use fixture::*;
use support::*;
use voteboat::{contracts::*, identity::*, log::*, native::log_store::*};
