// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Independent finite profile B. Assumes qualified ordinary child consensus;
//! selected-representative cut preserves two child runtimes, not all-child crash.
#[path = "logical_voter_payload/control.rs"]
mod control;
#[path = "logical_voter_payload/image.rs"]
mod image;
#[path = "logical_voter_payload/model.rs"]
mod model;
#[path = "logical_voter_payload/oracle.rs"]
mod oracle;
#[path = "logical_voter_payload/search.rs"]
mod search;
#[path = "logical_voter_payload/storage.rs"]
mod storage;
#[path = "logical_voter_payload/types.rs"]
mod types;
