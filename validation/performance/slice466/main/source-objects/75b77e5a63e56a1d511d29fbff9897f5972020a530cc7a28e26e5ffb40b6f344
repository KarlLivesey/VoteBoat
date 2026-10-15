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
#![cfg(feature = "native")]
#[path = "striped_log_store/faults.rs"]
mod faults;
#[path = "striped_log_store/files.rs"]
mod files;
#[path = "striped_log_store/histories.rs"]
mod histories;
#[path = "striped_log_store/membership.rs"]
mod membership;
#[path = "striped_log_store/model.rs"]
mod model;
mod support;
use support::*;
use voteboat::{contracts::*, identity::*, log::*, native::log_store::striped::*};
fn stripes() -> StripedLimits {
    StripedLimits {
        chunk_bytes: 256,
        ..StripedLimits::default()
    }
}
fn open(io: model::Io) -> StripedLogStore<model::Io> {
    StripedLogStore::create_striped(io, identity(1), LogLimits::default(), stripes()).unwrap()
}
fn recover(io: model::Io) -> Result<StripedLogStore<model::Io>, StorageError> {
    StripedLogStore::recover_striped(io, identity(1), LogLimits::default(), stripes())
}
fn seeded() -> (model::Io, StripedLogStore<model::Io>) {
    let io = model::Io::default();
    let mut store = open(io.clone());
    append(&mut store, vec![LogMutation::Create(bootstrap(1, 3))]);
    (io, store)
}
fn add(store: &mut impl LogStore, value: u8, commit: bool) -> Vec<LogTicket> {
    let s = store.state(group(1)).unwrap();
    let index = s.last_index() + 1;
    let mut e = entry(index, 1, value);
    if let EntryPayload::Command { bytes, .. } = &mut e.payload {
        *bytes = vec![value; 1300].into();
    }
    store
        .append_batch(vec![update(
            &s,
            1,
            if commit { index } else { s.commit_index },
            Some(Suffix {
                from: index,
                entries: vec![e].into(),
            }),
        )])
        .unwrap()
}
