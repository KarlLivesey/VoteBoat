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
//! Independent finite application protocol model, not a native transaction engine.
#[path = "transaction_model/controls.rs"]
mod controls;
#[path = "transaction_model/oracle.rs"]
mod oracle;
#[path = "transaction_model/protocol.rs"]
mod protocol;
#[path = "transaction_model/search.rs"]
mod search;

#[test]
fn bounded_two_transaction_durable_protocol_preserves_atomicity_and_locks() {
    let report = search::explore(protocol::State::new(20), 80, 250_000);
    println!("{report:?}");
    assert!(report.states > 1000);
    assert!(report.edges > report.states);
    assert!(report.losses > 0 && report.restarts > 0);
    assert!(report.finished_commit_states > 0 && report.finished_abort_states > 0);
    assert!(report.two_commit_finished_states > 0 && report.commit_abort_finished_states > 0);
    assert!(
        report.exhausted && !report.depth_limited && !report.state_limited,
        "declared two-transaction graph was truncated: {report:?}"
    );
}
