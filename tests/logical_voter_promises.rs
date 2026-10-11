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
//! Independent bounded logical-voter promise/response graphs. Ordinary child
//! consensus is assumed; matching written/synchronized stores are explicit.
//! Fixed workload pools, one active child mutation plus one held original output,
//! and all-store catch-up before the next request are selected bounds.
//! Immutable command records identify the bytes in each physical prefix; recovery
//! reads only the selected store's synchronized prefix. Committed/applied flags
//! model surviving ordinary child-consensus authority, not physical persisted
//! metadata or an independently proved child election protocol.
#[path = "logical_voter_promises/controls.rs"]
mod controls;
#[path = "logical_voter_promises/engine.rs"]
mod engine;
#[path = "logical_voter_promises/oracle.rs"]
mod oracle;
#[path = "logical_voter_promises/program.rs"]
mod program;
#[path = "logical_voter_promises/search.rs"]
mod search;
#[path = "logical_voter_promises/types.rs"]
mod types;
static MODEL: std::sync::Mutex<()> = std::sync::Mutex::new(());
#[test]
fn exhausted_bounded_promise_capsule_graphs() {
    let _serial = MODEL.lock().unwrap_or_else(|e| e.into_inner());
    for graph in [
        types::Graph::Promises,
        types::Graph::Repeated,
        types::Graph::Read,
        types::Graph::Written,
        types::Graph::StaleCompletion,
    ] {
        let report = search::explore(graph);
        println!("{graph:?}: {report:?}");
        assert!(report.exhausted && !report.depth_cap && !report.state_cap && !report.edge_cap);
        assert!(
            report.terminals > 0
                && report.losses > 0
                && report.cuts > 0
                && report.recoveries > 0
                && report.partitions > 0
                && report.repairs > 0
        );
        assert!(report.quorum_before_third > 0 && report.observed_before_third > 0);
        if graph == types::Graph::Read {
            assert!(report.read_terminals > 0);
        }
        if matches!(
            graph,
            types::Graph::Promises
                | types::Graph::Repeated
                | types::Graph::Read
                | types::Graph::Written
        ) {
            assert!(report.refused_terminals > 0);
        }
    }
}
