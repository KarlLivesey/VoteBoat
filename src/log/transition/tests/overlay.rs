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
//! Consensus log transitions and the host-replaceable storage contract.
use super::*;

#[test]
fn virtual_accepted_prefix_and_tail_preserve_multiple_unpublished_updates() {
    let base = current();
    let first = update(
        &base,
        1,
        1,
        Some(Suffix {
            from: 3,
            entries: vec![entry(3, 1)].into(),
        }),
    );
    let plan = NextLog::plan(
        &LogView::new(&base),
        &LogSummary::from_validated(&base),
        &first,
        LogLimits::default(),
    )
    .unwrap();
    let NextLog::Delta(delta) = plan else {
        panic!("plain append delta required")
    };
    let metadata = delta.metadata();
    let tail = EntryHistory::from(delta.append.unwrap().entries);
    let view = LogView::overlay(&base, &tail, metadata);
    assert_eq!(view.term_at(1), Some(1));
    assert_eq!(view.term_at(3), Some(1));
    assert_eq!(view.term_at(4), None);
    let accepted = stage_update(&base, &first, LogLimits::default()).unwrap();
    assert_eq!(view.materialize(), accepted);
    let second = update(
        &accepted,
        2,
        2,
        Some(Suffix {
            from: 4,
            entries: vec![entry(4, 2)].into(),
        }),
    );
    let plan = NextLog::plan(&view, &delta.summary, &second, LogLimits::default()).unwrap();
    let expected = stage_update(&accepted, &second, LogLimits::default()).unwrap();
    assert_eq!(plan.metadata(), LogMetadata::from_log(&expected));
    let NextLog::Delta(next) = plan else {
        panic!("second plain delta required")
    };
    let mut published = base;
    published.entries.append_history(tail);
    metadata.install_header(&mut published);
    let summary = NextLog::Delta(next)
        .install(&mut published, second)
        .unwrap();
    assert_eq!(published, expected);
    assert_eq!(summary, LogSummary::from_validated(&published));
}
