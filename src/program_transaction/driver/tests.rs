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
use super::*;
#[test]
fn budget_rejects_invalid_nested_scope() {
    let mut budget = ProgramDriverBudget::default();
    budget.node.replica.reconcile = 0;
    assert_eq!(budget.validate(), Err(ProgramDriverError::InvalidLimits));
    budget = ProgramDriverBudget::default();
    budget.node_polls = 0;
    budget.completions = 0;
    budget.transitions = 0;
    assert_eq!(budget.validate(), Ok(()));
}

#[cfg(feature = "tls")]
mod native;

#[cfg(feature = "tls")]
mod history;
