// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! The same physical publication faults with a committed prefix below the tip.
use super::*;

#[test]
fn partial_commit_and_new_suffix_recover_atomically_without_applying_retained_old_tail() {
    native::check_power_loss(true);
}
