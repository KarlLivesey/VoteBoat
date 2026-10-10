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
#[path = "../credential_refresh/native.rs"]
mod native;
#[test]
fn quic_rotation_revokes_old_io_and_reconnect_preserves_identity() {
    let mut old_connectors = connectors();
    let (mut sessions, now) = establish(&mut old_connectors, 0, 1);
    let a = sessions.remove(&(1, 2)).unwrap();
    let b = sessions.remove(&(2, 1)).unwrap();
    let credentials = native::exercise(a, b, MonoTime(now));
    let mut fresh_connectors = connectors();
    let (mut fresh, now) = establish(&mut fresh_connectors, 0, 2);
    let a = fresh.remove(&(1, 2)).unwrap();
    let b = fresh.remove(&(2, 1)).unwrap();
    native::resume(a, b, &credentials, MonoTime(now));
}
