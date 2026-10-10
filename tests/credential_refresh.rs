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
#[path = "credential_refresh/host.rs"]
mod host;
#[cfg(feature = "tls")]
#[path = "credential_refresh/keys.rs"]
mod keys;
#[cfg(feature = "native")]
#[path = "credential_refresh/native.rs"]
mod native;
mod support;
use voteboat::{identity::*, runtime::MonoTime, secure::*};
fn local(id: u64) -> LocalIdentity {
    LocalIdentity {
        node: support::node(id),
        store: StoreBinding {
            identity: support::identity(id.into()),
            session: StoreSession::new(1).unwrap(),
        },
    }
}
#[cfg(feature = "tls")]
#[test]
fn native_tls_rotation_revokes_old_io_and_new_session_is_usable() {
    let (a, b) = support::tls::pair(local(1), local(2), 1);
    let credentials = native::exercise(a, b, MonoTime(0));
    let (a, b) = support::tls::pair(local(1), local(2), 2);
    native::resume(a, b, &credentials, MonoTime(0));
}
