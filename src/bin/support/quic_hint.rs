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
//! The one refusal every binary gives when asked for QUIC in a build without
//! the `quic` feature: the three services and the benchmark's cluster mode.
//! Empty when QUIC is built in.

#[cfg(not(feature = "quic"))]
const MESSAGE: &str = "QUIC support requires building with --features quic";

/// QUIC was requested, but this build has no `quic` feature.
#[cfg(not(feature = "quic"))]
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct QuicNotBuilt;
#[cfg(not(feature = "quic"))]
impl std::fmt::Display for QuicNotBuilt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(MESSAGE)
    }
}
/// The binaries' `main` prints a returned error with `Debug`; this prints
/// what the string error printed before (the quoted message), so their
/// output is unchanged.
#[cfg(not(feature = "quic"))]
impl std::fmt::Debug for QuicNotBuilt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(MESSAGE, f)
    }
}
#[cfg(not(feature = "quic"))]
impl std::error::Error for QuicNotBuilt {}
