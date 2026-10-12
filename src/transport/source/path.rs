// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Exact bounded authenticated captures; static routes cannot construct authority.
use super::*;
pub const AUTHENTICATED_DELEGATION_MAX_HOPS: usize = 16;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuthenticatedDelegationPath {
    hops: [Option<SessionBinding>; AUTHENTICATED_DELEGATION_MAX_HOPS],
    length: usize,
}
impl AuthenticatedDelegationPath {
    pub fn hops(&self) -> impl ExactSizeIterator<Item = SessionBinding> + '_ {
        self.hops[..self.length].iter().map(|h| h.unwrap())
    }
    pub fn len(&self) -> usize {
        self.length
    }
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
    #[cfg(feature = "native")]
    pub(crate) fn first(&self) -> SessionBinding {
        self.hops[0].unwrap()
    }
    #[cfg(feature = "native")]
    pub(crate) fn last(&self) -> SessionBinding {
        self.hops[self.length - 1].unwrap()
    }
    #[cfg(feature = "native")]
    pub(crate) fn leaf(binding: SessionBinding) -> Self {
        let mut hops = [None; AUTHENTICATED_DELEGATION_MAX_HOPS];
        hops[0] = Some(binding);
        Self { hops, length: 1 }
    }
    #[cfg(feature = "native")]
    pub(crate) fn extended(mut self, actual: SessionBinding) -> Option<Self> {
        if self.length == AUTHENTICATED_DELEGATION_MAX_HOPS
            || self.last().local != actual.peer
            || self.hops().any(|h| h.local.node == actual.local.node)
        {
            return None;
        }
        self.hops[self.length] = Some(actual);
        self.length += 1;
        Some(self)
    }
}
