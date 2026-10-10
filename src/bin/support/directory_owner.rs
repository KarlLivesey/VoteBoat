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
//! Ownership survives a disconnected remote manifest lookup.
use super::{
    setup::{checked, Failure},
    Node,
};
use voteboat::{
    native::{lookup_discovery::*, remote_manifest::*},
    routing::*,
    runtime::*,
    secure::*,
};
type Remote = NativeManifestResponder<Box<dyn SecureSession>, NativeManifestLookup<Node>>;
enum State {
    Node(Box<Node>),
    Remote(Box<Remote>),
}
pub struct Owner {
    state: Option<State>,
    authority: voteboat::identity::GroupIdentity,
}
impl Owner {
    pub fn new(node: Node, authority: voteboat::identity::GroupIdentity) -> Self {
        Self {
            state: Some(State::Node(Box::new(node))),
            authority,
        }
    }
    pub fn node(&mut self) -> &mut Node {
        match self.state.as_mut().expect("owned node") {
            State::Node(n) => n,
            State::Remote(r) => r.source_mut().source_mut(),
        }
    }
    pub fn remote(&self) -> bool {
        matches!(self.state, Some(State::Remote(_)))
    }
    pub fn upgrade(
        &mut self,
        session: Box<dyn SecureSession>,
        now: MonoTime,
    ) -> Result<(), Failure> {
        let binding = checked(require_authenticated(&*session))?;
        let Some(State::Node(node)) = self.state.take() else {
            unreachable!("one selected connection")
        };
        let authority = self.authority;
        let source = match NativeManifestLookup::new(
            *node,
            ManifestCacheLimits {
                manifests: 64,
                bytes: 256 * 1024,
            },
            30_000,
            8_000,
            100,
            now,
        ) {
            Ok(source) => source,
            Err((e, node)) => {
                self.state = Some(State::Node(Box::new(node)));
                return Err(format!("lookup setup: {e:?}").into());
            }
        };
        let peer = PeerIdentity {
            node: binding.peer.node,
            store: binding.peer.store.identity,
        };
        match NativeManifestResponder::new(
            session,
            peer,
            authority,
            source,
            RemoteManifestConfig::default(),
            now,
        ) {
            Ok(remote) => self.state = Some(State::Remote(Box::new(remote))),
            Err((e, _, source)) => {
                self.state = Some(State::Node(Box::new(source.into_recovery().0)));
                return Err(format!("remote setup: {e:?}").into());
            }
        }
        Ok(())
    }
    pub fn close_remote(&mut self) {
        if let Some(State::Remote(r)) = &mut self.state {
            r.close();
        }
    }
    pub fn poll(&mut self, now: MonoTime) -> Result<(), Failure> {
        checked(self.node().poll(now, NodePollBudget::default()))?;
        let Some(State::Remote(r)) = &mut self.state else {
            return Ok(());
        };
        match r.source_mut().poll(now) {
            Ok(_) | Err(ManifestLookupPollError::Discovery(_)) => (),
            Err(ManifestLookupPollError::Source(_)) => {
                return Err("manifest source lost exact read ownership".into())
            }
        }
        if !r.is_closed() {
            let was_pending = r.source_mut().pending().is_some();
            let _ = r.poll(now, SessionPollBudget::default());
            if !was_pending {
                if let Some(pending) = r.source_mut().pending() {
                    eprintln!(
                        "manifest read accepted sequence={}",
                        pending.ticket.sequence
                    );
                }
            }
        }
        if r.is_closed()
            && r.source_mut().is_drained()
            && r.source_mut().source_mut().local().reads.usage().requests == 0
        {
            let Some(State::Remote(r)) = self.state.take() else {
                unreachable!()
            };
            let (_, source) = r.into_parts();
            self.state = Some(State::Node(Box::new(source.into_recovery().0)));
        }
        Ok(())
    }
    pub fn take(mut self) -> Node {
        match self.state.take().expect("owned node") {
            State::Node(n) => *n,
            State::Remote(_) => unreachable!("remote drained before shutdown"),
        }
    }
}
