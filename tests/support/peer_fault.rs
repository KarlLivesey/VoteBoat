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
//! Test-only authenticated transport fault injection; no production fault policy.
use std::{cell::Cell, rc::Rc};
use voteboat::{
    identity::NodeId,
    native::{transport::NativeTransportFactory, wire::NativeWireCodec},
    outbound::*,
    runtime::MonoTime,
    secure::*,
    transport::*,
};
#[derive(Default)]
pub struct Faults {
    pub isolated: Cell<Option<NodeId>>,
    pub dropped: Cell<usize>,
}
pub struct Factory {
    pub native: NativeTransportFactory<NativeWireCodec>,
    pub faults: Rc<Faults>,
}
struct FaultTransport<P> {
    inner: P,
    faults: Rc<Faults>,
}
impl<S: SecureSession + 'static> PeerTransportFactory<S> for Factory {
    type Transport = Box<dyn PeerTransport>;
    fn build<O: OutboundQueue>(
        &mut self,
        session: S,
        outbound: &O,
    ) -> Result<Self::Transport, TransportError> {
        Ok(Box::new(FaultTransport {
            inner: self.native.build(session, outbound)?,
            faults: self.faults.clone(),
        }))
    }
}
impl<P: PeerTransport> PeerTransport for FaultTransport<P> {
    fn security(&self) -> SessionSecurity {
        self.inner.security()
    }
    fn binding(&self) -> SessionBinding {
        self.inner.binding()
    }
    fn state(&self) -> TransportState {
        self.inner.state()
    }
    fn limits(&self) -> TransportLimits {
        self.inner.limits()
    }
    fn usage(&self) -> TransportUsage {
        self.inner.usage()
    }
    fn submit(&mut self, batch: OutboundBatch) -> Result<(), TransportRejected> {
        self.inner.submit(batch)
    }
    fn poll(
        &mut self,
        now: MonoTime,
        budget: TransportPollBudget,
    ) -> Result<TransportProgress, TransportError> {
        let progress = self.inner.poll(now, budget)?;
        let binding = self.binding();
        if self
            .faults
            .isolated
            .get()
            .is_some_and(|id| id == binding.local.node || id == binding.peer.node)
            && self.inner.take_received().is_some()
        {
            self.faults.dropped.set(self.faults.dropped.get() + 1);
        }
        Ok(progress)
    }
    fn take_send(&mut self) -> Option<TransportSend> {
        self.inner.take_send()
    }
    fn received_info(&self) -> Option<ReceiveInfo> {
        self.inner.received_info()
    }
    fn take_received(&mut self) -> Option<ReceivedBatch> {
        self.inner.take_received()
    }
    fn close(&mut self) {
        self.inner.close();
    }
    fn abort(&mut self) {
        self.inner.abort();
    }
}
