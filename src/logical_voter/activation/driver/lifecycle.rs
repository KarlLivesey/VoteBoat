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
impl<'a, P: LogicalActivationParent> ChildActivationDriver<'a, P> {
    pub fn advance(
        &mut self,
        now: MonoTime,
        budget: ChildActivationDriverBudget,
        mut pump: impl for<'p> FnMut(
            LogicalNodePump<'p, ActivationReceipt<P::Receipt>, ActivationRead<P::Read>>,
        ) -> Result<(), LogicalBridgeError>,
    ) -> Result<(), ChildActivationDriverError> {
        if budget.completions > 65536 || budget.transitions > 65536 {
            return Err(ChildActivationDriverError::InvalidLimits);
        }
        if let Err(error) = self.parent.pump(&mut pump) {
            self.fail(ChildActivationDriverError::Lower(Box::new(error)));
        }
        if self.parent.is_running() {
            if let Err(error) = self.parent.poll(now, budget.node) {
                self.fail(ChildActivationDriverError::Node(Box::new(error)));
            }
        }
        let mut remaining = budget.completions;
        if self.fault.is_some() && remaining > 0 {
            self.cleanup_fault()?;
            remaining -= 1;
        }
        for _ in 0..remaining {
            if !self.consume()? {
                break;
            }
        }
        if self.stopped || self.failure.is_some() {
            return self.failure.clone().map_or(Ok(()), Err);
        }
        for _ in 0..budget.transitions {
            if self.pending.is_some() {
                break;
            }
            match self.phase {
                ChildActivationDriverPhase::Observe => {
                    match self
                        .parent
                        .status(self.intent.mapping.parent, self.intent.operations.begin)
                    {
                        Ok(ticket) => self.pending = Some(Attempt::Read(ticket)),
                        Err(ReadInvocationError::Overloaded) => break,
                        Err(error) => {
                            return Err(self.fail(ChildActivationDriverError::Read(error)));
                        }
                    }
                }
                ChildActivationDriverPhase::Propose => {
                    let request = ClientRequest {
                        group: self.intent.mapping.parent,
                        operation: self.intent.operations.begin,
                        bytes: self.begin.clone(),
                    };
                    match self.parent.propose(request) {
                        Ok(ticket) => {
                            self.pending = Some(Attempt::Client(ticket));
                            self.phase = ChildActivationDriverPhase::AwaitBegin;
                        }
                        Err(rejected) if rejected.reason == ClientError::Overloaded => break,
                        Err(rejected) => {
                            return Err(
                                self.fail(ChildActivationDriverError::Client(rejected.reason))
                            );
                        }
                    }
                }
                _ => break,
            }
        }
        Ok(())
    }
    pub fn stop_waiting(&mut self) -> Result<(), ChildActivationDriverError> {
        self.stopped = true;
        self.child.cleanup_pending |= self.authorization.is_some();
        if self.failure.is_none() {
            self.phase = ChildActivationDriverPhase::Stopped;
        }
        match self.pending {
            Some(Attempt::Client(t)) => self
                .parent
                .cancel_client(t)
                .map_err(ChildActivationDriverError::Client),
            Some(Attempt::Read(t)) => self
                .parent
                .cancel_read(t)
                .map_err(ChildActivationDriverError::Read),
            None => Ok(()),
        }
    }
    /// Return the original parent after driver-owned work drains. The host must
    /// complete any previously exported ordinary outputs before Node shutdown.
    pub fn try_reclaim(self) -> Result<&'a mut P, Box<Self>> {
        if self.is_drained() {
            Ok(self.parent)
        } else {
            Err(Box::new(self))
        }
    }
    fn cleanup_fault(&mut self) -> Result<(), ChildActivationDriverError> {
        if !self.stopped {
            return self.failure.clone().map_or(Ok(()), Err);
        }
        match self.fault.take() {
            Some(Fault::Client(output)) => {
                let original =
                    matches!(self.pending,Some(Attempt::Client(t)) if t==output.ticket());
                match self.parent.complete_client(*output) {
                    Ok(_) => {
                        if original {
                            self.pending = None;
                        }
                    }
                    Err(rejected) => {
                        self.fault = Some(Fault::Client(rejected.completion));
                        return Err(ChildActivationDriverError::Client(rejected.reason));
                    }
                }
            }
            Some(Fault::Read(output)) => {
                let original = matches!(self.pending,Some(Attempt::Read(t)) if t==output.ticket());
                match self.parent.complete_read(*output) {
                    Ok(_) => {
                        if original {
                            self.pending = None;
                        }
                    }
                    Err(rejected) => {
                        self.fault = Some(Fault::Read(rejected.completion));
                        return Err(ChildActivationDriverError::Read(rejected.reason));
                    }
                }
            }
            None => (),
        }
        Ok(())
    }
}
