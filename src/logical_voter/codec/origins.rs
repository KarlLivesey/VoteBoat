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
impl Encoder {
    pub fn owner(&mut self, o: crate::runtime::RuntimeOwner) -> Result<(), ApplicationError> {
        self.binding(o.store)?;
        self.u64(o.lane.get())?;
        self.u64(o.generation.get())
    }
    pub fn visit(&mut self, v: crate::runtime::VisitTicket) -> Result<(), ApplicationError> {
        self.owner(v.owner)?;
        self.group(v.group)?;
        self.u64(v.sequence)
    }
    pub fn effect(&mut self, e: crate::runtime::EffectTicket) -> Result<(), ApplicationError> {
        self.visit(e.visit)?;
        self.u64(e.sequence)
    }
    pub fn persist(&mut self, p: PersistOrigin) -> Result<(), ApplicationError> {
        self.binding(p.request.binding.store)?;
        self.u64(p.request.binding.generation.get())?;
        self.u64(p.request.sequence)?;
        self.visit(p.visit)
    }
    pub fn snapshot_origin(&mut self, s: SnapshotOrigin) -> Result<(), ApplicationError> {
        self.binding(s.request.binding.store)?;
        self.u64(s.request.binding.generation.get())?;
        self.u64(s.request.sequence)?;
        self.visit(s.visit)
    }
    pub fn capsule_origin(&mut self, o: LogicalCapsuleOrigin) -> Result<(), ApplicationError> {
        match o {
            LogicalCapsuleOrigin::Protocol(e) => {
                self.u8(0)?;
                self.effect(e)
            }
            LogicalCapsuleOrigin::Client {
                effect,
                ticket,
                position,
            } => {
                self.u8(1)?;
                self.effect(effect)?;
                self.owner(ticket.owner)?;
                self.u64(ticket.generation.get())?;
                self.u64(ticket.sequence)?;
                self.group(ticket.group)?;
                self.u128(ticket.operation.get())?;
                self.u64(position.index)?;
                self.u64(position.term)
            }
            LogicalCapsuleOrigin::Read {
                effect,
                ticket,
                barrier,
            } => {
                self.u8(2)?;
                self.effect(effect)?;
                self.owner(ticket.owner)?;
                self.u64(ticket.generation.get())?;
                self.u64(ticket.sequence)?;
                self.group(ticket.group)?;
                self.u64(ticket.request.get())?;
                self.group(barrier.group)?;
                self.u64(barrier.configuration.get())?;
                self.u64(barrier.term)?;
                self.u64(barrier.request.get())?;
                self.u64(barrier.index)
            }
        }
    }
}
impl Decoder<'_> {
    pub fn owner(&mut self) -> Result<crate::runtime::RuntimeOwner, ApplicationError> {
        Ok(crate::runtime::RuntimeOwner {
            store: self.binding()?,
            lane: self.id(ExecutionLaneId::new)?,
            generation: self.id(RuntimeGeneration::new)?,
        })
    }
    pub fn sequence(&mut self) -> Result<u64, ApplicationError> {
        self.u64().and_then(|n| {
            if n > 0 {
                Ok(n)
            } else {
                Err(ApplicationError::InvalidCommand)
            }
        })
    }
    pub fn visit(&mut self) -> Result<crate::runtime::VisitTicket, ApplicationError> {
        Ok(crate::runtime::VisitTicket {
            owner: self.owner()?,
            group: self.group()?,
            sequence: self.sequence()?,
        })
    }
    pub fn effect(&mut self) -> Result<crate::runtime::EffectTicket, ApplicationError> {
        Ok(crate::runtime::EffectTicket {
            visit: self.visit()?,
            sequence: self.sequence()?,
        })
    }
    pub fn persist(&mut self) -> Result<PersistOrigin, ApplicationError> {
        Ok(PersistOrigin {
            request: crate::worker::WorkerTicket {
                binding: crate::worker::WorkerBinding {
                    store: self.binding()?,
                    generation: self.id(StorageWorkerGeneration::new)?,
                },
                sequence: self.sequence()?,
            },
            visit: self.visit()?,
        })
    }
    pub fn snapshot_origin(&mut self) -> Result<SnapshotOrigin, ApplicationError> {
        Ok(SnapshotOrigin {
            request: crate::snapshot_worker::SnapshotWorkTicket {
                binding: crate::snapshot_worker::SnapshotWorkerBinding {
                    store: self.binding()?,
                    generation: self.id(SnapshotWorkerGeneration::new)?,
                },
                sequence: self.sequence()?,
            },
            visit: self.visit()?,
        })
    }
    pub fn capsule_origin(&mut self) -> Result<LogicalCapsuleOrigin, ApplicationError> {
        match self.u8()? {
            0 => Ok(LogicalCapsuleOrigin::Protocol(self.effect()?)),
            1 => Ok(LogicalCapsuleOrigin::Client {
                effect: self.effect()?,
                ticket: LogicalClientTicket {
                    owner: self.owner()?,
                    generation: self.id(ClientRouterGeneration::new)?,
                    sequence: self.sequence()?,
                    group: self.group()?,
                    operation: self.id128(OperationId::new)?,
                },
                position: crate::runtime::ProposalPosition {
                    index: self.sequence()?,
                    term: self.sequence()?,
                },
            }),
            2 => Ok(LogicalCapsuleOrigin::Read {
                effect: self.effect()?,
                ticket: LogicalReadTicket {
                    owner: self.owner()?,
                    generation: self.id(ReadInvocationGeneration::new)?,
                    sequence: self.sequence()?,
                    group: self.group()?,
                    request: self.id(ReadRequestId::new)?,
                },
                barrier: LogicalReadContext {
                    group: self.group()?,
                    configuration: self.id(ConfigurationId::new)?,
                    term: self.sequence()?,
                    request: self.id(ReadRequestId::new)?,
                    index: self.u64()?,
                },
            }),
            _ => Err(ApplicationError::InvalidCommand),
        }
    }
}
