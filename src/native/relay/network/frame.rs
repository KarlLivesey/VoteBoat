// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Bounded fixed relay framing keeps the original inner packet untouched.
use super::*;
mod codec;
pub(super) use codec::*;
mod bounds;
pub(super) use bounds::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelayFrameKind {
    SourceOpened,
    SourceClosed,
    Output,
    Forwarded,
    Retire,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelayRecordKind {
    Request,
    Reply,
    OneWay,
}
pub(super) type RecordKind = RelayRecordKind;
#[derive(Clone)]
pub(super) struct OutputFrame {
    pub ticket: SendTicket,
    pub kinds: Vec<RecordKind>,
    pub wire: Vec<u8>,
}
#[derive(Clone)]
pub(super) struct ForwardFrame {
    pub source: RelaySourceCapture,
    pub output: OutputFrame,
    pub returns: Vec<Option<SendTicket>>,
}
#[derive(Clone)]
pub(super) struct RetiredFrame {
    pub source: RelaySourceCapture,
    pub original: SendTicket,
}
#[derive(Clone)]
pub(super) enum FrameBody {
    Opened(Box<RelaySourceCapture>),
    Closed(Box<RelaySourceCapture>),
    Output(Box<OutputFrame>),
    Forwarded(Box<ForwardFrame>),
    Retire(SendTicket),
    Retired(Box<RetiredFrame>),
}
/// Constructors are confined to original-output capture and real hop/forwarder
/// capture. Owning bytes or a static source catalog cannot mint a frame.
#[derive(Clone)]
pub struct RelayFrame {
    pub(super) body: FrameBody,
}
impl RelayFrame {
    pub fn kind(&self) -> RelayFrameKind {
        match self.body {
            FrameBody::Opened(_) => RelayFrameKind::SourceOpened,
            FrameBody::Closed(_) => RelayFrameKind::SourceClosed,
            FrameBody::Output(_) => RelayFrameKind::Output,
            FrameBody::Forwarded(_) => RelayFrameKind::Forwarded,
            FrameBody::Retire(_) | FrameBody::Retired(_) => RelayFrameKind::Retire,
        }
    }
    pub fn original_ticket(&self) -> Option<SendTicket> {
        match &self.body {
            FrameBody::Output(o) => Some(o.ticket),
            FrameBody::Forwarded(f) => Some(f.output.ticket),
            FrameBody::Retire(t) => Some(*t),
            FrameBody::Retired(t) => Some(t.original),
            _ => None,
        }
    }
    pub fn wire_bytes(&self) -> Option<&[u8]> {
        match &self.body {
            FrameBody::Output(o) => Some(&o.wire),
            FrameBody::Forwarded(f) => Some(&f.output.wire),
            _ => None,
        }
    }
    pub fn source_capture(&self) -> Option<RelaySourceCapture> {
        match &self.body {
            FrameBody::Opened(s) | FrameBody::Closed(s) => Some(**s),
            FrameBody::Forwarded(f) => Some(f.source),
            FrameBody::Retired(t) => Some(t.source),
            _ => None,
        }
    }
    pub fn record_kinds(&self) -> Option<&[RelayRecordKind]> {
        match &self.body {
            FrameBody::Output(o) => Some(&o.kinds),
            FrameBody::Forwarded(f) => Some(&f.output.kinds),
            _ => None,
        }
    }
    pub fn returned_requests(&self) -> Option<&[Option<SendTicket>]> {
        match &self.body {
            FrameBody::Forwarded(f) => Some(&f.returns),
            _ => None,
        }
    }
    pub(super) fn retained_bytes(&self) -> usize {
        let output_cost = |o: &OutputFrame| {
            std::mem::size_of::<OutputFrame>()
                + o.wire.capacity()
                + o.kinds.capacity() * std::mem::size_of::<RecordKind>()
        };
        std::mem::size_of::<Self>()
            + match &self.body {
                FrameBody::Opened(_) | FrameBody::Closed(_) => {
                    std::mem::size_of::<RelaySourceCapture>()
                }
                FrameBody::Output(o) => output_cost(o),
                FrameBody::Forwarded(f) => {
                    std::mem::size_of::<ForwardFrame>()
                        + output_cost(&f.output)
                        + f.returns.capacity() * std::mem::size_of::<Option<SendTicket>>()
                }
                FrameBody::Retire(_) => 0,
                FrameBody::Retired(_) => std::mem::size_of::<RetiredFrame>(),
            }
    }
}
impl NativeRelayBatch {
    pub fn frame<Q: ReadyScheduler, T: TimerService, E: ElectionEntropy>(
        &self,
        owner: &EffectOwner<Q, T, E>,
        codec: &impl WireCodec,
    ) -> Result<RelayFrame, RelayOutputError> {
        let wire = self.encode(owner, codec)?;
        let kinds = self
            .scopes
            .iter()
            .map(|s| match s {
                OriginalScope::Request(_) => RecordKind::Request,
                OriginalScope::Reply => RecordKind::Reply,
                OriginalScope::OneWay => RecordKind::OneWay,
                OriginalScope::Retired => unreachable!("encode rejects retired"),
            })
            .collect();
        Ok(RelayFrame {
            body: FrameBody::Output(Box::new(OutputFrame {
                ticket: self.ticket(),
                kinds,
                wire,
            })),
        })
    }
}
