// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Validate fixed prefix and nested counts before allocating declared payloads.
use super::*;
mod fields;
use fields::*;
pub(in crate::native::relay::network) const FRAME_HEADER: usize = 16;
const MAGIC: &[u8; 8] = b"VBRELY02";

pub(in crate::native::relay::network) fn frame_length(
    header: &[u8],
    maximum: usize,
) -> Result<usize, RelayNetworkError> {
    if header.len() != FRAME_HEADER || &header[..8] != MAGIC {
        return Err(RelayNetworkError::InvalidFrame);
    }
    usize::try_from(u64::from_le_bytes(header[8..16].try_into().unwrap()))
        .ok()
        .filter(|n| *n > FRAME_HEADER && *n <= maximum)
        .ok_or(RelayNetworkError::TooLarge)
}
pub(in crate::native::relay::network) fn encode(
    frame: &RelayFrame,
    maximum: usize,
) -> Result<Vec<u8>, RelayNetworkError> {
    let mut e = Encoder::new(maximum);
    e.put(MAGIC)?;
    e.u64(0)?;
    match &frame.body {
        FrameBody::Opened(s) => {
            e.u8(1)?;
            e.source(**s)?;
        }
        FrameBody::Closed(s) => {
            e.u8(2)?;
            e.source(**s)?;
        }
        FrameBody::Output(o) => {
            e.u8(3)?;
            output(&mut e, o)?;
        }
        FrameBody::Forwarded(f) => {
            e.u8(4)?;
            e.source(f.source)?;
            output(&mut e, &f.output)?;
            if f.returns.len() != f.output.kinds.len() {
                return Err(RelayNetworkError::InvalidFrame);
            }
            for t in &f.returns {
                e.u8(u8::from(t.is_some()))?;
                if let Some(t) = t {
                    e.ticket(*t)?;
                }
            }
        }
        FrameBody::Retire(t) => {
            e.u8(5)?;
            e.ticket(*t)?;
        }
        FrameBody::Retired(t) => {
            e.u8(6)?;
            e.source(t.source)?;
            e.ticket(t.original)?;
        }
    }
    let length = e.bytes.len() as u64;
    e.bytes[8..16].copy_from_slice(&length.to_le_bytes());
    Ok(e.bytes.into_boxed_slice().into_vec())
}
fn output(e: &mut Encoder, o: &OutputFrame) -> Result<(), RelayNetworkError> {
    if o.kinds.is_empty() || o.kinds.len() > 4096 {
        return Err(RelayNetworkError::TooLarge);
    }
    e.ticket(o.ticket)?;
    e.u64(o.kinds.len() as u64)?;
    for k in &o.kinds {
        e.u8(match k {
            RecordKind::Request => 1,
            RecordKind::Reply => 2,
            RecordKind::OneWay => 3,
        })?;
    }
    e.blob(&o.wire)
}
pub(in crate::native::relay::network) fn decode(
    bytes: &[u8],
    limits: RelayNetworkLimits,
) -> Result<RelayFrame, RelayNetworkError> {
    if bytes.len() < FRAME_HEADER
        || frame_length(&bytes[..FRAME_HEADER], limits.frame_bytes)? != bytes.len()
    {
        return Err(RelayNetworkError::InvalidFrame);
    }
    let mut d = Decoder::new(&bytes[FRAME_HEADER..]);
    let body = match d.u8()? {
        1 => FrameBody::Opened(Box::new(d.source()?)),
        2 => FrameBody::Closed(Box::new(d.source()?)),
        3 => FrameBody::Output(Box::new(read_output(&mut d, limits)?)),
        4 => {
            let source = d.source()?;
            let output = read_output(&mut d, limits)?;
            let count = output.kinds.len();
            let overhead = count
                .checked_mul(std::mem::size_of::<Option<SendTicket>>())
                .filter(|n| *n <= limits.decoded_bytes)
                .ok_or(RelayNetworkError::TooLarge)?;
            if overhead + output.wire.len() > limits.decoded_bytes {
                return Err(RelayNetworkError::TooLarge);
            }
            let mut returns = Vec::with_capacity(count);
            for _ in 0..count {
                returns.push(match d.u8()? {
                    0 => None,
                    1 => Some(d.ticket()?),
                    _ => return Err(RelayNetworkError::InvalidFrame),
                });
            }
            FrameBody::Forwarded(Box::new(ForwardFrame {
                source,
                output,
                returns,
            }))
        }
        5 => FrameBody::Retire(d.ticket()?),
        6 => FrameBody::Retired(Box::new(RetiredFrame {
            source: d.source()?,
            original: d.ticket()?,
        })),
        _ => return Err(RelayNetworkError::InvalidFrame),
    };
    d.finish()?;
    let frame = RelayFrame { body };
    if frame.retained_bytes() > limits.decoded_bytes {
        return Err(RelayNetworkError::TooLarge);
    }
    Ok(frame)
}
fn read_output(
    d: &mut Decoder<'_>,
    limits: RelayNetworkLimits,
) -> Result<OutputFrame, RelayNetworkError> {
    let ticket = d.ticket()?;
    let count = usize::try_from(d.u64()?)
        .ok()
        .filter(|n| *n > 0 && *n <= 4096)
        .ok_or(RelayNetworkError::TooLarge)?;
    let flags = d.take(count)?;
    let mut kinds = Vec::with_capacity(count);
    for flag in flags {
        kinds.push(match flag {
            1 => RecordKind::Request,
            2 => RecordKind::Reply,
            3 => RecordKind::OneWay,
            _ => return Err(RelayNetworkError::InvalidFrame),
        });
    }
    let wire = d.blob(limits.frame_bytes)?.to_vec();
    Ok(OutputFrame {
        ticket,
        kinds,
        wire,
    })
}
