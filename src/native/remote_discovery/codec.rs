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
use super::{RemoteDiscoveryError as Error, REMOTE_DISCOVERY_WIRE_VERSION};
use crate::{discovery::HintGeneration, identity::*, secure::PeerIdentity};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
pub(super) const LENGTH: usize = 96;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Payload {
    Request,
    Missing,
    Unavailable,
    Hint {
        generation: HintGeneration,
        endpoint: SocketAddr,
        lifetime_ms: u64,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Frame {
    pub sequence: u64,
    pub peer: PeerIdentity,
    pub payload: Payload,
}
impl Frame {
    pub fn encode(self) -> [u8; LENGTH] {
        let mut b = [0; LENGTH];
        b[..4].copy_from_slice(b"VBDH");
        b[4] = REMOTE_DISCOVERY_WIRE_VERSION;
        b[5] = match self.payload {
            Payload::Request => 1,
            Payload::Hint { .. } => 2,
            Payload::Missing => 3,
            Payload::Unavailable => 4,
        };
        b[8..16].copy_from_slice(&self.sequence.to_be_bytes());
        b[16..24].copy_from_slice(&self.peer.node.get().to_be_bytes());
        b[24..40].copy_from_slice(&self.peer.store.id.get().to_be_bytes());
        b[40..48].copy_from_slice(&self.peer.store.incarnation.get().to_be_bytes());
        if let Payload::Hint {
            generation,
            endpoint,
            lifetime_ms,
        } = self.payload
        {
            b[48..56].copy_from_slice(&generation.get().to_be_bytes());
            b[56..58].copy_from_slice(&endpoint.port().to_be_bytes());
            match endpoint {
                SocketAddr::V4(v) => {
                    b[58..62].copy_from_slice(&v.ip().octets());
                    b[74] = 4;
                }
                SocketAddr::V6(v) => {
                    b[58..74].copy_from_slice(&v.ip().octets());
                    b[74] = 6;
                    b[75..79].copy_from_slice(&v.scope_id().to_be_bytes());
                    b[79..83].copy_from_slice(&v.flowinfo().to_be_bytes());
                }
            }
            b[83..91].copy_from_slice(&lifetime_ms.to_be_bytes());
        }
        b
    }
    pub fn decode(b: [u8; LENGTH]) -> Result<Self, Error> {
        if &b[..4] != b"VBDH" || b[4] != REMOTE_DISCOVERY_WIRE_VERSION {
            return Err(Error::Protocol);
        }
        let sequence = u64::from_be_bytes(b[8..16].try_into().unwrap());
        if sequence == 0 {
            return Err(Error::Protocol);
        }
        let peer = PeerIdentity {
            node: NodeId::new(u64::from_be_bytes(b[16..24].try_into().unwrap()))
                .ok_or(Error::Protocol)?,
            store: StoreIdentity {
                id: StoreId::new(u128::from_be_bytes(b[24..40].try_into().unwrap()))
                    .ok_or(Error::Protocol)?,
                incarnation: StoreIncarnation::new(u64::from_be_bytes(
                    b[40..48].try_into().unwrap(),
                ))
                .ok_or(Error::Protocol)?,
            },
        };
        let payload = match b[5] {
            1 => Payload::Request,
            2 => Self::hint(&b)?,
            3 => Payload::Missing,
            4 => Payload::Unavailable,
            _ => return Err(Error::Protocol),
        };
        let frame = Self {
            sequence,
            peer,
            payload,
        };
        if frame.encode() != b {
            return Err(Error::Protocol);
        }
        Ok(frame)
    }
    fn hint(b: &[u8; LENGTH]) -> Result<Payload, Error> {
        let generation = HintGeneration::new(u64::from_be_bytes(b[48..56].try_into().unwrap()))
            .ok_or(Error::Protocol)?;
        let port = u16::from_be_bytes(b[56..58].try_into().unwrap());
        let endpoint = match b[74] {
            4 => SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from(<[u8; 4]>::try_from(&b[58..62]).unwrap())),
                port,
            ),
            6 => SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(<[u8; 16]>::try_from(&b[58..74]).unwrap()),
                port,
                u32::from_be_bytes(b[79..83].try_into().unwrap()),
                u32::from_be_bytes(b[75..79].try_into().unwrap()),
            )),
            _ => return Err(Error::Protocol),
        };
        Ok(Payload::Hint {
            generation,
            endpoint,
            lifetime_ms: u64::from_be_bytes(b[83..91].try_into().unwrap()),
        })
    }
}
