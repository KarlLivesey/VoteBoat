// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Optional selected-network control and concrete diagnostics.
use super::*;
/// Replace one actual adjacent channel after its local ownership settles.
/// Semantic remote collectors are distinct and survive the channel exchange.
pub trait PeerNetworkChannelReplacement: PeerNetwork {
    type Channel;
    fn retire_channel(&mut self, now: MonoTime) -> Result<(), PeerDriverError>;
    fn replace_channel(
        &mut self,
        channel: Self::Channel,
        now: MonoTime,
    ) -> Result<Self::Channel, PeerNetworkRejected<Self::Channel>>;
}
use crate::{authorization::CredentialGeneration, connect::ConnectError};

pub trait PeerNetworkCredentials: PeerNetwork {
    type Credentials;
    fn replace_peer_credentials(
        &mut self,
        expected: CredentialGeneration,
        replacement: CredentialGeneration,
        material: Self::Credentials,
    ) -> Result<Self::Credentials, (ConnectError, Self::Credentials)>;
}

pub trait PeerNetworkInspection: PeerNetwork {
    type DrainStatus;
    fn peer_drain_status(&mut self, source: NodeId) -> Option<Self::DrainStatus>;
}
