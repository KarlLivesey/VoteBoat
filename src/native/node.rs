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
//! Standard native node types. from_parts consumes explicitly opened/recovered
//! providers; it does not start a second runtime or choose alternate backends.
use super::{
    connect::NativePeerConnector,
    log_store::{FileLogIo, NativeLogStore},
    outbound::NativeOutbound,
    runtime::{DeadlineQueue, FairScheduler, JitterEntropy},
    snapshot_store::{FileSnapshotIo, NativeSnapshotStore},
    snapshot_worker::NativeSnapshotWorker,
    transport::NativeTransportFactory,
    wire::NativeWireCodec,
    worker::NativeLogWorker,
};
use crate::runtime::{Node, NodeLocalParts, NodeParts};
pub type NativeNode<A> = Node<
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    A,
    NativeLogWorker<NativeLogStore<FileLogIo>>,
    NativeOutbound,
    NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>,
    NativePeerConnector,
    NativeTransportFactory<NativeWireCodec>,
>;
pub type NativeNodeParts<A> = NodeParts<
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    A,
    NativeLogWorker<NativeLogStore<FileLogIo>>,
    NativeOutbound,
    NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>,
    NativePeerConnector,
    NativeTransportFactory<NativeWireCodec>,
>;
pub type NativeLocalParts<A> = NodeLocalParts<
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    A,
    NativeLogWorker<NativeLogStore<FileLogIo>>,
    NativeOutbound,
    NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>,
>;
