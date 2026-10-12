// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use voteboat::{
    log_format::NativeLogCodec,
    logical_voter::LogicalChildBridge,
    native::{
        connect::NativeServiceConnector,
        log_store::{FileLogIo, NativeLogStore},
        outbound::NativeOutbound,
        runtime::{DeadlineQueue, FairScheduler, JitterEntropy},
        snapshot_store::{FileSnapshotIo, NativeSnapshotStore},
        snapshot_worker::NativeSnapshotWorker,
        transport::NativeTransportFactory,
        worker::NativeLogWorker,
    },
    snapshot_format::NativeSnapshotCodec,
    wire_format::NativeWireCodec,
};
pub type Bridge<'a> = LogicalChildBridge<
    'a,
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    NativeLogCodec,
    NativeSnapshotCodec,
    NativeWireCodec,
    NativeLogWorker<NativeLogStore<FileLogIo>>,
    NativeOutbound,
    NativeSnapshotWorker<NativeSnapshotStore<FileSnapshotIo>>,
    NativeServiceConnector,
    NativeTransportFactory<NativeWireCodec>,
>;
pub type Middle = voteboat::logical_voter::LogicalNode<
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    voteboat::logical_voter::LogicalApplication,
>;
pub type Top = voteboat::logical_voter::LogicalNode<
    FairScheduler,
    DeadlineQueue,
    JitterEntropy,
    super::application::App,
>;
pub type UpperBridge<'a> = voteboat::logical_voter::LogicalBackingBridge<'a, Middle>;
