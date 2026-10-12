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
//! Actual logical snapshot port/router over the same sole child provider.
use super::*;
use voteboat::snapshot_worker::*;
pub fn attach(mut parts: fixture::Parts, image: &VerifiedLogicalImage) -> fixture::Parts {
    let (worker, port) = logical_snapshots(
        image,
        &parts.persistence,
        SnapshotWorkerGeneration::new(1).unwrap(),
        work_limits(),
    )
    .unwrap();
    let router = SnapshotRouter::new(
        parts.local.owner.identity(),
        worker.binding(),
        SnapshotRouterLimits {
            requests: 4,
            max_image_bytes: snapshot_load_reservation(groups::storage_limits().parent_snapshots)
                .unwrap(),
        },
    )
    .unwrap();
    parts.local.snapshots = Some(NodeSnapshots { worker, router });
    parts.snapshots = Some(port);
    parts
}

pub fn work_limits() -> SnapshotWorkLimits {
    SnapshotWorkLimits {
        max_groups: 1,
        max_requests: 4,
        max_bytes: 512 * 1024,
        control_requests: 1,
        control_bytes: 128 * 1024,
    }
}
