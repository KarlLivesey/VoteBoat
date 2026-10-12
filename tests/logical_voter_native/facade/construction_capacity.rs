// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;

pub fn return_original_parts(
    image: &VerifiedLogicalImage,
    parts: fixture::Parts,
) -> fixture::Parts {
    let original = parts.local.owner.identity();
    let worker = parts.persistence.binding();
    let results = parts.local.results.binding();
    let mut limits = fixture::limits();
    let whole = limits
        .releases
        .capture_reservation_bytes::<CounterReceipt, i64>(
            setup::limits().parent_log.max_entries_per_group,
        )
        .unwrap();
    limits.releases.bytes = limits.releases.control_bytes + whole - 1;
    let rejected = FacadeNode::from_parts(image, parts, limits, MonoTime(0))
        .err()
        .expect("impossible whole static committed batch");
    assert_eq!(
        rejected.reason,
        LogicalNodeError::Replica(ReplicaError::Logical(LogicalReleaseError::InvalidLimits))
    );
    assert_eq!(rejected.parts.local.owner.identity(), original);
    assert_eq!(rejected.parts.persistence.binding(), worker);
    assert_eq!(rejected.parts.local.results.binding(), results);
    assert_eq!(
        rejected.parts.local.results.usage(),
        ApplicationResultUsage::default()
    );
    assert!(rejected.parts.persistence.is_drained());
    *rejected.parts
}
