// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
use super::*;
fn settings(envelopes: Envelopes, batch: usize) -> Settings {
    Settings {
        clients: 4096,
        batch,
        requested_operations: 16384,
        operations: 16384,
        warmup: 0,
        maintenance: Maintenance::DefaultRetention,
        envelopes,
        attribution: false,
    }
}
#[test]
fn fixed_request_profiles_derive_equal_original_bounds_and_independent_replication() {
    for envelopes in [Envelopes::UpstreamDefault, Envelopes::Common64] {
        for batch in [1, 4] {
            let s = settings(envelopes, batch);
            let limits = envelopes.voteboat_limits(batch).unwrap();
            assert_eq!(
                limits.max_proposal_entries,
                envelopes.request_capacity() * batch
            );
            assert_eq!(
                limits.max_replication_entries,
                if envelopes == Envelopes::UpstreamDefault {
                    1024
                } else {
                    64
                }
            );
            let v = CapacityContract::new(
                "voteboat",
                s,
                (
                    limits.max_proposal_entries,
                    limits.max_proposal_entries,
                    limits.max_replication_entries,
                ),
            )
            .unwrap();
            let o = CapacityContract::new(
                "openraft",
                s,
                (
                    envelopes.request_capacity(),
                    envelopes.request_capacity(),
                    limits.max_replication_entries,
                ),
            )
            .unwrap();
            assert_eq!(
                v.api_merge_original_bound_for_fixed_request_batch,
                o.api_merge_original_bound_for_fixed_request_batch
            );
            assert_eq!(o.api_capacity_unit, "client_write_request_messages");
            assert_eq!(v.api_capacity_unit, "original_entries");
            assert!(o.append_capacity_semantics.contains("does_not_split"));
        }
        for invalid in [0, 2, usize::MAX] {
            assert!(envelopes.voteboat_limits(invalid).is_err());
        }
    }
}
#[test]
fn empty_owned_budget_is_derived_before_launch_without_changing_native_defaults() {
    let default = LogLimits::default();
    for envelopes in [Envelopes::UpstreamDefault, Envelopes::Common64] {
        for batch in [1, 4] {
            let s = settings(envelopes, batch);
            let limits = s.log_limits().unwrap();
            let e = s.empty_envelope().unwrap();
            assert_eq!(
                e.proposal_input_bytes,
                size_of::<Vec<Proposal>>() + e.original_capacity * size_of::<Proposal>()
            );
            assert_eq!(
                e.proposal_and_staged_entries_bytes,
                e.proposal_input_bytes + e.original_capacity * size_of::<LogEntry>()
            );
            assert_eq!(
                e.complete_empty_envelope_bytes,
                e.proposal_and_staged_entries_bytes + e.update_and_mutation_metadata_bytes
            );
            assert_eq!(
                limits.max_batch_bytes,
                default.max_batch_bytes.max(e.complete_empty_envelope_bytes)
            );
            assert!(limits.max_batch_bytes <= MAX_HOST_EMPTY_ENVELOPE_BYTES);
            assert_eq!(limits.max_command_bytes, default.max_command_bytes);
            assert_eq!(limits.control_reserve_bytes, default.control_reserve_bytes);
            assert_eq!(limits.max_wal_bytes, default.max_wal_bytes);
            assert_eq!(BatchLimits::default().max_proposal_entries, 64);
        }
    }
    assert!(
        settings(Envelopes::UpstreamDefault, 4)
            .empty_envelope()
            .unwrap()
            .proposal_and_staged_entries_bytes
            > default.max_batch_bytes
    );
    assert!(EmptyEnvelope::new(usize::MAX).is_err());
}
