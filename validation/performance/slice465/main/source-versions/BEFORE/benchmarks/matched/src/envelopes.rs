// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Fixed-workload request/original units and explicitly bounded host staging.
use super::*;
use std::mem::size_of;
use voteboat::{
    log::*,
    raft::{BatchLimits, Effect, Proposal},
};
const MAX_HOST_EMPTY_ENVELOPE_BYTES: usize = 4 * 1024 * 1024;
impl Envelopes {
    pub fn request_capacity(self) -> usize {
        match self {
            Self::UpstreamDefault => API_REQUEST_BATCH_CAPACITY,
            Self::Common64 => COMMON_ENTRY_BATCH_CAPACITY,
        }
    }
    pub fn voteboat_limits(self, batch: usize) -> Result<BatchLimits, Failure> {
        if !matches!(batch, 1 | 4) {
            return Err("request batch must be1 or4".into());
        }
        let originals = self
            .request_capacity()
            .checked_mul(batch)
            .ok_or("aggregate originals overflow")?;
        BatchLimits {
            max_proposal_entries: originals,
            max_replication_entries: match self {
                Self::UpstreamDefault => 1024,
                Self::Common64 => COMMON_ENTRY_BATCH_CAPACITY,
            },
            max_ready_reply_inputs: 2,
        }
        .validate()
        .map_err(|error| format!("{error:?}").into())
    }
}
#[derive(Clone, Copy, Serialize)]
pub(super) struct EmptyEnvelope {
    original_capacity: usize,
    proposal_input_bytes: usize,
    proposal_and_staged_entries_bytes: usize,
    update_and_mutation_metadata_bytes: usize,
    complete_empty_envelope_bytes: usize,
    selected_log_batch_bytes: usize,
}
impl EmptyEnvelope {
    fn new(original_capacity: usize) -> Result<Self, Failure> {
        let proposal_input_bytes = original_capacity
            .checked_mul(size_of::<Proposal>())
            .and_then(|bytes| bytes.checked_add(size_of::<Vec<Proposal>>()))
            .ok_or("proposal input overflow")?;
        let proposal_and_staged_entries_bytes = original_capacity
            .checked_mul(size_of::<LogEntry>())
            .and_then(|bytes| bytes.checked_add(proposal_input_bytes))
            .ok_or("owned staging overflow")?;
        // Header values include the Suffix/entry Vec descriptor. Payload capacity is zero
        // in this declared workload; every actual Core call still checks raw capacity.
        let update_and_mutation_metadata_bytes = size_of::<LogUpdate>()
            .checked_add(size_of::<LogMutation>())
            .and_then(|bytes| bytes.checked_add(size_of::<Vec<LogMutation>>()))
            .and_then(|bytes| bytes.checked_add(size_of::<Effect>()))
            .and_then(|bytes| bytes.checked_add(size_of::<Vec<Effect>>()))
            .ok_or("update metadata overflow")?;
        let complete_empty_envelope_bytes = proposal_and_staged_entries_bytes
            .checked_add(update_and_mutation_metadata_bytes)
            .ok_or("empty envelope overflow")?;
        let selected_log_batch_bytes =
            complete_empty_envelope_bytes.max(LogLimits::default().max_batch_bytes);
        if selected_log_batch_bytes > MAX_HOST_EMPTY_ENVELOPE_BYTES {
            return Err("empty envelope exceeds bounded4MiB host staging".into());
        }
        Ok(Self {
            original_capacity,
            proposal_input_bytes,
            proposal_and_staged_entries_bytes,
            update_and_mutation_metadata_bytes,
            complete_empty_envelope_bytes,
            selected_log_batch_bytes,
        })
    }
}
impl Settings {
    pub fn empty_envelope(self) -> Result<EmptyEnvelope, Failure> {
        EmptyEnvelope::new(
            self.envelopes
                .voteboat_limits(self.batch)?
                .max_proposal_entries,
        )
    }
    pub fn log_limits(self) -> Result<LogLimits, Failure> {
        let envelope = self.empty_envelope()?;
        Ok(LogLimits {
            max_entries_per_group: self.retained_budget(),
            max_batch_bytes: envelope.selected_log_batch_bytes,
            ..LogLimits::default()
        }
        .validate()?)
    }
}
#[derive(Serialize)]
pub(super) struct CapacityContract {
    legacy_capacity_fields: &'static str,
    api_capacity_unit: &'static str,
    api_capacity_semantics: &'static str,
    api_merge_original_bound_for_fixed_request_batch: usize,
    append_capacity_unit: &'static str,
    append_capacity_semantics: &'static str,
    replication_capacity_unit: &'static str,
    common_profile_unit: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    voteboat_empty_owned_envelope: Option<EmptyEnvelope>,
}
impl CapacityContract {
    pub fn new(
        engine: &str,
        settings: Settings,
        capacities: (usize, usize, usize),
    ) -> Result<Self, Failure> {
        let openraft = engine == "openraft";
        let bound = if openraft {
            capacities
                .0
                .checked_mul(settings.batch)
                .ok_or("api original bound overflow")?
        } else {
            capacities.0
        };
        Ok(Self {
            legacy_capacity_fields:
                "configuration_values_use_this_contract_for_units_and_hard_or_soft_meaning",
            api_capacity_unit: if openraft {
                "client_write_request_messages"
            } else {
                "original_entries"
            },
            api_capacity_semantics: if openraft {
                "consecutive_request_merge_count_fixed_workload_original_bound"
            } else {
                "hard_original_and_allocated_element_count_ceiling"
            },
            api_merge_original_bound_for_fixed_request_batch: bound,
            append_capacity_unit: "original_entries",
            append_capacity_semantics: if openraft {
                "additional_append_merge_threshold_does_not_split_first_command"
            } else {
                "hard_proposal_original_ceiling_owned_staging_still_checked"
            },
            replication_capacity_unit: "original_entries_per_ordinary_replication_rpc",
            common_profile_unit:
                "common64_means64_api_request_messages_fixed_request_batch_derives_original_bound",
            voteboat_empty_owned_envelope: if openraft {
                None
            } else {
                Some(settings.empty_envelope()?)
            },
        })
    }
}
#[cfg(test)]
mod tests;
