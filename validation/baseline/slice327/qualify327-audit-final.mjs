import fs from 'node:fs';
import path from 'node:path';

const [directory, platform] = process.argv.slice(2);
const profiles = ['all', 'default', 'core', 'native'];
const expected = ['fmt', 'inventory', 'conformance', 'conformance_controls'];
const tests = new Map();
for (const profile of profiles) {
    expected.push(`${profile}-clippy`);
    if (profile === 'all' || profile === 'core') {
        expected.push(`${profile}-response-private`, `${profile}-configuration-public`);
        expected.push(`${profile}-child-profile-private`, `${profile}-wrapper-profile-private`, `${profile}-child-profile-public`, `${profile}-wrapper-profile-public`);
        tests.set(`${profile}-wrapper-profile-private`, { summaries: 1, minimum: 2, names: [
            'logical_voter::activation::tests::control_profile::actual_profile_classifies_complete_control_envelopes_without_promoting_business',
            'logical_voter::activation::tests::control_profile::profile_encoding_is_bounded_and_pins_all_selected_limits_and_inner_schema'
        ] });

        tests.set(`${profile}-child-profile-private`, { summaries: 1, minimum: 4, names: ["logical_voter::application::tests::control_profile::selected_profile_is_immutable_and_checkpoint_binding_is_exact", "logical_voter::application::tests::control_profile::original_mixed_action_uses_reserved_capacity_and_replays_without_splitting", "logical_voter::application::tests::control_profile::malformed_and_business_wrapped_controls_do_not_use_reserved_capacity", "logical_voter::application::tests::control_profile::control_result_reservation_requires_exact_schema_group_and_operation"] });
        tests.set(`${profile}-child-profile-public`, { summaries: 1, minimum: 1, names: ['selected_actual_activation_profile_pins_configuration_and_refuses_rebinding'] });
        tests.set(`${profile}-wrapper-profile-public`, { summaries: 1, minimum: 1, names: ['contracts::opaque_control_profile_is_derived_from_exact_actual_wrapper_configuration'] });

        tests.set(`${profile}-response-private`, { summaries: 1, minimum: 15, names: ["logical_voter::response::tests::limits::accepted_state_growth_cannot_exceed_the_checked_response_envelope", "logical_voter::response::tests::limits::bounds_stay_stable_across_apply_checkpoint_and_restore", "logical_voter::response::tests::limits::full_image_framing_refuses_growth_even_when_parent_codec_fits", "logical_voter::response::tests::limits::short_read_destinations_and_overbound_values_are_atomic", "logical_voter::response::tests::limits::staged_response_capacity_never_exceeds_the_static_encoding_bound", "logical_voter::response::tests::providers::original_configuration_response_rejects_nonfaithful_selected_batch_codec", "logical_voter::response::tests::providers::refusing_or_nonfaithful_selected_log_codecs_cannot_encode_image", "logical_voter::response::tests::providers::selected_snapshot_roundtrip_failure_leaves_output_and_state_unchanged", "logical_voter::response::tests::reads::all_read_variants_preserve_exact_typed_values", "logical_voter::response::tests::reads::complete_image_membership_parent_and_pin_fields_are_retained", "logical_voter::response::tests::reads::complete_outcome_status_and_maximum_command_fit_the_static_bound", "logical_voter::response::tests::reads::metadata_optional_and_counter_fields_are_retained", "logical_voter::response::tests::receipts::exact_query_variants_and_every_snapshot_reference_field_are_distinct", "logical_voter::response::tests::receipts::receipts_preserve_all_outcomes_and_original_fields", "logical_voter::response::tests::receipts::short_receipt_and_query_destinations_remain_unchanged"] });
        tests.set(`${profile}-configuration-public`, { summaries: 1, minimum: 7, names: ["encoding::exact_original_entries_and_additive_query_encoding_roundtrip", "encoding::invalid_original_history_shapes_cannot_encode_and_short_buffers_are_atomic", "encoding::retained_allocations_are_charged_and_config_apply_refuses_small_envelope_atomically", "encoding::truncated_trailing_overcount_and_wrong_scope_bytes_refuse", "retention::invalid_configuration_progression_is_atomic", "retention::original_configuration_positions_survive_own_checkpoint_and_suffix", "retention::unknown_is_scoped_inspection_and_future_prefix_is_not_applied"] });
    }
    if (profile === 'all' || profile === 'native') {
        expected.push(`${profile}-snapshot-ack`);
        expected.push(`${profile}-parent-driver`);
        tests.set(`${profile}-parent-driver`, { summaries: 1, minimum: profile === 'all' ? 2 : 1, names: [
            'history::original_parent_begin_fresh_status_retry_busy_and_cancel_tcp',
            ...(profile === 'all' ? ['history::original_parent_begin_fresh_status_retry_busy_and_cancel_quic'] : [])
        ] });

        tests.set(`${profile}-snapshot-ack`, { summaries: 1, minimum: profile === 'all' ? 2 : 1, names: [
            'authority::snapshot_ack::original_duplicate_snapshot_ack_above_and_below_base_tcp',
            ...(profile === 'all' ? ['authority::snapshot_ack::original_duplicate_snapshot_ack_above_and_below_base_quic'] : [])
        ] });
    }
}

const read = name => {
    const file = path.join(directory, `${platform}-${name}`);
    return fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : '';
};
const exits = read('exits.txt').trim().split('\n').filter(Boolean);
const report = { platform, declaredStages: expected.length, expected, exits,
    missing: expected.filter(stage => !exits.includes(`${stage}=0`)),
    unexpected: exits.filter(exit => !expected.some(stage => exit === `${stage}=0`)),
    duplicates: exits.filter((exit, i) => exits.indexOf(exit) !== i), stages: {},
    terminal: read('terminal.txt').trim().split('\n'),
    excluded: ['logical_nested_native', 'effect_owner native-only Mac315b', 'Mac324 failed election aggregate', 'actual membership activation after parent Begin', 'full nested logical composition', 'full P8 acceptance'] };
for (const stage of expected) {
    const text = read(`${stage}.log`);
    const checks = tests.get(stage);
    const results = [...text.matchAll(/test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;/g)]
        .map(m => ({ result: m[1], passed: Number(m[2]), failed: Number(m[3]),
            ignored: Number(m[4]), measured: Number(m[5]), filtered: Number(m[6]) }));
    const passed = results.reduce((n, r) => n + r.passed, 0);
    // Successful fmt normally emits an empty log. A missing file is distinct
    // from an empty non-test log; test stages must still contain exact summaries.
    report.stages[stage] = { present: fs.existsSync(path.join(directory, `${platform}-${stage}.log`)), warning: /(^|\n)warning:/.test(text),
        ...(checks ? { results, passed, failed: results.reduce((n, r) => n + r.failed, 0),
            expectedSummaries: checks.summaries, minimumPassed: checks.minimum,
            missingTests: (checks.names ?? []).filter(name => !text.includes(`test ${name} ... ok`)) } : {}) };
}
const conformance = (() => { try { return JSON.parse(read('conformance.log')); } catch { return {}; } })();
report.metadata = {
    inventory126: /126 implemented contracts: inventory shape and conformance paths pass\./.test(read('inventory.log')),
    partialReviews27: conformance.reviewed_contracts === 27,
    operationNames310: conformance.reviewed_operations === 310,
    negative33: /^(?:# |ℹ )tests 33$/m.test(read('conformance_controls.log')) && /^(?:# |ℹ )pass 33$/m.test(read('conformance_controls.log')) && /^(?:# |ℹ )fail 0$/m.test(read('conformance_controls.log'))
};
report.failed = !!(Object.values(report.metadata).some(value => !value) || report.missing.length || report.unexpected.length || report.duplicates.length
    || !report.terminal.includes('run=0') || !report.terminal.includes('source_guard=0')
    || Object.values(report.stages).some(row => !row.present || row.warning
        || ('results' in row && (row.results.length !== row.expectedSummaries
            || row.passed !== row.minimumPassed || row.failed || row.missingTests.length))));
console.log(JSON.stringify(report, null, 2));
if (report.failed) process.exitCode = 1;
