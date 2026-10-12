import fs from 'node:fs';
import path from 'node:path';

const [directory, platform] = process.argv.slice(2);
const profiles = ['all', 'default', 'core', 'native'];
const expected = ['fmt', 'inventory', 'conformance', 'conformance_controls'];
const tests = new Map();
for (const profile of profiles) {
    expected.push(`${profile}-clippy`);
    if (profile === 'all' || profile === 'core') {
        expected.push(`${profile}-activation-private`, `${profile}-activation-public`);
        tests.set(`${profile}-activation-private`, { summaries: 1, minimum: 7, names: ["logical_voter::activation::tests::recovery::own_checkpoint_and_original_suffix_restore_busy_and_active_state", "logical_voter::activation::tests::recovery::invalid_restore_is_atomic_and_rejects_history_materialization_and_profile", "logical_voter::activation::tests::semantics::one_replicated_slot_retains_busy_terminal_and_first_positions", "logical_voter::activation::tests::semantics::reserved_terminal_capacity_survives_full_business_and_intent_lifetimes", "logical_voter::activation::tests::semantics::malformed_late_and_unprepared_phases_do_not_partially_apply", "logical_voter::activation::tests::semantics::all_pending_and_retained_phase_aliases_refuse", "logical_voter::activation::tests::semantics::malformed_configuration_or_cross_incarnation_cannot_begin"] });
        tests.set(`${profile}-activation-public`, { summaries: 1, minimum: 4, names: ["contracts::original_business_content_retry_and_contiguous_control_positions", "contracts::bounded_receipts_reads_and_faithful_response_tags", "contracts::explicit_schema_mapping_and_lifetime_envelopes", "contracts::selected_inner_context_is_forwarded_without_freezing_parent_configuration"] });
    }
    if (profile === 'all' || profile === 'default') {
        const quic = profile === 'all';
        expected.push(`${profile}-logical-native`, `${profile}-logical-network`);
        tests.set(`${profile}-logical-native`, { summaries: 1, minimum: quic ? 30 : 19, names: [
            'facade::shutdown::logical_facade_close_drives_original_held_checkpoint_tcp',
            'authority::vote_window::original_granted_reply_acceptance_preserves_full_follower_window',
            'facade::abort::logical_facade_abort_retains_original_child_attempt_and_resolves_unknown_tcp',
            'facade::cancel::logical_facade_cancelled_waits_retain_actual_child_work_tcp',
            'facade::restart::logical_facade_checkpoint_suffix_cold_backing_recovery_tcp',
            ...(quic ? ['facade::shutdown::logical_facade_close_drives_original_held_checkpoint_quic'] : []),
        ] });
        tests.set(`${profile}-logical-network`, { summaries: 2, minimum: quic ? 6 : 5 });
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
    excluded: ['logical_nested_native', 'effect_owner native-only Mac315b', 'full P8 acceptance'] };
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
report.failed = !!(report.missing.length || report.unexpected.length || report.duplicates.length
    || !report.terminal.includes('run=0') || !report.terminal.includes('source_guard=0')
    || Object.values(report.stages).some(row => !row.present || row.warning
        || ('results' in row && (row.results.length !== row.expectedSummaries
            || row.passed < row.minimumPassed || row.failed || row.missingTests.length))));
console.log(JSON.stringify(report, null, 2));
if (report.failed) process.exitCode = 1;
