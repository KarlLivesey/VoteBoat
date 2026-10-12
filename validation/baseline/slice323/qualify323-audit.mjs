import fs from 'node:fs';
import path from 'node:path';

const [directory, platform] = process.argv.slice(2);
const profiles = ['all', 'default', 'core', 'native'];
const expected = ['fmt', 'inventory', 'conformance', 'conformance_controls'];
const tests = new Map();
const voteNames = [
    'raft::send_window::tests::granted_reply_requires_exact_original_follower_ballot_context_and_store',
    'raft::send_window::tests::only_exact_current_candidate_vote_refreshes',
    'raft::send_window::tests::unrelated_roles_and_old_campaign_are_unchanged_and_overflow_refuses',
];
for (const profile of profiles) {
    expected.push(`${profile}-clippy`, `${profile}-vote-window`);
    tests.set(`${profile}-vote-window`, { summaries: 1, minimum: voteNames.length, names: voteNames });
    if (profile === 'all' || profile === 'default') {
        const quic = profile === 'all';
        expected.push(`${profile}-logical-native`, `${profile}-logical-network`);
        tests.set(`${profile}-logical-native`, { summaries: 1, minimum: quic ? 28 : 18, names: [
            'authority::vote_window::original_accepted_votes_refresh_and_invalidate_queued_election_after_refusal',
            'authority::vote_window::postaccept_timer_exhaustion_fences_but_keeps_original_queue_proof',
            'authority::vote_window::original_granted_reply_acceptance_preserves_full_follower_window',
            'facade::abort::logical_facade_abort_retains_original_child_attempt_and_resolves_unknown_tcp',
            'facade::cancel::logical_facade_cancelled_waits_retain_actual_child_work_tcp',
            'facade::restart::logical_facade_checkpoint_suffix_cold_backing_recovery_tcp',
            ...(quic ? [
                'facade::abort::logical_facade_abort_retains_original_child_attempt_and_resolves_unknown_quic',
                'facade::cancel::logical_facade_cancelled_waits_retain_actual_child_work_quic',
                'facade::restart::logical_facade_checkpoint_suffix_cold_backing_recovery_quic',
            ] : []),
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
