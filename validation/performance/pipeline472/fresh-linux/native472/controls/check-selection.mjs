import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
const root = process.argv[2];
const tokens = text => Object.fromEntries(text.trim().split(/\s+/).map(pair => pair.split('=')));
const expected = {
    worker_units: '4', append_window_enabled: 'true', append_window_units: '4',
    append_window_originals: '64', append_window_core_bytes: '1048576', group: '1',
};
for (const mode of ['create', 'recover']) {
    const rows = fs.readFileSync(path.join(root, `append-window-${mode}.txt`), 'utf8').trim().split('\n').map(tokens);
    assert.equal(rows.length, 3);
    assert.deepEqual(rows.map(row => row.replica).sort(), ['1', '2', '3']);
    for (const row of rows) {
        assert.deepEqual(Object.keys(row).sort(), [...Object.keys(expected), 'replica'].sort());
        for (const [key, value] of Object.entries(expected)) assert.equal(row[key], value, `${mode}:${row.replica}:${key}`);
    }
}
const summary = tokens(fs.readFileSync(path.join(root, 'summary.txt'), 'utf8'));
for (const [key, value] of Object.entries(expected).filter(([key]) => key.startsWith('append_window'))) assert.equal(summary[key], value, key);
assert.equal(summary.ready_visit_items_effective, '16');
assert.equal(summary.operations, '128');
assert.equal(summary.window, '64');
assert.equal(summary.warmup, '64');
assert.ok(['startup', 'shared'].includes(summary.assembly));
assert.equal(summary.proposal_batch_diagnostics, String(summary.assembly === 'shared'));
assert.equal(summary.barrier_dependency_diagnostics, String(summary.assembly === 'shared'));
if (summary.assembly === 'shared') assert.equal(summary.physical_barrier_correlation, 'true');
console.log(JSON.stringify({root, create_and_recover_replicas: 3, selected: expected, scope: 'Actual asserted release functional provider readback; no paired throughput or barrier-amortization claim.'}));
